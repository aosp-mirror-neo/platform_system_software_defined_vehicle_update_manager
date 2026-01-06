/*
 * Copyright (C) 2026 The Android Open Source Project
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

use crate::apex_service::ApexService;
use crate::payload::{ServiceBundleUpdatePayload, SystemUpdatePayload};
use apkverify::verify;
use log::error;
use sdv_status_rs::{derive, SdvResult, SdvStatus, SdvStatusCode};
use std::path::{Path, PathBuf};
use std::sync::Arc;

pub const APEX_SERVICE_SESSION_DIR_PREFIX: &str = "session_";

#[derive(Debug, Clone, PartialEq)]
pub struct SystemUpdateMetadata {
    pub payload: SystemUpdatePayload,
    pub initial_boot_slot: i32,
}

impl SystemUpdateMetadata {
    pub fn new(payload: SystemUpdatePayload, initial_boot_slot: i32) -> Self {
        Self { payload, initial_boot_slot }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct ApexChildSession {
    pub apex_path: String,
    pub session_id: i32,
}

#[derive(Debug, Clone, PartialEq)]
pub struct ApexInfo {
    pub module_name: String,
    pub version_code: i64,
}

impl ApexInfo {
    pub fn new(module_name: &str, version_code: i64) -> Self {
        ApexInfo { module_name: module_name.to_string(), version_code }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct ServiceBundleUpdateMetadata {
    pub apex_child_sessions: Vec<ApexChildSession>,
    pub parent_session_id: i32,
    pub boot_attempts: i32,
    pub submitted_apex_info: Vec<ApexInfo>,
}

pub fn session_id_to_session_directory_path(app_staging_dir: &Path, session_id: i32) -> PathBuf {
    app_staging_dir.join(format!("{APEX_SERVICE_SESSION_DIR_PREFIX}{session_id}"))
}

async fn create_session_directory(
    app_staging_dir: &Path,
    session_id: i32,
    created_dirs: &mut Vec<i32>,
) -> SdvResult<PathBuf> {
    let session_dir = session_id_to_session_directory_path(app_staging_dir, session_id);
    tokio::fs::create_dir(session_dir.clone()).await.map_err(derive!(
        SdvStatusCode::Internal,
        format!("Failed to create {} directory", session_dir.display())
    ))?;
    created_dirs.push(session_id);

    Ok(session_dir)
}

async fn copy_apex_into_session_directory(
    apex_path: &str,
    child_session_dir: &Path,
) -> SdvResult<()> {
    let from_file = Path::new(&apex_path);
    let to_file = child_session_dir.join(from_file.file_name().ok_or_else(|| {
        SdvStatus::with_message(
            SdvStatusCode::FailedPrecondition,
            format!("Apex path {} has an invalid file name", from_file.display()),
        )
    })?);
    tokio::fs::copy(from_file, to_file).await.map_err(derive!(
        SdvStatusCode::Internal,
        "Failed to copy the apex file to the session directory"
    ))?;

    Ok(())
}

// This helper function is used to prepare all of the APEXes in a service bundle update.
// It first creates the parent session directory, then it creates a child session directory
// for each APEX before copying it to that location. Finally, it submits the collection of
// sessions to the ApexService.
//
// If at anypoint a failure occurs, the process is halted and an Err is returned. The
// "created_dirs" argument is used to track all of the directories that were successfully
// created so that the caller of this function can clean them up.
async fn copy_apexes_and_submit_session(
    apex_child_sessions: &[ApexChildSession],
    apex_service: Arc<dyn ApexService>,
    app_staging_dir: &Path,
    parent_session_id: i32,
    created_dirs: &mut Vec<i32>,
) -> SdvResult<Vec<ApexInfo>> {
    // The parent session directory does not have any APEXes placed within it, however it
    // is used to track the uniqueness of the parent session id.
    create_session_directory(app_staging_dir, parent_session_id, created_dirs).await?;

    for apex_child_session in apex_child_sessions {
        let child_session_dir =
            create_session_directory(app_staging_dir, apex_child_session.session_id, created_dirs)
                .await?;
        copy_apex_into_session_directory(&apex_child_session.apex_path, &child_session_dir).await?;
    }

    let child_session_ids: Vec<i32> = apex_child_sessions.iter().map(|a| a.session_id).collect();
    let apex_info_list =
        apex_service.submit_staged_session(parent_session_id, &child_session_ids).await?;

    let apex_infos = apex_info_list
        .apexInfos
        .iter()
        .map(|a| ApexInfo::new(&a.moduleName, a.versionCode))
        .collect();

    Ok(apex_infos)
}

impl ServiceBundleUpdateMetadata {
    pub fn new(
        apex_child_sessions: Vec<ApexChildSession>,
        parent_session_id: i32,
        boot_attempts: i32,
        submitted_apex_info: Vec<ApexInfo>,
    ) -> Self {
        Self { apex_child_sessions, parent_session_id, boot_attempts, submitted_apex_info }
    }

    fn get_current_sdk() -> SdvResult<u32> {
        match rustutils::android::system_properties::read("ro.build.version.sdk") {
            Ok(Some(sdk_string)) => sdk_string.parse::<u32>().map_err(|e| {
                SdvStatus::with_message(
                    SdvStatusCode::Internal,
                    format!("Malformed SDK version: {e}"),
                )
            }),
            Ok(None) => Err(SdvStatus::with_message(
                SdvStatusCode::Internal,
                "SDK version property missing",
            )),
            Err(e) => Err(SdvStatus::with_message(
                SdvStatusCode::Internal,
                format!("Failed to read SDK version property: {e}"),
            )),
        }
    }
    pub fn from_payload_and_parent_session_id(
        payload: ServiceBundleUpdatePayload,
        parent_session_id: i32,
    ) -> SdvResult<Self> {
        let apex_child_sessions = payload
            .apex_paths
            .into_iter()
            .enumerate()
            .map(|(index, apex_path)| {
                let index: i32 = index.try_into().map_err(derive!(
                    SdvStatusCode::ResourceExhausted,
                    "Failed to map apex to a child session id"
                ))?;
                let session_id = parent_session_id + index + 1;

                SdvResult::Ok(ApexChildSession { apex_path, session_id })
            })
            .collect::<Result<Vec<ApexChildSession>, _>>()?;

        Ok(Self::new(apex_child_sessions, parent_session_id, payload.boot_attempts, Vec::new()))
    }

    /// Submit the ServiceBundleUpdatePayload to the ApexService. Submitting the payload
    /// performs initial verification but it does not activate it.
    pub async fn submit(
        &self,
        apex_service: Arc<dyn ApexService>,
        app_staging_dir: &Path,
    ) -> SdvResult<Vec<ApexInfo>> {
        if self.apex_child_sessions.is_empty() {
            return Err(SdvStatus::with_message(
                SdvStatusCode::InvalidArgument,
                "No APEX paths supplied in the payload.",
            ));
        }
        let current_sdk = Self::get_current_sdk()?;
        for apex_session in &self.apex_child_sessions {
            if let Err(e) = verify(Path::new(&apex_session.apex_path), current_sdk) {
                return Err(SdvStatus::with_message(
                    SdvStatusCode::Internal,
                    format!("Failed to verify signature of {}: {}", apex_session.apex_path, e),
                ));
            }
        }
        let last_child_session = self.apex_child_sessions.last().ok_or(SdvStatus::with_message(
            SdvStatusCode::FailedPrecondition,
            "Must submit at least one APEX",
        ))?;
        let largest_child_session_id = last_child_session.session_id;

        if largest_child_session_id < self.parent_session_id {
            return Err(SdvStatus::with_message(
                SdvStatusCode::ResourceExhausted,
                "Not enough remaining free sessions",
            ));
        }

        let mut created_dirs: Vec<i32> = Vec::new();
        let result = copy_apexes_and_submit_session(
            &self.apex_child_sessions,
            apex_service,
            app_staging_dir,
            self.parent_session_id,
            &mut created_dirs,
        )
        .await;

        if result.is_err() {
            for id in created_dirs {
                let created_dir = session_id_to_session_directory_path(app_staging_dir, id);
                if let Err(err) = tokio::fs::remove_dir_all(&created_dir).await {
                    error!(
                        "Failed to remove session directory {} after a submit error: {}",
                        created_dir.display(),
                        err
                    );
                };
            }
        }

        result
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum UpdateMetadata {
    System(SystemUpdateMetadata),
    ServiceBundle(ServiceBundleUpdateMetadata),
}

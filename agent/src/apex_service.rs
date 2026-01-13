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

use apex_aidl_interface::aidl::android::apex::ApexSessionParams::ApexSessionParams;
use apex_aidl_interface::aidl::android::apex::IApexService::IApexServiceAsync;
use apex_aidl_interface::aidl::android::apex::{
    ApexInfo::ApexInfo, ApexInfoList::ApexInfoList, ApexSessionInfo::ApexSessionInfo,
};
use async_trait::async_trait;
use binder::Strong;
use binder_tokio::Tokio;
use mockall::automock;
use sdv_status_rs::{derive, SdvResult, SdvStatus, SdvStatusCode};

#[automock]
#[async_trait]
pub trait ApexService: Send + Sync {
    /// Submit a staged session of APEXes. Each APEX should be placed in a unique session
    /// directory with the form "session_<id>". The ids of these directories should be
    /// passed to "child_session_ids". Finally, an empty "parent" directory should be
    /// created and the id of this directory should be passed to "session_id".
    async fn submit_staged_session(
        &self,
        session_id: i32,
        child_session_ids: &[i32],
    ) -> SdvResult<ApexInfoList>;

    /// Mark a staged session as ready to be activated on the next reboot.
    async fn mark_staged_session_ready(&self, session_id: i32) -> SdvResult<()>;

    /// Mark a staged session as successfully activated.
    async fn mark_staged_session_successful(&self, session_id: i32) -> SdvResult<()>;

    /// Abort a session that has been staged. This must be called after the session is
    /// staged but before the system is rebooted and activated.
    async fn abort_staged_session(&self, session_id: i32) -> SdvResult<()>;

    /// Revert an active session. This should be called after a session has been activated
    /// after a system reboot.
    async fn revert_active_sessions(&self) -> SdvResult<()>;

    /// Returns the session info for a staged session.
    async fn get_staged_session_info(&self, session_id: i32) -> SdvResult<ApexSessionInfo>;

    /// Get all of the active APEX packages.
    async fn get_active_packages(&self) -> SdvResult<Vec<ApexInfo>>;

    /// Mark the boot as successful. Allows the ApexService to cleanup the session directories.
    async fn mark_boot_successful(&self) -> SdvResult<()>;

    /// Schedules the deletion of active APEXes in /data/apex/active directory on the next reboot.
    async fn unstage_packages(&self, active_package_paths: &[String]) -> SdvResult<()>;
}

pub struct BinderApexService {
    apex_service: Strong<dyn IApexServiceAsync<Tokio>>,
}

impl BinderApexService {
    pub async fn new() -> SdvResult<Self> {
        const DESCRIPTOR: &str = "apexservice";

        let apex_service: Strong<dyn IApexServiceAsync<Tokio>> =
            binder_tokio::wait_for_interface(DESCRIPTOR).await.map_err(|e| {
                SdvStatus::with_message(
                    SdvStatusCode::Unavailable,
                    format!("Failed to find IApexService interface: {e}"),
                )
            })?;

        Ok(BinderApexService { apex_service })
    }
}

#[async_trait]
impl ApexService for BinderApexService {
    async fn submit_staged_session(
        &self,
        session_id: i32,
        child_session_ids: &[i32],
    ) -> SdvResult<ApexInfoList> {
        let params = ApexSessionParams {
            sessionId: session_id,
            childSessionIds: child_session_ids.to_vec(),
            ..Default::default()
        };

        let mut packages = ApexInfoList::default();

        self.apex_service.submitStagedSession(&params, &mut packages).await.map_err(derive!(
            SdvStatusCode::Internal,
            format!("IApexService::submitStagedSession({}) failed", session_id)
        ))?;

        Ok(packages)
    }

    async fn mark_staged_session_ready(&self, session_id: i32) -> SdvResult<()> {
        self.apex_service.markStagedSessionReady(session_id).await.map_err(derive!(
            SdvStatusCode::Internal,
            format!("IApexService::markStagedSessionReady({}) failed", session_id)
        ))
    }

    async fn mark_staged_session_successful(&self, session_id: i32) -> SdvResult<()> {
        self.apex_service.markStagedSessionSuccessful(session_id).await.map_err(derive!(
            SdvStatusCode::Internal,
            format!("IApexService::markStagedSessionSuccessful({}) failed", session_id)
        ))
    }

    async fn abort_staged_session(&self, session_id: i32) -> SdvResult<()> {
        self.apex_service.abortStagedSession(session_id).await.map_err(derive!(
            SdvStatusCode::Internal,
            format!("IApexService::abortStagedSession({}) failed", session_id)
        ))
    }

    async fn revert_active_sessions(&self) -> SdvResult<()> {
        self.apex_service.revertActiveSessions().await.map_err(derive!(
            SdvStatusCode::Internal,
            "IApexService::revertActiveSessions() failed"
        ))
    }

    async fn get_staged_session_info(&self, session_id: i32) -> SdvResult<ApexSessionInfo> {
        self.apex_service.getStagedSessionInfo(session_id).await.map_err(derive!(
            SdvStatusCode::Internal,
            format!("IApexService::getStagedSessionInfo({}) failed", session_id)
        ))
    }

    async fn get_active_packages(&self) -> SdvResult<Vec<ApexInfo>> {
        self.apex_service.getActivePackages().await.map_err(derive!(
            SdvStatusCode::Internal,
            format!("IApexService::getActivePackages() failed")
        ))
    }

    async fn mark_boot_successful(&self) -> SdvResult<()> {
        self.apex_service.markBootCompleted().await.map_err(derive!(
            SdvStatusCode::Internal,
            format!("IApexService::markBootCompleted() failed")
        ))
    }

    async fn unstage_packages(&self, active_package_paths: &[String]) -> SdvResult<()> {
        self.apex_service.unstagePackages(active_package_paths).await.map_err(derive!(
            SdvStatusCode::Internal,
            format!("IApexService::unstagePackages({:#?}) failed", active_package_paths)
        ))
    }
}

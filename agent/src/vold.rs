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

use async_trait::async_trait;
use mockall::automock;
use sdv_status_rs::{derive, SdvResult, SdvStatus, SdvStatusCode};
use tokio::process::Command;

const VDC_COMMAND: &str = "/system/bin/vdc";

#[automock]
#[async_trait]
pub trait VoldService: Send + Sync {
    /// Start the file system checkpoint on the next boot. The "boot_attempts"
    /// parameter controls how many successive boots to leave the checkpoint enabled.
    /// If no more boot attempts remain, then the checkpoint is disabled and user data
    /// will be persisted once again.
    ///
    /// If this is called with a "boot_attempts" value of 0, then the checkpoint will
    /// be disabled on the next boot.
    async fn start_checkpoint(&self, boot_attempts: i32) -> SdvResult<()>;

    /// Commit the user data changes to permanent storage. If the filesystem is not in
    /// checkpoint mode, this does not have an effect.
    ///
    /// If the filesystem is in checkpoint mode, it will also mark the current boot slot as
    /// successful.
    async fn commit_changes(&self) -> SdvResult<()>;
}

pub struct VdcVoldService {}

impl VdcVoldService {
    pub fn new() -> Self {
        VdcVoldService {}
    }
}

// The VdcVoldService requires that the calling user is either root (UID 0) or system (UID 1000).
#[async_trait]
impl VoldService for VdcVoldService {
    async fn start_checkpoint(&self, boot_attempts: i32) -> SdvResult<()> {
        let vdc_output = Command::new(VDC_COMMAND)
            .args(["checkpoint", "startCheckpoint", &boot_attempts.to_string()])
            .output()
            .await
            .map_err(derive!(
                SdvStatusCode::Internal,
                "Failed to start startCheckpoint through vdc"
            ))?;

        if !vdc_output.status.success() {
            return Err(SdvStatus::with_message(
                SdvStatusCode::Internal,
                format!("startCheckpoint failed: {vdc_output:?}"),
            ));
        }

        Ok(())
    }

    async fn commit_changes(&self) -> SdvResult<()> {
        let vdc_output = Command::new(VDC_COMMAND)
            .args(["checkpoint", "commitChanges"])
            .output()
            .await
            .map_err(derive!(
                SdvStatusCode::Internal,
                "Failed to start commitChanges through vdc"
            ))?;

        if !vdc_output.status.success() {
            return Err(SdvStatus::with_message(
                SdvStatusCode::Internal,
                format!("commitChanges failed: {vdc_output:?}"),
            ));
        }

        Ok(())
    }
}

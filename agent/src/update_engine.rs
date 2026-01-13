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
use binder::{
    BinderFeatures, ExceptionCode, Interface, ParcelFileDescriptor, Result as BinderResult, Strong,
};
use binder_tokio::Tokio;
use libupdate_engine_aidl_interface::aidl::android::os::IUpdateEngine::IUpdateEngineAsync;
use libupdate_engine_aidl_interface::aidl::android::os::IUpdateEngineCallback::{
    BnUpdateEngineCallback, IUpdateEngineCallback,
};
use log::{debug, error};
use sdv_status_rs::{derive, SdvResult, SdvStatus, SdvStatusCode};
use std::fs::File;
use std::sync::{Arc, Condvar, Mutex};
use std::time::Duration;
use tokio::sync::mpsc::UnboundedSender;

// Map an error from UpdateEngine to an SdvResult.
// Returns a SdvStatusCode::FailedPrecondition if UpdateEngine returns a SERVICE_SPECIFIC error
// (typically a logical error).
// Returns a SdvStatusCode::Internal for any other type of error (eg. permissions).
// Example usage: .map_err(map_update_engine_error!("Update isn't running", "Request error"))
macro_rules! map_update_engine_error {
    ($failed_precondition_message:expr, $internal_failure_message:expr) => {
        |e| {
            if e.exception_code() == ExceptionCode::SERVICE_SPECIFIC
                && e.service_specific_error() == 1
            {
                return SdvStatus::with_cause_and_message(
                    SdvStatusCode::FailedPrecondition,
                    e,
                    $failed_precondition_message,
                );
            }

            SdvStatus::with_cause_and_message(SdvStatusCode::Internal, e, $internal_failure_message)
        }
    };
}

pub mod error_code {
    pub const SUCCESS: i32 = 0;
    pub const USER_CANCELED: i32 = 48;
    pub const UPDATED_BUT_NOT_ACTIVE: i32 = 52;
}

pub mod status_code {
    pub const STATUS_UNSET: i32 = -1;
    pub const IDLE: i32 = 0;
}

pub enum UpdateEngineMessage {
    StatusUpdate { status_code: i32, percentage: f32 },
    PayloadApplicationComplete { error_code: i32 },
}

pub struct UpdateEngineCallbacks {
    last_status: Arc<(Mutex<i32>, Condvar)>,
    sender: UnboundedSender<UpdateEngineMessage>,
}

impl UpdateEngineCallbacks {
    fn new(
        sender: UnboundedSender<UpdateEngineMessage>,
        last_status: Arc<(Mutex<i32>, Condvar)>,
    ) -> Self {
        UpdateEngineCallbacks { last_status, sender }
    }

    pub fn on_status_update(&self, status_code: i32, percentage: f32) {
        debug!("onStatusUpdate: {status_code}, {percentage}%");

        {
            let (lock, condvar) = &*self.last_status;
            let mut last_status = lock.lock().expect("Could not lock on the last_status");
            let first_status = *last_status == status_code::STATUS_UNSET;
            *last_status = status_code;

            if first_status {
                condvar.notify_one();
            }
        }

        self.sender
            .send(UpdateEngineMessage::StatusUpdate { status_code, percentage })
            .unwrap_or_else(|e| {
                error!("Failed to send UpdateEngineMessage::StatusUpdate: {e:?}");
            });
    }

    pub fn on_payload_application_complete(&self, error_code: i32) {
        debug!("onPayloadApplicationComplete: {error_code}");

        self.sender
            .send(UpdateEngineMessage::PayloadApplicationComplete { error_code })
            .unwrap_or_else(|e| {
                error!("Failed to send UpdateEngineMessage::PayloadApplicationComplete: {e:?}");
            });
    }
}

struct BinderUpdateEngineCallbacks {
    callbacks: UpdateEngineCallbacks,
}

impl Interface for BinderUpdateEngineCallbacks {}

impl IUpdateEngineCallback for BinderUpdateEngineCallbacks {
    fn onStatusUpdate(&self, status_code: i32, percentage: f32) -> BinderResult<()> {
        self.callbacks.on_status_update(status_code, percentage);

        Ok(())
    }

    fn onPayloadApplicationComplete(&self, error_code: i32) -> BinderResult<()> {
        self.callbacks.on_payload_application_complete(error_code);

        Ok(())
    }
}

#[async_trait]
pub trait UpdateEngine: Send + Sync {
    async fn bind_callbacks(&self, callbacks: UpdateEngineCallbacks) -> SdvResult<()>;

    async fn apply_payload_fd(
        &self,
        payload: &str,
        payload_offset: i64,
        payload_size: i64,
        header_key_value_pairs: &[String],
    ) -> SdvResult<()>;

    async fn cancel(&self) -> SdvResult<()>;

    async fn reset_status(&self) -> SdvResult<()>;

    async fn resume(&self) -> SdvResult<()>;

    async fn suspend(&self) -> SdvResult<()>;
}

pub struct BinderUpdateEngine {
    update_engine_service: Strong<dyn IUpdateEngineAsync<Tokio>>,
}

impl BinderUpdateEngine {
    pub async fn new() -> SdvResult<Self> {
        const DESCRIPTOR: &str = "android.os.UpdateEngineService";

        let update_engine_service: Strong<dyn IUpdateEngineAsync<Tokio>> =
            binder_tokio::wait_for_interface(DESCRIPTOR).await.map_err(|e| {
                SdvStatus::with_message(
                    SdvStatusCode::Unavailable,
                    format!("Failed to find IUpdateEngine interface: {e}"),
                )
            })?;

        Ok(BinderUpdateEngine { update_engine_service })
    }
}

#[async_trait]
impl UpdateEngine for BinderUpdateEngine {
    async fn bind_callbacks(&self, callbacks: UpdateEngineCallbacks) -> SdvResult<()> {
        let update_engine_callbacks = BnUpdateEngineCallback::new_binder(
            BinderUpdateEngineCallbacks { callbacks },
            BinderFeatures::default(),
        );

        let result: bool =
            self.update_engine_service.bind(&update_engine_callbacks).await.map_err(|e| {
                SdvStatus::with_message(
                    SdvStatusCode::Internal,
                    format!("Failed to bind update_engine callback: {e}"),
                )
            })?;

        if !result {
            return Err(SdvStatus::with_message(
                SdvStatusCode::AlreadyExists,
                "update_engine callback already bound",
            ));
        }

        Ok(())
    }

    async fn apply_payload_fd(
        &self,
        payload: &str,
        payload_offset: i64,
        payload_size: i64,
        header_key_value_pairs: &[String],
    ) -> SdvResult<()> {
        let file = File::open(payload).map_err(|e| {
            SdvStatus::with_message(
                SdvStatusCode::Internal,
                format!("Failed to open payload: {e:?}"),
            )
        })?;

        let pfd = ParcelFileDescriptor::new(file);

        self.update_engine_service
            .applyPayloadFd(&pfd, payload_offset, payload_size, header_key_value_pairs)
            .await
            .map_err(|e| {
                SdvStatus::with_message(
                    SdvStatusCode::Internal,
                    format!("Failed to apply payload: {e}"),
                )
            })?;

        Ok(())
    }

    async fn cancel(&self) -> SdvResult<()> {
        self.update_engine_service.cancel().await.map_err(map_update_engine_error!(
            "No ongoing update to cancel",
            "Failed to cancel update"
        ))
    }

    async fn reset_status(&self) -> SdvResult<()> {
        self.update_engine_service
            .resetStatus()
            .await
            .map_err(derive!(SdvStatusCode::Internal, "Failed to reset status"))
    }

    async fn resume(&self) -> SdvResult<()> {
        self.update_engine_service.resume().await.map_err(map_update_engine_error!(
            "No ongoing update to resume",
            "Failed to resume update"
        ))
    }

    async fn suspend(&self) -> SdvResult<()> {
        self.update_engine_service.suspend().await.map_err(map_update_engine_error!(
            "No ongoing update to suspend",
            "Failed to suspend update"
        ))
    }
}

pub struct UpdateEngineClient {
    last_status: Arc<(Mutex<i32>, Condvar)>,
    update_engine: Box<dyn UpdateEngine>,
}

impl UpdateEngineClient {
    pub async fn new(
        update_engine: Box<dyn UpdateEngine>,
        sender: UnboundedSender<UpdateEngineMessage>,
        initial_status_timeout: Duration,
    ) -> SdvResult<Self> {
        let last_status = Arc::new((Mutex::new(status_code::STATUS_UNSET), Condvar::new()));
        let callbacks = UpdateEngineCallbacks::new(sender, last_status.clone());
        update_engine.bind_callbacks(callbacks).await?;

        {
            let (lock, condvar) = &*last_status;
            let mut last_status_value = lock.lock().expect("Could not lock on the last_status");

            if *last_status_value == status_code::STATUS_UNSET {
                debug!("Waiting for UpdateEngine to send initial status");

                // We don't want to wait forever for this status. If there is an issue with the
                // update that prevents UpdateEngine from sending the status, then we want to be
                // able to rollback the update. In order to do that we need to finish initializing
                // the Update Manager.
                let (new_last_status_value, wait_timeout_result) = condvar
                    .wait_timeout(last_status_value, initial_status_timeout)
                    .expect("Could not perform the wait_timeout");
                last_status_value = new_last_status_value;

                if wait_timeout_result.timed_out() {
                    error!("Failed to receive status from UpdateEngine after binding callbacks. This could indicate a critical error with the service.");
                }
            }

            debug!("UpdateEngine initial status: {}", *last_status_value);
        }

        Ok(UpdateEngineClient { last_status, update_engine })
    }

    pub async fn apply_payload_fd(
        &self,
        payload: &str,
        payload_offset: i64,
        payload_size: i64,
        header_key_value_pairs: &[String],
    ) -> SdvResult<()> {
        self.update_engine
            .apply_payload_fd(payload, payload_offset, payload_size, header_key_value_pairs)
            .await
    }

    pub async fn cancel(&self) -> SdvResult<()> {
        self.update_engine.cancel().await
    }

    pub async fn reset_status(&self) -> SdvResult<()> {
        self.update_engine.reset_status().await
    }

    pub async fn resume(&self) -> SdvResult<()> {
        self.update_engine.resume().await
    }

    pub async fn suspend(&self) -> SdvResult<()> {
        self.update_engine.suspend().await
    }

    pub fn get_last_status(&self) -> i32 {
        let (lock, _) = &*self.last_status;
        *lock.lock().expect("Could not lock on the last_status")
    }
}

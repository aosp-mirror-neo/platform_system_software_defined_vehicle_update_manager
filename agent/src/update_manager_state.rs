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

use crate::atomic_file::AtomicFile;
use crate::atomic_file::AtomicFileImpl;
use crate::boot_control_service::get_opposite_slot;
use crate::metadata::{
    session_id_to_session_directory_path, ApexInfo, ServiceBundleUpdateMetadata,
    SystemUpdateMetadata, UpdateMetadata,
};
use crate::update_engine::error_code::{SUCCESS, UPDATED_BUT_NOT_ACTIVE, USER_CANCELED};
use crate::update_engine::status_code::{IDLE, STATUS_UNSET};
use crate::update_manager_service_impl::UpdateManagerEnvironment;
use async_trait::async_trait;
use log::{debug, error, warn};
use protobuf::Message;
use sdv_status_rs::{derive, SdvResult, SdvStatus, SdvStatusCode};
use std::collections::HashMap;
use std::error::Error;
use std::fs::remove_dir_all;
use std::path::Path;
use std::sync::Arc;
use update_manager_api::{
    error_code::ErrorCode, update_manager_context::UpdateManagerContext,
    update_state::UpdateState as ApiUpdateState,
};

/// Check whether the given UpdateMetadata matches the given variant.
/// Example usage: assert!(check_metadata(UpdateMetadata::System { /* SystemUpdatePayload */ }, System))
#[macro_export]
macro_rules! check_metadata {
    ($enum_var:expr, $variant:ident) => {
        matches!($enum_var, UpdateMetadata::$variant(_))
    };
}

#[derive(Clone, Debug, PartialEq)]
pub enum UpdateManagerEvent {
    // Event used to trigger the state machine recovery behavior
    Init(bool),
    Reset,
    PrepareRequest(UpdateMetadata),
    ActivateRequest,
    ActivateSuccessful,
    RollbackRequest,
    CommitRequest,
    UpdateEngineError(String),
    UpdateEngineCode(i32),
    UpdateEngineSuspended,
    // Generic successful result from the ApexService
    ApexServiceSuccess,
    // Submit successful result, returning info on the submitted APEXes
    ApexServiceSubmitSuccess(Vec<ApexInfo>),
    ApexServiceError(String),
    BootControlServiceError(String),
    VoldServiceError(String),
    RollbackComplete,
    CommitComplete,
    SuspendRequest,
    ResumeRequest,
    PowerOffPostFinish,
    SuspendToRamPostFinish,
    SuspendToDiskPostFinish,
}

#[derive(Clone)]
pub enum UpdateState {
    Ready(Arc<dyn UpdateManagerState>),
    InProgress(Arc<dyn UpdateManagerStateMetadata>),
    Failed(FailureState),
}

#[derive(Clone)]
pub struct FailureState {
    pub state: Arc<dyn UpdateManagerStateMetadata>,
    pub reason: FailureReason,
}

#[derive(Clone, Debug, PartialEq)]
pub struct FailureReason {
    pub error_code: ErrorCode,
    pub error_message: String,
}

impl FailureReason {
    pub fn new(error_code: ErrorCode, error_message: String) -> Self {
        Self { error_code, error_message }
    }
}

/// A trait for the update manager state.
#[async_trait]
pub trait UpdateManagerState: Sync + Send {
    /// Gets the value of the UpdateManager state API enum.
    fn value(&self) -> ApiUpdateState;

    /// Calculates the next state based on the event.
    fn get_next_state(&self, event: UpdateManagerEvent) -> SdvResult<UpdateState>;

    /// This function is called once when the state machine transitions to this state. It can
    /// return an optional event which will be used to trigger another transition. If no event is
    /// returned, no further transitions will occur.
    async fn on_enter(&self, _env: Arc<UpdateManagerEnvironment>) -> Option<UpdateManagerEvent> {
        None
    }

    /// This function is called when the state machine is being initialized and the state has been
    /// recovered from disk. It is only called for this initial state. If this function returns an
    /// optional event to trigger another transition, this next state will have its "on_enter"
    /// function called, not "on_recovery". If no event is returned, no further transitions will occur.
    ///
    /// The default implementation calls "on_enter"
    async fn on_recovery(&self, env: Arc<UpdateManagerEnvironment>) -> Option<UpdateManagerEvent> {
        self.on_enter(env).await
    }
}

fn event_matches_metadata_type(
    metadata: &UpdateMetadata,
    event: &UpdateManagerEvent,
    state: ApiUpdateState,
) -> SdvResult<()> {
    match event {
        UpdateManagerEvent::ApexServiceSuccess
        | UpdateManagerEvent::ApexServiceSubmitSuccess(_)
        | UpdateManagerEvent::ApexServiceError(_) => {
            if let UpdateMetadata::System(_) = metadata {
                return Err(SdvStatus::with_message(
                    SdvStatusCode::FailedPrecondition,
                    format!(
                        "Received an {event:?} while handling a system update in state {state:?}"
                    ),
                ));
            }
        }
        UpdateManagerEvent::UpdateEngineError(_) | UpdateManagerEvent::UpdateEngineCode(_) => {
            if let UpdateMetadata::ServiceBundle(_) = metadata {
                return Err(SdvStatus::with_message(
                    SdvStatusCode::FailedPrecondition,
                    format!(
                        "Received an {event:?} while handling a service bundle update in state {state:?}"
                    ),
                ));
            }
        }
        _ => (),
    }

    Ok(())
}

async fn handle_activate_rollback_on_enter(
    env: Arc<UpdateManagerEnvironment>,
    metadata: &UpdateMetadata,
) -> Option<UpdateManagerEvent> {
    match metadata {
        UpdateMetadata::System(system) => {
            if let Err(e) = env.update_engine_client.reset_status().await {
                // TODO b/381946548 transition to a rollback failure state. For now log the
                // error and treat the rollback as successful
                error!("Failed to reset update engine status: {e:?}");
            }

            // TODO b/381946548 transition to a rollback failure state. For now log the
            // error and treat the rollback as successful
            if let Err(e) = env.boot_control_service.revert_to_slot(system.initial_boot_slot).await
            {
                error!("{e}");
            }
        }
        UpdateMetadata::ServiceBundle(_service_bundle) => {
            // TODO b/381946548 transition to a rollback failure state. For now log the
            // error and treat the rollback as successful
            if let Err(e) = env.apex_service.revert_active_sessions().await {
                error!("Failed when reverting the active sessions: {e:?}");
            }

            // TODO b/381946548 transition to a rollback failure state. For now log the
            // error and treat the rollback as successful
            //
            // Calling start_checkpoint with an argument of 0 disables the checkpoint on the next
            // boot without committing the changes.
            if let Err(e) = env.vold_service.start_checkpoint(0).await {
                error!("Failed when disabling the checkpoint: {e:?}");
            }
        }
    }

    Some(UpdateManagerEvent::RollbackComplete)
}

async fn check_submitted_apexes_active(
    env: Arc<UpdateManagerEnvironment>,
    submitted_apex_info: &[ApexInfo],
) -> SdvResult<bool> {
    let active_apexes = env
        .apex_service
        .get_active_packages()
        .await
        .map_err(derive!(SdvStatusCode::Internal, "Failed to get the active APEXes"))?;

    let active_apex_map = HashMap::<String, i64>::from_iter(
        active_apexes
            .into_iter()
            .map(|active_apex| (active_apex.moduleName, active_apex.versionCode)),
    );

    let all_are_active = submitted_apex_info.iter().all(|submitted_apex| {
        active_apex_map.get(&submitted_apex.module_name).is_some_and(|active_apex_version_code| {
            submitted_apex.version_code == *active_apex_version_code
        })
    });

    Ok(all_are_active)
}

fn delete_staged_apexes(
    app_staging_dir: &Path,
    metadata: &ServiceBundleUpdateMetadata,
) -> SdvResult<()> {
    let mut sessions_not_deleted = Vec::<String>::new();
    let mut sessions_to_delete: Vec<i32> =
        metadata.apex_child_sessions.iter().map(|x| x.session_id).collect();
    sessions_to_delete.push(metadata.parent_session_id);

    for session_to_delete in sessions_to_delete {
        let session_dir = session_id_to_session_directory_path(app_staging_dir, session_to_delete);
        if let Err(e) = remove_dir_all(&session_dir) {
            sessions_not_deleted.push(format!("session {session_to_delete}: {e:?}"));
        }
    }

    if !sessions_not_deleted.is_empty() {
        let error_message = sessions_not_deleted.join(", ");
        return Err(SdvStatus::with_message(
            SdvStatusCode::Internal,
            format!("Failed to delete staged session directories: {error_message}"),
        ));
    }

    Ok(())
}

fn is_update_engine_idle(status_code: i32) -> bool {
    if status_code == STATUS_UNSET {
        debug!("UpdateEngine has not sent a status code, assuming the service is idle");
    }

    status_code == IDLE || status_code == STATUS_UNSET
}

fn send_power_state_report_processing_complete(env: Arc<UpdateManagerEnvironment>) {
    // VPM will proceed with the power state transition after a set timeout, so treat a failure
    // to notify VPM about the completed PowerStateReport processing as just a warning.
    if let Err(e) = env.power_state_client.power_state_report_processing_complete() {
        warn!("Failed to complete processing of PowerStateReport: {e:?}");
    }
}

// Needed for clippy lint
impl Default for UpdateState {
    fn default() -> Self {
        Self::new()
    }
}

impl UpdateState {
    pub fn new() -> Self {
        UpdateState::Ready(Arc::new(Ready {}))
    }

    pub fn load(path: &Path) -> SdvResult<Self> {
        let atomic_file = get_atomic_file(path)?;
        let bytes =
            atomic_file.read_all().map_err(|e| sdv_error("Failed to read state from disk", e))?;
        let update_manager_context = UpdateManagerContext::parse_from_bytes(&bytes)
            .map_err(|e| sdv_error("Failed to pares stored state", e))?;
        let state: UpdateState = update_manager_context.try_into()?;
        Ok(state)
    }

    pub fn save(&self, path: &Path) -> SdvResult<()> {
        let update_manager_context: UpdateManagerContext = self.into();
        let state_bytes = update_manager_context
            .write_to_bytes()
            .map_err(|e| sdv_error("Failed to serialize state to proto", e))?;
        get_atomic_file(path)?.write_all(&state_bytes).map_err(|e| {
            sdv_error(
                "

            state.on_recover(Failed to write state to disk",
                e,
            )
        })?;
        Ok(())
    }

    pub fn metadata(&self) -> Option<&UpdateMetadata> {
        match self {
            UpdateState::Ready(_) => None,
            UpdateState::InProgress(update_manager_state_metadata) => {
                Some(update_manager_state_metadata.metadata())
            }
            UpdateState::Failed(failure_state) => Some(failure_state.state.metadata()),
        }
    }

    pub fn failure_reason(&self) -> Option<&FailureReason> {
        match self {
            UpdateState::Ready(_) => None,
            UpdateState::InProgress(_) => None,
            UpdateState::Failed(failure_state) => Some(&failure_state.reason),
        }
    }
}

fn sdv_error(message: &str, cause: impl Error + Send + Sync + 'static) -> SdvResult<()> {
    Err(SdvStatus::with_cause_and_message(SdvStatusCode::InvalidArgument, cause, message))
}

fn get_atomic_file(path: &Path) -> SdvResult<AtomicFileImpl> {
    Ok(AtomicFileImpl::new(path).map_err(|e| sdv_error("Failed to read state from disk", e))?)
}

#[async_trait]
impl UpdateManagerState for UpdateState {
    fn value(&self) -> ApiUpdateState {
        match self {
            UpdateState::Ready(state) => state.value(),
            UpdateState::InProgress(state) => state.value(),
            UpdateState::Failed(failure_state) => failure_state.state.value(),
        }
    }

    fn get_next_state(&self, event: UpdateManagerEvent) -> SdvResult<UpdateState> {
        match self {
            UpdateState::Ready(state) => state.get_next_state(event),
            UpdateState::InProgress(state) => state.get_next_state(event),
            UpdateState::Failed(failure_state) => failure_state.state.get_next_state(event),
        }
    }

    async fn on_enter(&self, env: Arc<UpdateManagerEnvironment>) -> Option<UpdateManagerEvent> {
        match self {
            UpdateState::Ready(state) => state.on_enter(env).await,
            UpdateState::InProgress(state) => state.on_enter(env).await,
            UpdateState::Failed(failure_state) => failure_state.state.on_enter(env).await,
        }
    }

    async fn on_recovery(&self, env: Arc<UpdateManagerEnvironment>) -> Option<UpdateManagerEvent> {
        match self {
            UpdateState::Ready(state) => state.on_recovery(env).await,
            UpdateState::InProgress(state) => state.on_recovery(env).await,
            UpdateState::Failed(failure_state) => failure_state.state.on_recovery(env).await,
        }
    }
}

pub trait UpdateManagerStateMetadata: UpdateManagerState + Sync + Send {
    /// Gets the metadata of the update process.
    fn metadata(&self) -> &UpdateMetadata;

    /// Gets an SdvStatus declaring the given event unexpected given the state's metadata type
    fn create_unexpected_event_for_metadata_sdv_status(
        &self,
        event: &UpdateManagerEvent,
    ) -> SdvStatus {
        SdvStatus::with_message(
            SdvStatusCode::FailedPrecondition,
            format!(
                "State {:?} with metadata {:?} does not expect {:?}",
                self.value(),
                self.metadata(),
                event
            ),
        )
    }
}

pub struct Ready {}

#[async_trait]
impl UpdateManagerState for Ready {
    fn value(&self) -> ApiUpdateState {
        ApiUpdateState::READY
    }
    fn get_next_state(&self, event: UpdateManagerEvent) -> SdvResult<UpdateState> {
        match event {
            UpdateManagerEvent::PrepareRequest(metadata) => {
                Ok(UpdateState::InProgress(Arc::new(Prepare { metadata })))
            }
            _ => Err(SdvStatus::with_message(
                SdvStatusCode::FailedPrecondition,
                format!("State {:?} does not expect {:?}", self.value(), event),
            )),
        }
    }

    async fn on_enter(&self, env: Arc<UpdateManagerEnvironment>) -> Option<UpdateManagerEvent> {
        let result = env.apex_service.mark_boot_successful().await;

        if let Err(e) = result {
            // TODO b/382312003 determine if there is a better way to log non-critical failures
            // This is not a critical failure. This function is mostly used to ensure the
            // ApexService session directories are cleaned up after an update. The APEXes will still
            // activate on each boot without this succeeding.
            warn!(
                "Failed to indicate to ApexService that the boot was successful, updated APEXes will still be activated: {e:?}",
            );
        }

        None
    }
}

#[derive(Clone)]
pub struct Prepare {
    pub metadata: UpdateMetadata,
}

#[async_trait]
impl UpdateManagerState for Prepare {
    fn value(&self) -> ApiUpdateState {
        ApiUpdateState::PREPARE
    }
    fn get_next_state(&self, event: UpdateManagerEvent) -> SdvResult<UpdateState> {
        event_matches_metadata_type(self.metadata(), &event, self.value())?;

        match event {
            UpdateManagerEvent::ApexServiceSubmitSuccess(apex_infos) => {
                let mut metadata = self.metadata.clone();

                // The call to "event_matches_metadata_type" above ensures that this condition is
                // always true.
                if let UpdateMetadata::ServiceBundle(ref mut service_bundle) = &mut metadata {
                    service_bundle.submitted_apex_info = apex_infos;
                }

                Ok(UpdateState::InProgress(Arc::new(PrepareComplete { metadata })))
            }
            UpdateManagerEvent::ApexServiceError(_) => {
                error!("Received {event:?}");

                Ok(UpdateState::Failed(FailureState {
                    state: Arc::new(PrepareFailure { metadata: self.metadata.clone() }),
                    reason: event.into(),
                }))
            }
            UpdateManagerEvent::UpdateEngineError(_) => Ok(UpdateState::Failed(FailureState {
                state: Arc::new(PrepareFailure { metadata: self.metadata.clone() }),
                reason: event.into(),
            })),
            UpdateManagerEvent::UpdateEngineCode(code) => {
                if code != UPDATED_BUT_NOT_ACTIVE {
                    error!("Received an unexpected UpdateManagerEvent::UpdateEngineCode({code})");
                    return Ok(UpdateState::Failed(FailureState {
                        state: Arc::new(PrepareFailure { metadata: self.metadata.clone() }),
                        reason: event.into(),
                    }));
                }

                Ok(UpdateState::InProgress(Arc::new(PrepareComplete {
                    metadata: self.metadata.clone(),
                })))
            }
            UpdateManagerEvent::RollbackRequest => {
                if !check_metadata!(self.metadata, System) {
                    return Err(self.create_unexpected_event_for_metadata_sdv_status(&event));
                }

                Ok(UpdateState::InProgress(Arc::new(PrepareCancel {
                    metadata: self.metadata.clone(),
                })))
            }
            UpdateManagerEvent::SuspendRequest
            | UpdateManagerEvent::PowerOffPostFinish
            | UpdateManagerEvent::SuspendToRamPostFinish
            | UpdateManagerEvent::SuspendToDiskPostFinish => {
                if !check_metadata!(self.metadata, System) {
                    return Err(self.create_unexpected_event_for_metadata_sdv_status(&event));
                }

                Ok(UpdateState::InProgress(Arc::new(PrepareSuspend {
                    metadata: self.metadata.clone(),
                })))
            }
            _ => Err(SdvStatus::with_message(
                SdvStatusCode::FailedPrecondition,
                format!("State {:?} does not expect {:?}", self.value(), event),
            )),
        }
    }
    async fn on_enter(&self, env: Arc<UpdateManagerEnvironment>) -> Option<UpdateManagerEvent> {
        match &self.metadata {
            UpdateMetadata::System(system_update_metadata) => {
                if is_update_engine_idle(env.update_engine_client.get_last_status()) {
                    let system_payload = &system_update_metadata.payload;
                    if let Err(e) = system_payload.apply(&env.update_engine_client, false).await {
                        return Some(UpdateManagerEvent::UpdateEngineError(e.to_string()));
                    }
                } else {
                    debug!("Attempting to resume the update.");

                    // UpdateEngine may or may not still be processing the update. There isn't an
                    // API that we can query to determine this. Calling resume on an update that
                    // is not suspended will result in an error. We can expect this to fail in
                    // this way and simply log the error.
                    if let Err(e) = env.update_engine_client.resume().await {
                        if e.status_code() != SdvStatusCode::FailedPrecondition {
                            return Some(UpdateManagerEvent::UpdateEngineError(e.to_string()));
                        }

                        debug!("There is no UpdateEngine update to resume. This may be because the Update Manager service was restarted while an update was running.");
                    }
                }

                None
            }
            UpdateMetadata::ServiceBundle(service_bundle_update_metadata) => {
                let result = service_bundle_update_metadata
                    .submit(env.apex_service.clone(), &env.app_staging_dir)
                    .await;

                let event = match result {
                    Ok(apex_infos) => UpdateManagerEvent::ApexServiceSubmitSuccess(apex_infos),
                    Err(e) => UpdateManagerEvent::ApexServiceError(e.to_string()),
                };

                Some(event)
            }
        }
    }
}

impl UpdateManagerStateMetadata for Prepare {
    fn metadata(&self) -> &UpdateMetadata {
        &self.metadata
    }
}

#[derive(Clone)]
pub struct PrepareSuspend {
    pub metadata: UpdateMetadata,
}

#[async_trait]
impl UpdateManagerState for PrepareSuspend {
    fn value(&self) -> ApiUpdateState {
        ApiUpdateState::PREPARE_SUSPEND
    }

    // NOTE: Each of the possible end states need to call send_power_state_report_processing_complete()
    // to ensure VPM can complete its current stage without timing out. If a new end state is
    // added, be sure to add a call to this function in its on_enter() function.
    fn get_next_state(&self, event: UpdateManagerEvent) -> SdvResult<UpdateState> {
        if !check_metadata!(self.metadata, System) {
            return Err(self.create_unexpected_event_for_metadata_sdv_status(&event));
        }

        match event {
            UpdateManagerEvent::UpdateEngineError(_) => Ok(UpdateState::Failed(FailureState {
                state: Arc::new(PrepareFailure { metadata: self.metadata.clone() }),
                reason: event.into(),
            })),
            // It may be possible for the update to complete right as the suspend request comes in.
            // In this case, the prepare step is considered complete and the update is not
            // suspended.
            UpdateManagerEvent::UpdateEngineCode(code) => {
                if code != UPDATED_BUT_NOT_ACTIVE {
                    error!("Received an unexpected UpdateManagerEvent::UpdateEngineCode({code})");
                    return Ok(UpdateState::Failed(FailureState {
                        state: Arc::new(PrepareFailure { metadata: self.metadata.clone() }),
                        reason: event.into(),
                    }));
                }

                Ok(UpdateState::InProgress(Arc::new(PrepareComplete {
                    metadata: self.metadata.clone(),
                })))
            }
            UpdateManagerEvent::UpdateEngineSuspended => {
                Ok(UpdateState::InProgress(Arc::new(PrepareSuspendComplete {
                    metadata: self.metadata.clone(),
                })))
            }
            _ => Err(SdvStatus::with_message(
                SdvStatusCode::FailedPrecondition,
                format!("State {:?} does not expect {:?}", self.value(), event),
            )),
        }
    }
    async fn on_enter(&self, env: Arc<UpdateManagerEnvironment>) -> Option<UpdateManagerEvent> {
        if let Err(e) = env.update_engine_client.suspend().await {
            if e.status_code() != SdvStatusCode::FailedPrecondition {
                return Some(UpdateManagerEvent::UpdateEngineError(e.to_string()));
            }

            debug!("There is no UpdateEngine update to resume. This may be because the Update Manager service was restarted while an update was running.");
        }

        Some(UpdateManagerEvent::UpdateEngineSuspended)
    }
}

impl UpdateManagerStateMetadata for PrepareSuspend {
    fn metadata(&self) -> &UpdateMetadata {
        &self.metadata
    }
}

#[derive(Clone)]
pub struct PrepareSuspendComplete {
    pub metadata: UpdateMetadata,
}

#[async_trait]
impl UpdateManagerState for PrepareSuspendComplete {
    fn value(&self) -> ApiUpdateState {
        ApiUpdateState::PREPARE_SUSPEND_COMPLETE
    }
    fn get_next_state(&self, event: UpdateManagerEvent) -> SdvResult<UpdateState> {
        if !check_metadata!(self.metadata, System) {
            return Err(self.create_unexpected_event_for_metadata_sdv_status(&event));
        }

        match event {
            // It may be possible for the update to complete right as the suspend request comes in.
            // In this case, the prepare step is considered complete and the update is not
            // suspended.
            UpdateManagerEvent::UpdateEngineCode(code) => {
                if code != UPDATED_BUT_NOT_ACTIVE {
                    error!("Received an unexpected UpdateManagerEvent::UpdateEngineCode({code})");
                    return Ok(UpdateState::Failed(FailureState {
                        state: Arc::new(PrepareFailure { metadata: self.metadata.clone() }),
                        reason: event.into(),
                    }));
                }

                Ok(UpdateState::InProgress(Arc::new(PrepareComplete {
                    metadata: self.metadata.clone(),
                })))
            }
            UpdateManagerEvent::ResumeRequest => {
                Ok(UpdateState::InProgress(Arc::new(Prepare { metadata: self.metadata.clone() })))
            }
            UpdateManagerEvent::RollbackRequest => {
                Ok(UpdateState::InProgress(Arc::new(PrepareCancel {
                    metadata: self.metadata.clone(),
                })))
            }
            _ => Err(SdvStatus::with_message(
                SdvStatusCode::FailedPrecondition,
                format!("State {:?} does not expect {:?}", self.value(), event),
            )),
        }
    }

    async fn on_enter(&self, env: Arc<UpdateManagerEnvironment>) -> Option<UpdateManagerEvent> {
        if check_metadata!(self.metadata, System) {
            send_power_state_report_processing_complete(env);
        }

        None
    }
}

impl UpdateManagerStateMetadata for PrepareSuspendComplete {
    fn metadata(&self) -> &UpdateMetadata {
        &self.metadata
    }
}

pub struct PrepareComplete {
    pub metadata: UpdateMetadata,
}

#[async_trait]
impl UpdateManagerState for PrepareComplete {
    fn value(&self) -> ApiUpdateState {
        ApiUpdateState::PREPARE_COMPLETE
    }
    fn get_next_state(&self, event: UpdateManagerEvent) -> SdvResult<UpdateState> {
        match event {
            UpdateManagerEvent::ActivateRequest => {
                Ok(UpdateState::InProgress(Arc::new(ActivatePreReboot {
                    metadata: self.metadata.clone(),
                })))
            }
            UpdateManagerEvent::RollbackRequest => {
                Ok(UpdateState::InProgress(Arc::new(PrepareRollback {
                    metadata: self.metadata.clone(),
                })))
            }
            _ => Err(SdvStatus::with_message(
                SdvStatusCode::FailedPrecondition,
                format!("State {:?} does not expect {:?}", self.value(), event),
            )),
        }
    }
    async fn on_enter(&self, env: Arc<UpdateManagerEnvironment>) -> Option<UpdateManagerEvent> {
        // If we transitioned here from PrepareSuspend, ensure we notify VPM that processing the
        // PowerStateReport has completed.
        if check_metadata!(self.metadata, System) {
            send_power_state_report_processing_complete(env);
        }

        None
    }
}

impl UpdateManagerStateMetadata for PrepareComplete {
    fn metadata(&self) -> &UpdateMetadata {
        &self.metadata
    }
}

#[derive(Clone)]
pub struct ActivatePreReboot {
    pub metadata: UpdateMetadata,
}
#[async_trait]
impl UpdateManagerState for ActivatePreReboot {
    fn value(&self) -> ApiUpdateState {
        ApiUpdateState::ACTIVATE_PRE_REBOOT
    }
    fn get_next_state(&self, event: UpdateManagerEvent) -> SdvResult<UpdateState> {
        event_matches_metadata_type(self.metadata(), &event, self.value())?;

        match event {
            UpdateManagerEvent::ApexServiceSuccess => {
                Ok(UpdateState::InProgress(Arc::new(ActivatePreRebootComplete {
                    metadata: self.metadata.clone(),
                })))
            }
            UpdateManagerEvent::ApexServiceError(_) => {
                error!("Received {event:?}");

                Ok(UpdateState::Failed(FailureState {
                    state: Arc::new(ActivatePreRebootFailure { metadata: self.metadata.clone() }),
                    reason: event.into(),
                }))
            }
            UpdateManagerEvent::UpdateEngineError(_) => Ok(UpdateState::Failed(FailureState {
                state: Arc::new(ActivatePreRebootFailure { metadata: self.metadata.clone() }),
                reason: event.into(),
            })),
            UpdateManagerEvent::UpdateEngineCode(code) => {
                if code != SUCCESS {
                    error!("Received an unexpected UpdateManagerEvent::UpdateEngineCode({code})");
                    return Ok(UpdateState::Failed(FailureState {
                        state: Arc::new(ActivatePreRebootFailure {
                            metadata: self.metadata.clone(),
                        }),
                        reason: event.into(),
                    }));
                }

                Ok(UpdateState::InProgress(Arc::new(ActivatePreRebootComplete {
                    metadata: self.metadata.clone(),
                })))
            }
            UpdateManagerEvent::VoldServiceError(_) => {
                if !check_metadata!(self.metadata, ServiceBundle) {
                    return Err(self.create_unexpected_event_for_metadata_sdv_status(&event));
                }

                Ok(UpdateState::Failed(FailureState {
                    state: Arc::new(ActivatePreRebootFailure { metadata: self.metadata().clone() }),
                    reason: event.into(),
                }))
            }
            _ => Err(SdvStatus::with_message(
                SdvStatusCode::FailedPrecondition,
                format!("State {:?} does not expect {:?}", self.value(), event),
            )),
        }
    }
    async fn on_enter(&self, env: Arc<UpdateManagerEnvironment>) -> Option<UpdateManagerEvent> {
        match &self.metadata {
            UpdateMetadata::System(system_metadata) => {
                if is_update_engine_idle(env.update_engine_client.get_last_status()) {
                    if let Err(e) =
                        system_metadata.payload.apply(&env.update_engine_client, true).await
                    {
                        return Some(UpdateManagerEvent::UpdateEngineError(e.to_string()));
                    }
                } else {
                    debug!("Resuming {:?} during an ongoing update. This may be because the Update Manager service was restarted while an update was running.", self.value());
                }
                None
            }
            UpdateMetadata::ServiceBundle(service_bundle_metadata) => {
                let parent_session_id = service_bundle_metadata.parent_session_id;
                let boot_attempts = service_bundle_metadata.boot_attempts;

                if let Err(e) = env.apex_service.mark_staged_session_ready(parent_session_id).await
                {
                    return Some(UpdateManagerEvent::ApexServiceError(e.to_string()));
                }

                if let Err(e) = env.vold_service.start_checkpoint(boot_attempts).await {
                    return Some(UpdateManagerEvent::VoldServiceError(e.to_string()));
                }

                Some(UpdateManagerEvent::ApexServiceSuccess)
            }
        }
    }
}

impl UpdateManagerStateMetadata for ActivatePreReboot {
    fn metadata(&self) -> &UpdateMetadata {
        &self.metadata
    }
}

pub struct ActivatePreRebootComplete {
    pub metadata: UpdateMetadata,
}

impl ActivatePreRebootComplete {
    async fn check_system_update_status_and_recover(
        env: Arc<UpdateManagerEnvironment>,
        metadata: &SystemUpdateMetadata,
    ) -> Option<UpdateManagerEvent> {
        let initial_slot = metadata.initial_boot_slot;

        let current_slot = match env.boot_control_service.get_current_slot().await {
            Ok(current_slot) => current_slot,
            Err(e) => {
                return Some(UpdateManagerEvent::BootControlServiceError(e.to_string()));
            }
        };

        if current_slot == initial_slot {
            // Check the slot before marking the update as failed.

            let opposite_slot = match get_opposite_slot(current_slot) {
                Ok(opposite_slot) => opposite_slot,
                Err(e) => return Some(UpdateManagerEvent::BootControlServiceError(e.to_string())),
            };

            let is_opposite_slot_bootable =
                match env.boot_control_service.is_slot_bootable(opposite_slot).await {
                    Ok(is_opposite_slot_bootable) => is_opposite_slot_bootable,
                    Err(e) => {
                        return Some(UpdateManagerEvent::BootControlServiceError(e.to_string()));
                    }
                };

            if is_opposite_slot_bootable {
                debug!("Update Manager was killed before the reboot took place.");
                None
            } else {
                // Booted from the same slot, update was reverted after restart, indicate the failure.
                Some(UpdateManagerEvent::UpdateEngineError(
                    "Booted from the same slot after restart".to_string(),
                ))
            }
        } else {
            let current_slot_is_marked_successful =
                match env.boot_control_service.is_slot_marked_successful(current_slot).await {
                    Ok(current_slot_is_marked_successful) => current_slot_is_marked_successful,
                    Err(e) => {
                        return Some(UpdateManagerEvent::BootControlServiceError(e.to_string()));
                    }
                };

            if current_slot_is_marked_successful {
                // This is an unexpected condition, treat this as the update completed
                debug!("The current slot was unexpectedly found to be marked as successful even though it does not match the initial slot. Treating this as a successful update.");
                Some(UpdateManagerEvent::Reset)
            } else {
                Some(UpdateManagerEvent::ActivateSuccessful)
            }
        }
    }

    async fn check_service_bundle_update_status_and_recover(
        env: Arc<UpdateManagerEnvironment>,
        metadata: &ServiceBundleUpdateMetadata,
    ) -> Option<UpdateManagerEvent> {
        let session_id = metadata.parent_session_id;

        let apex_session_info = match env.apex_service.get_staged_session_info(session_id).await {
            Ok(apex_session_info) => apex_session_info,
            Err(e) => {
                return Some(UpdateManagerEvent::ApexServiceError(e.to_string()));
            }
        };

        if apex_session_info.isStaged {
            // The Update Manager was killed before the reboot happened
            return None;
        }

        if apex_session_info.isActivated {
            return Some(UpdateManagerEvent::ActivateSuccessful);
        }

        Some(UpdateManagerEvent::ApexServiceError(format!(
            "Session {session_id} in error state: {apex_session_info:?}"
        )))
    }
}
#[async_trait]
impl UpdateManagerState for ActivatePreRebootComplete {
    fn value(&self) -> ApiUpdateState {
        ApiUpdateState::ACTIVATE_PRE_REBOOT_COMPLETE
    }
    fn get_next_state(&self, event: UpdateManagerEvent) -> SdvResult<UpdateState> {
        match event {
            UpdateManagerEvent::ActivateSuccessful => {
                Ok(UpdateState::InProgress(Arc::new(ActivatePostRebootComplete {
                    metadata: self.metadata.clone(),
                })))
            }
            UpdateManagerEvent::Reset => {
                if !check_metadata!(self.metadata, System) {
                    return Err(self.create_unexpected_event_for_metadata_sdv_status(&event));
                }

                Ok(UpdateState::Ready(Arc::new(Ready {})))
            }
            UpdateManagerEvent::BootControlServiceError(_) => {
                if !check_metadata!(self.metadata, System) {
                    return Err(self.create_unexpected_event_for_metadata_sdv_status(&event));
                }

                Ok(UpdateState::Failed(FailureState {
                    state: Arc::new(ActivatePreRebootFailure { metadata: self.metadata().clone() }),
                    reason: event.into(),
                }))
            }
            UpdateManagerEvent::UpdateEngineError(_) => {
                if !check_metadata!(self.metadata, System) {
                    return Err(self.create_unexpected_event_for_metadata_sdv_status(&event));
                }

                Ok(UpdateState::Failed(FailureState {
                    state: Arc::new(ActivatePreRebootFailure { metadata: self.metadata().clone() }),
                    reason: event.into(),
                }))
            }
            UpdateManagerEvent::ApexServiceError(_) => {
                if !check_metadata!(self.metadata, ServiceBundle) {
                    return Err(self.create_unexpected_event_for_metadata_sdv_status(&event));
                }

                Ok(UpdateState::Failed(FailureState {
                    state: Arc::new(ActivatePreRebootFailure { metadata: self.metadata().clone() }),
                    reason: event.into(),
                }))
            }
            UpdateManagerEvent::RollbackRequest => {
                Ok(UpdateState::InProgress(Arc::new(ActivatePreRebootRollback {
                    metadata: self.metadata.clone(),
                })))
            }
            _ => Err(SdvStatus::with_message(
                SdvStatusCode::FailedPrecondition,
                format!("State {:?} does not expect {:?}", self.value(), event),
            )),
        }
    }
    async fn on_recovery(&self, env: Arc<UpdateManagerEnvironment>) -> Option<UpdateManagerEvent> {
        match &self.metadata {
            UpdateMetadata::System(metadata) => {
                Self::check_system_update_status_and_recover(env, metadata).await
            }
            UpdateMetadata::ServiceBundle(metadata) => {
                Self::check_service_bundle_update_status_and_recover(env, metadata).await
            }
        }
    }
}

impl UpdateManagerStateMetadata for ActivatePreRebootComplete {
    fn metadata(&self) -> &UpdateMetadata {
        &self.metadata
    }
}

pub struct ActivatePostRebootComplete {
    pub metadata: UpdateMetadata,
}

impl ActivatePostRebootComplete {
    async fn check_system_update_status_and_recover(
        env: Arc<UpdateManagerEnvironment>,
        metadata: &SystemUpdateMetadata,
    ) -> Option<UpdateManagerEvent> {
        let initial_slot = metadata.initial_boot_slot;

        let current_slot = match env.boot_control_service.get_current_slot().await {
            Ok(current_slot) => current_slot,
            // TODO b/379900612 this should be handled by an error state
            Err(e) => {
                error!("Failed to get current slot, assuming update in progress: {e:?}");
                return None;
            }
        };

        if current_slot == initial_slot {
            // Booted from the same slot, update was reverted after restart, indicate the failure.
            return Some(UpdateManagerEvent::UpdateEngineError(
                "Booted from the same slot after restart".to_string(),
            ));
        }

        None
    }

    async fn check_service_bundle_update_status_and_recover(
        env: Arc<UpdateManagerEnvironment>,
        metadata: &ServiceBundleUpdateMetadata,
    ) -> Option<UpdateManagerEvent> {
        let submitted_apexes_active =
            check_submitted_apexes_active(env.clone(), &metadata.submitted_apex_info)
                .await
                .unwrap_or_else(|e| {
                    // TODO b/379900612 this should be handled by an error state
                    error!("Failed when checking if the submitted APEXes are active: {e:?}");

                    false
                });

        if submitted_apexes_active {
            // The Update Manager was most likely restarted before the Rollback() or Commit() request was received.
            debug!("Update Manager was killed before receiving a Rollback() or Commit() request.");
            return None;
        }

        Some(UpdateManagerEvent::ApexServiceError("Failed to activate the new APEXes".to_string()))
    }
}

#[async_trait]
impl UpdateManagerState for ActivatePostRebootComplete {
    fn value(&self) -> ApiUpdateState {
        ApiUpdateState::ACTIVATE_POST_REBOOT_COMPLETE
    }

    fn get_next_state(&self, event: UpdateManagerEvent) -> SdvResult<UpdateState> {
        match event {
            UpdateManagerEvent::RollbackRequest => {
                Ok(UpdateState::InProgress(Arc::new(ActivatePostRebootRollback {
                    metadata: self.metadata.clone(),
                })))
            }
            UpdateManagerEvent::CommitRequest => {
                Ok(UpdateState::InProgress(Arc::new(Commit { metadata: self.metadata.clone() })))
            }
            UpdateManagerEvent::ApexServiceError(_) => {
                if !check_metadata!(self.metadata, ServiceBundle) {
                    return Err(self.create_unexpected_event_for_metadata_sdv_status(&event));
                }

                Ok(UpdateState::Failed(FailureState {
                    state: Arc::new(ActivatePreRebootFailure { metadata: self.metadata.clone() }),
                    reason: event.into(),
                }))
            }
            UpdateManagerEvent::UpdateEngineError(_) => {
                if !check_metadata!(self.metadata, System) {
                    return Err(self.create_unexpected_event_for_metadata_sdv_status(&event));
                }

                Ok(UpdateState::Failed(FailureState {
                    state: Arc::new(ActivatePreRebootFailure { metadata: self.metadata.clone() }),
                    reason: event.into(),
                }))
            }
            _ => Err(SdvStatus::with_message(
                SdvStatusCode::FailedPrecondition,
                format!("State {:?} does not expect {:?}", self.value(), event),
            )),
        }
    }

    async fn on_recovery(&self, env: Arc<UpdateManagerEnvironment>) -> Option<UpdateManagerEvent> {
        match &self.metadata {
            UpdateMetadata::System(metadata) => {
                Self::check_system_update_status_and_recover(env, metadata).await
            }
            UpdateMetadata::ServiceBundle(metadata) => {
                Self::check_service_bundle_update_status_and_recover(env, metadata).await
            }
        }
    }
}

impl UpdateManagerStateMetadata for ActivatePostRebootComplete {
    fn metadata(&self) -> &UpdateMetadata {
        &self.metadata
    }
}

pub struct Commit {
    pub metadata: UpdateMetadata,
}
#[async_trait]
impl UpdateManagerState for Commit {
    fn value(&self) -> ApiUpdateState {
        ApiUpdateState::COMMIT
    }

    fn get_next_state(&self, event: UpdateManagerEvent) -> SdvResult<UpdateState> {
        match event {
            UpdateManagerEvent::CommitComplete => Ok(UpdateState::Ready(Arc::new(Ready {}))),
            _ => Err(SdvStatus::with_message(
                SdvStatusCode::FailedPrecondition,
                format!("State {:?} does not expect {:?}", self.value(), event),
            )),
        }
    }

    async fn on_enter(&self, env: Arc<UpdateManagerEnvironment>) -> Option<UpdateManagerEvent> {
        if let UpdateMetadata::ServiceBundle(service_bundle_update_metadata) = &self.metadata {
            let result = env
                .apex_service
                .mark_staged_session_successful(service_bundle_update_metadata.parent_session_id)
                .await;

            if let Err(e) = result {
                // TODO b/382312003 determine if there is a better way to log non-critical failures
                // This is not a critical failure. This function is mostly used to ensure the
                // ApexService session is properly marked as complete. The APEXes will still
                // activate on each boot without this succeeding.
                error!(
                    "Failed to mark session {} successful: {:?}",
                    service_bundle_update_metadata.parent_session_id, e
                );
            }

            if let Err(e) =
                delete_staged_apexes(&env.app_staging_dir, service_bundle_update_metadata)
            {
                warn!("Failed to delete the staged APEXes: {e:?}");
            }
        }

        let result = env.vold_service.commit_changes().await;

        if let Err(e) = result {
            // TODO b/382311745 Add a commit failure state
            error!("Failed to commit: {:?}", e.to_string());
        }

        Some(UpdateManagerEvent::CommitComplete)
    }
}

impl UpdateManagerStateMetadata for Commit {
    fn metadata(&self) -> &UpdateMetadata {
        &self.metadata
    }
}

pub struct PrepareFailure {
    pub metadata: UpdateMetadata,
}
#[async_trait]
impl UpdateManagerState for PrepareFailure {
    fn value(&self) -> ApiUpdateState {
        ApiUpdateState::PREPARE_FAILURE
    }

    fn get_next_state(&self, event: UpdateManagerEvent) -> SdvResult<UpdateState> {
        match event {
            UpdateManagerEvent::RollbackRequest => {
                Ok(UpdateState::InProgress(Arc::new(PrepareRollback {
                    metadata: self.metadata.clone(),
                })))
            }
            _ => Err(SdvStatus::with_message(
                SdvStatusCode::FailedPrecondition,
                format!("State {:?} does not expect {:?}", self.value(), event),
            )),
        }
    }

    async fn on_enter(&self, env: Arc<UpdateManagerEnvironment>) -> Option<UpdateManagerEvent> {
        // If we transitioned here from PrepareSuspend, ensure we notify VPM that processing the
        // PowerStateReport has completed.
        if check_metadata!(self.metadata, System) {
            send_power_state_report_processing_complete(env);
        }

        None
    }
}

impl UpdateManagerStateMetadata for PrepareFailure {
    fn metadata(&self) -> &UpdateMetadata {
        &self.metadata
    }
}

pub struct ActivatePreRebootRollback {
    pub metadata: UpdateMetadata,
}
#[async_trait]
impl UpdateManagerState for ActivatePreRebootRollback {
    fn value(&self) -> ApiUpdateState {
        ApiUpdateState::ACTIVATE_PRE_REBOOT_ROLLBACK
    }

    fn get_next_state(&self, event: UpdateManagerEvent) -> SdvResult<UpdateState> {
        match event {
            UpdateManagerEvent::RollbackComplete => Ok(UpdateState::Ready(Arc::new(Ready {}))),
            _ => Err(SdvStatus::with_message(
                SdvStatusCode::FailedPrecondition,
                format!("State {:?} does not expect {:?}", self.value(), event),
            )),
        }
    }

    async fn on_enter(&self, env: Arc<UpdateManagerEnvironment>) -> Option<UpdateManagerEvent> {
        if let UpdateMetadata::ServiceBundle(service_bundle) = &self.metadata {
            if let Err(e) = delete_staged_apexes(&env.app_staging_dir, service_bundle) {
                warn!("Failed to delete the staged APEXes: {e:?}");
            }
        }

        handle_activate_rollback_on_enter(env, &self.metadata).await
    }
}

impl UpdateManagerStateMetadata for ActivatePreRebootRollback {
    fn metadata(&self) -> &UpdateMetadata {
        &self.metadata
    }
}

pub struct ActivatePreRebootFailure {
    pub metadata: UpdateMetadata,
}
#[async_trait]
impl UpdateManagerState for ActivatePreRebootFailure {
    fn value(&self) -> ApiUpdateState {
        ApiUpdateState::ACTIVATE_PRE_REBOOT_FAILURE
    }

    fn get_next_state(&self, event: UpdateManagerEvent) -> SdvResult<UpdateState> {
        match event {
            UpdateManagerEvent::RollbackRequest => {
                Ok(UpdateState::InProgress(Arc::new(ActivatePreRebootRollback {
                    metadata: self.metadata.clone(),
                })))
            }
            _ => Err(SdvStatus::with_message(
                SdvStatusCode::FailedPrecondition,
                format!("State {:?} does not expect {:?}", self.value(), event),
            )),
        }
    }
}

impl UpdateManagerStateMetadata for ActivatePreRebootFailure {
    fn metadata(&self) -> &UpdateMetadata {
        &self.metadata
    }
}

pub struct PrepareCancel {
    pub metadata: UpdateMetadata,
}
#[async_trait]
impl UpdateManagerState for PrepareCancel {
    fn value(&self) -> ApiUpdateState {
        ApiUpdateState::PREPARE_CANCEL
    }

    fn get_next_state(&self, event: UpdateManagerEvent) -> SdvResult<UpdateState> {
        match event {
            UpdateManagerEvent::ApexServiceError(_) => {
                if !check_metadata!(self.metadata, ServiceBundle) {
                    return Err(self.create_unexpected_event_for_metadata_sdv_status(&event));
                }

                Ok(UpdateState::InProgress(Arc::new(PrepareRollback {
                    metadata: self.metadata.clone(),
                })))
            }
            UpdateManagerEvent::UpdateEngineCode(USER_CANCELED)
            | UpdateManagerEvent::UpdateEngineError(_) => {
                if !check_metadata!(self.metadata, System) {
                    return Err(self.create_unexpected_event_for_metadata_sdv_status(&event));
                }

                Ok(UpdateState::InProgress(Arc::new(PrepareRollback {
                    metadata: self.metadata.clone(),
                })))
            }
            _ => Err(SdvStatus::with_message(
                SdvStatusCode::FailedPrecondition,
                format!("State {:?} does not expect {:?}", self.value(), event),
            )),
        }
    }

    async fn on_enter(&self, env: Arc<UpdateManagerEnvironment>) -> Option<UpdateManagerEvent> {
        match &self.metadata {
            UpdateMetadata::System(_) => {
                if let Err(e) = env.update_engine_client.cancel().await {
                    let message: String;
                    if e.status_code() == SdvStatusCode::FailedPrecondition {
                        message = "no update in progress to cancel".to_string();
                        debug!("{message}");
                    } else {
                        // TODO b/381946548 transition to a rollback failure state. For now log the
                        // error and treat the rollback as successful
                        message = format!("Failed to cancel update: {e:?}");
                        error!("{message}");
                    }

                    return Some(UpdateManagerEvent::UpdateEngineError(message));
                }
            }
            UpdateMetadata::ServiceBundle(_) => {
                // It should not be possible to get to this state during a service bundle update.
                // Handle it anyway to prevent the Update Manager from being frozen in this state.
                // TODO b/392087333 Use a more appropriate error type
                return Some(UpdateManagerEvent::ApexServiceError(
                    "PREPARE_CANCEL is not supported for a service bundle update".to_string(),
                ));
            }
        }

        None
    }
}

impl UpdateManagerStateMetadata for PrepareCancel {
    fn metadata(&self) -> &UpdateMetadata {
        &self.metadata
    }
}

pub struct PrepareRollback {
    pub metadata: UpdateMetadata,
}
#[async_trait]
impl UpdateManagerState for PrepareRollback {
    fn value(&self) -> ApiUpdateState {
        ApiUpdateState::PREPARE_ROLLBACK
    }

    fn get_next_state(&self, event: UpdateManagerEvent) -> SdvResult<UpdateState> {
        match event {
            UpdateManagerEvent::RollbackComplete => Ok(UpdateState::Ready(Arc::new(Ready {}))),
            _ => Err(SdvStatus::with_message(
                SdvStatusCode::FailedPrecondition,
                format!("State {:?} does not expect {:?}", self.value(), event),
            )),
        }
    }

    async fn on_enter(&self, env: Arc<UpdateManagerEnvironment>) -> Option<UpdateManagerEvent> {
        match &self.metadata {
            UpdateMetadata::System(_) => {
                if let Err(e) = env.update_engine_client.reset_status().await {
                    // TODO b/381946548 transition to a rollback failure state. For now log the
                    // error and treat the rollback as successful
                    error!("Failed to reset update engine status: {e:?}");
                }
            }
            UpdateMetadata::ServiceBundle(service_bundle) => {
                if let Err(e) =
                    env.apex_service.abort_staged_session(service_bundle.parent_session_id).await
                {
                    // TODO b/381946548 transition to a rollback failure state. For now log the
                    // error and treat the rollback as successful
                    error!(
                        "Failed to abort staged session {}: {:?}",
                        service_bundle.parent_session_id, e,
                    );
                }

                if let Err(e) = delete_staged_apexes(&env.app_staging_dir, service_bundle) {
                    warn!("Failed to delete the staged APEXes: {e:?}");
                }
            }
        }

        Some(UpdateManagerEvent::RollbackComplete)
    }
}

impl UpdateManagerStateMetadata for PrepareRollback {
    fn metadata(&self) -> &UpdateMetadata {
        &self.metadata
    }
}

pub struct ActivatePostRebootRollback {
    pub metadata: UpdateMetadata,
}
#[async_trait]
impl UpdateManagerState for ActivatePostRebootRollback {
    fn value(&self) -> ApiUpdateState {
        ApiUpdateState::ACTIVATE_POST_REBOOT_ROLLBACK
    }

    fn get_next_state(&self, event: UpdateManagerEvent) -> SdvResult<UpdateState> {
        match event {
            UpdateManagerEvent::RollbackComplete => {
                Ok(UpdateState::InProgress(Arc::new(ActivatePostRebootRollbackComplete {
                    metadata: self.metadata.clone(),
                })))
            }
            _ => Err(SdvStatus::with_message(
                SdvStatusCode::FailedPrecondition,
                format!("State {:?} does not expect {:?}", self.value(), event),
            )),
        }
    }
    async fn on_enter(&self, env: Arc<UpdateManagerEnvironment>) -> Option<UpdateManagerEvent> {
        handle_activate_rollback_on_enter(env, &self.metadata).await
    }
}

impl UpdateManagerStateMetadata for ActivatePostRebootRollback {
    fn metadata(&self) -> &UpdateMetadata {
        &self.metadata
    }
}

pub struct ActivatePostRebootRollbackComplete {
    pub metadata: UpdateMetadata,
}

impl ActivatePostRebootRollbackComplete {
    async fn check_service_bundle_update_status_and_recover(
        env: Arc<UpdateManagerEnvironment>,
        metadata: &ServiceBundleUpdateMetadata,
    ) -> Option<UpdateManagerEvent> {
        let submitted_apexes_active =
            check_submitted_apexes_active(env.clone(), &metadata.submitted_apex_info)
                .await
                .unwrap_or_else(|e| {
                    // TODO b/381946548 transition to a rollback failure state. For now log the
                    // error and treat the rollback as successful
                    error!("Failed when checking if the submitted APEXes are active: {e:?}");

                    false
                });

        if submitted_apexes_active {
            // The Update Manager was most likely restarted before the system was rebooted
            debug!("Update Manager was killed before the system was rebooted.");
            return None;
        }

        debug!("The submitted APEXes are not active, rollback complete.");

        if let Err(e) = delete_staged_apexes(&env.app_staging_dir, metadata) {
            warn!("Failed to delete the staged APEXes: {e:?}");
        }

        Some(UpdateManagerEvent::RollbackComplete)
    }

    async fn check_system_update_status_and_recover(
        env: Arc<UpdateManagerEnvironment>,
        metadata: &SystemUpdateMetadata,
    ) -> Option<UpdateManagerEvent> {
        let initial_slot = metadata.initial_boot_slot;

        let current_slot = match env.boot_control_service.get_current_slot().await {
            Ok(current_slot) => current_slot,
            // TODO b/379900612 this should be handled by an error state
            Err(e) => {
                error!("Failed when getting the current slot: {e:?}");
                return None;
            }
        };

        if current_slot == initial_slot {
            // The rollback has completed, reset back to READY
            Some(UpdateManagerEvent::RollbackComplete)
        } else {
            // Update Manager was restarted before the VM was restarted, resume the state
            None
        }
    }
}

#[async_trait]
impl UpdateManagerState for ActivatePostRebootRollbackComplete {
    fn value(&self) -> ApiUpdateState {
        ApiUpdateState::ACTIVATE_POST_REBOOT_ROLLBACK_COMPLETE
    }

    fn get_next_state(&self, event: UpdateManagerEvent) -> SdvResult<UpdateState> {
        match event {
            UpdateManagerEvent::RollbackComplete => Ok(UpdateState::Ready(Arc::new(Ready {}))),
            _ => Err(SdvStatus::with_message(
                SdvStatusCode::FailedPrecondition,
                format!("State {:?} does not expect {:?}", self.value(), event),
            )),
        }
    }

    async fn on_enter(&self, env: Arc<UpdateManagerEnvironment>) -> Option<UpdateManagerEvent> {
        let result = env.apex_service.mark_boot_successful().await;

        if let Err(e) = result {
            // TODO b/382312003 determine if there is a better way to log non-critical failures
            // This is not a critical failure. This function is mostly used to ensure the
            // ApexService session directories are cleaned up after an update. The APEXes will still
            // activate on each boot without this succeeding.
            warn!(
                "Failed to indicate to ApexService that the boot was successful, updated APEXes will still be activated: {e:?}",
            );
        }

        None
    }

    async fn on_recovery(&self, env: Arc<UpdateManagerEnvironment>) -> Option<UpdateManagerEvent> {
        match &self.metadata {
            UpdateMetadata::System(metadata) => {
                Self::check_system_update_status_and_recover(env, metadata).await
            }
            UpdateMetadata::ServiceBundle(metadata) => {
                Self::check_service_bundle_update_status_and_recover(env, metadata).await
            }
        }
    }
}

impl UpdateManagerStateMetadata for ActivatePostRebootRollbackComplete {
    fn metadata(&self) -> &UpdateMetadata {
        &self.metadata
    }
}

#[cfg(test)]
mod tests {
    use crate::metadata::{
        ApexInfo, ServiceBundleUpdateMetadata, SystemUpdateMetadata, UpdateMetadata,
    };
    use crate::payload::{ServiceBundleUpdatePayload, SystemUpdatePayload};
    use crate::update_manager_state::FailureReason;
    use crate::update_manager_state::FailureState;
    use crate::update_manager_state::UpdateManagerEvent;
    use crate::update_manager_state::UpdateManagerState;
    use crate::update_manager_state::UpdateState;
    use crate::update_manager_state::{
        ActivatePostRebootComplete, ActivatePostRebootRollback, ActivatePostRebootRollbackComplete,
        ActivatePreReboot, ActivatePreRebootComplete, ActivatePreRebootFailure,
        ActivatePreRebootRollback, Commit, Prepare, PrepareCancel, PrepareComplete, PrepareFailure,
        PrepareRollback, PrepareSuspend, PrepareSuspendComplete, Ready, UpdateManagerStateMetadata,
    };
    use crate::update_manager_state::{SUCCESS, UPDATED_BUT_NOT_ACTIVE, USER_CANCELED};
    use std::collections::HashMap;
    use std::sync::Arc;
    use tempfile::tempdir;
    use update_manager_api::error_code::ErrorCode;
    use update_manager_api::update_state::UpdateState as ApiUpdateState;

    const UPDATE_ENGINE_CODE: i32 = 123;
    const ERROR_STR: &str = "Error!";

    #[derive(Clone, Debug, PartialEq)]
    enum UpdateMetadataType {
        ServiceBundle,
        System,
    }

    #[derive(Clone, Debug, PartialEq)]
    struct StateAndEvent {
        state: ApiUpdateState,
        update_metadata_type: Option<UpdateMetadataType>,
        event: UpdateManagerEvent,
    }

    #[derive(Clone, PartialEq)]
    struct StateTransition {
        source: StateAndEvent,
        target: ApiUpdateState,
    }

    struct StateTransitionError {
        state_transition: StateTransition,
        error: String,
    }

    struct StateAndEventError {
        state_and_event: StateAndEvent,
        error: String,
    }

    fn remove_element<T: PartialEq>(vec: &mut Vec<T>, element: &T) -> bool {
        if let Some(pos) = vec.iter().position(|x| *x == *element) {
            vec.remove(pos);
            true
        } else {
            false
        }
    }

    fn get_system_payload() -> SystemUpdatePayload {
        SystemUpdatePayload::new(
            "/fake/payload.zip",
            /* payload_offset= */ 0,
            /* payload_size= */ 0,
            &HashMap::<String, String>::new(),
        )
        .unwrap()
    }

    fn get_system_metadata() -> UpdateMetadata {
        UpdateMetadata::System(SystemUpdateMetadata::new(
            get_system_payload(),
            /*initial_boot_slot*/ 1,
        ))
    }

    fn get_service_bundle_payload() -> ServiceBundleUpdatePayload {
        ServiceBundleUpdatePayload::new(
            vec!["/my/a.apex".to_string(), "/my/b.apex".to_string()],
            /* boot_attempts= */ 2,
        )
        .unwrap()
    }

    fn get_service_bundle_metadata() -> UpdateMetadata {
        UpdateMetadata::ServiceBundle(
            ServiceBundleUpdateMetadata::from_payload_and_parent_session_id(
                get_service_bundle_payload(),
                /* parent_session_id= */ 2,
            )
            .unwrap(),
        )
    }

    fn get_service_bundle_metadata_with_apex_info() -> UpdateMetadata {
        let mut metadata = ServiceBundleUpdateMetadata::from_payload_and_parent_session_id(
            get_service_bundle_payload(),
            /* parent_session_id= */ 2,
        )
        .unwrap();

        metadata.submitted_apex_info = get_apex_info();

        UpdateMetadata::ServiceBundle(metadata)
    }

    fn get_metadata(
        api_update_state: ApiUpdateState,
        update_metadata_type: &UpdateMetadataType,
    ) -> UpdateMetadata {
        match update_metadata_type {
            UpdateMetadataType::ServiceBundle => match api_update_state {
                ApiUpdateState::READY | ApiUpdateState::PREPARE => get_service_bundle_metadata(),
                _ => get_service_bundle_metadata_with_apex_info(),
            },
            UpdateMetadataType::System => get_system_metadata(),
        }
    }

    fn get_apex_info() -> Vec<ApexInfo> {
        vec![ApexInfo::new("a", 1), ApexInfo::new("b", 2)]
    }

    fn get_update_state(
        api_update_state: ApiUpdateState,
        update_metadata_type: Option<&UpdateMetadataType>,
    ) -> UpdateState {
        // TODO switch the failure reason to an apex specific version for service bundle
        // updates
        match api_update_state {
            ApiUpdateState::READY => UpdateState::Ready(Arc::new(Ready {})),
            ApiUpdateState::PREPARE => UpdateState::InProgress(Arc::new(Prepare {
                metadata: get_metadata(api_update_state, update_metadata_type.unwrap()),
            })),
            ApiUpdateState::PREPARE_SUSPEND => UpdateState::InProgress(Arc::new(PrepareSuspend {
                metadata: get_metadata(api_update_state, update_metadata_type.unwrap()),
            })),
            ApiUpdateState::PREPARE_SUSPEND_COMPLETE => {
                UpdateState::InProgress(Arc::new(PrepareSuspendComplete {
                    metadata: get_metadata(api_update_state, update_metadata_type.unwrap()),
                }))
            }
            ApiUpdateState::PREPARE_FAILURE => UpdateState::Failed(FailureState {
                state: Arc::new(PrepareFailure {
                    metadata: get_metadata(api_update_state, update_metadata_type.unwrap()),
                }),
                reason: FailureReason::new(
                    ErrorCode::UpdateEngineError,
                    UPDATE_ENGINE_CODE.to_string(),
                ),
            }),
            ApiUpdateState::PREPARE_COMPLETE => {
                UpdateState::InProgress(Arc::new(PrepareComplete {
                    metadata: get_metadata(api_update_state, update_metadata_type.unwrap()),
                }))
            }
            ApiUpdateState::PREPARE_CANCEL => UpdateState::InProgress(Arc::new(PrepareCancel {
                metadata: get_metadata(api_update_state, update_metadata_type.unwrap()),
            })),
            ApiUpdateState::PREPARE_ROLLBACK => {
                UpdateState::InProgress(Arc::new(PrepareRollback {
                    metadata: get_metadata(api_update_state, update_metadata_type.unwrap()),
                }))
            }
            ApiUpdateState::ACTIVATE_PRE_REBOOT => {
                UpdateState::InProgress(Arc::new(ActivatePreReboot {
                    metadata: get_metadata(api_update_state, update_metadata_type.unwrap()),
                }))
            }
            ApiUpdateState::ACTIVATE_PRE_REBOOT_FAILURE => UpdateState::Failed(FailureState {
                state: Arc::new(ActivatePreRebootFailure {
                    metadata: get_metadata(api_update_state, update_metadata_type.unwrap()),
                }),
                reason: FailureReason::new(
                    ErrorCode::UpdateEngineError,
                    UPDATE_ENGINE_CODE.to_string(),
                ),
            }),
            ApiUpdateState::ACTIVATE_PRE_REBOOT_COMPLETE => {
                UpdateState::InProgress(Arc::new(ActivatePreRebootComplete {
                    metadata: get_metadata(api_update_state, update_metadata_type.unwrap()),
                }))
            }
            ApiUpdateState::ACTIVATE_PRE_REBOOT_ROLLBACK => {
                UpdateState::InProgress(Arc::new(ActivatePreRebootRollback {
                    metadata: get_metadata(api_update_state, update_metadata_type.unwrap()),
                }))
            }
            ApiUpdateState::ACTIVATE_POST_REBOOT_COMPLETE => {
                UpdateState::InProgress(Arc::new(ActivatePostRebootComplete {
                    metadata: get_metadata(api_update_state, update_metadata_type.unwrap()),
                }))
            }
            ApiUpdateState::ACTIVATE_POST_REBOOT_ROLLBACK => {
                UpdateState::InProgress(Arc::new(ActivatePostRebootRollback {
                    metadata: get_metadata(api_update_state, update_metadata_type.unwrap()),
                }))
            }
            ApiUpdateState::ACTIVATE_POST_REBOOT_ROLLBACK_COMPLETE => {
                UpdateState::InProgress(Arc::new(ActivatePostRebootRollbackComplete {
                    metadata: get_metadata(api_update_state, update_metadata_type.unwrap()),
                }))
            }
            ApiUpdateState::COMMIT => UpdateState::InProgress(Arc::new(Commit {
                metadata: get_metadata(api_update_state, update_metadata_type.unwrap()),
            })),
        }
    }

    fn get_all_api_update_states() -> Vec<ApiUpdateState> {
        vec![
            ApiUpdateState::READY,
            ApiUpdateState::PREPARE,
            ApiUpdateState::PREPARE_SUSPEND,
            ApiUpdateState::PREPARE_SUSPEND_COMPLETE,
            ApiUpdateState::PREPARE_FAILURE,
            ApiUpdateState::PREPARE_COMPLETE,
            ApiUpdateState::PREPARE_CANCEL,
            ApiUpdateState::PREPARE_ROLLBACK,
            ApiUpdateState::ACTIVATE_PRE_REBOOT,
            ApiUpdateState::ACTIVATE_PRE_REBOOT_FAILURE,
            ApiUpdateState::ACTIVATE_PRE_REBOOT_COMPLETE,
            ApiUpdateState::ACTIVATE_PRE_REBOOT_ROLLBACK,
            ApiUpdateState::ACTIVATE_POST_REBOOT_COMPLETE,
            ApiUpdateState::ACTIVATE_POST_REBOOT_ROLLBACK,
            ApiUpdateState::ACTIVATE_POST_REBOOT_ROLLBACK_COMPLETE,
            ApiUpdateState::COMMIT,
        ]
    }

    fn get_all_states(metadata_type: UpdateMetadataType) -> Vec<UpdateState> {
        let all_api_update_states = get_all_api_update_states();

        all_api_update_states
            .into_iter()
            .map(|api_update_state| get_update_state(api_update_state, Some(&metadata_type)))
            .collect()
    }

    fn get_all_events() -> Vec<UpdateManagerEvent> {
        vec![
            UpdateManagerEvent::Reset,
            UpdateManagerEvent::PrepareRequest(get_system_metadata()),
            UpdateManagerEvent::PrepareRequest(get_service_bundle_metadata()),
            UpdateManagerEvent::ActivateRequest,
            UpdateManagerEvent::ActivateSuccessful,
            UpdateManagerEvent::RollbackRequest,
            UpdateManagerEvent::CommitRequest,
            UpdateManagerEvent::UpdateEngineError(ERROR_STR.to_string()),
            UpdateManagerEvent::UpdateEngineCode(SUCCESS),
            UpdateManagerEvent::UpdateEngineCode(UPDATED_BUT_NOT_ACTIVE),
            UpdateManagerEvent::UpdateEngineCode(USER_CANCELED),
            UpdateManagerEvent::UpdateEngineSuspended,
            UpdateManagerEvent::ApexServiceSuccess,
            UpdateManagerEvent::ApexServiceSubmitSuccess(get_apex_info()),
            UpdateManagerEvent::ApexServiceError(ERROR_STR.to_string()),
            UpdateManagerEvent::BootControlServiceError(ERROR_STR.to_string()),
            UpdateManagerEvent::VoldServiceError(ERROR_STR.to_string()),
            UpdateManagerEvent::RollbackComplete,
            UpdateManagerEvent::CommitComplete,
            UpdateManagerEvent::SuspendRequest,
            UpdateManagerEvent::ResumeRequest,
            UpdateManagerEvent::PowerOffPostFinish,
            UpdateManagerEvent::SuspendToRamPostFinish,
            UpdateManagerEvent::SuspendToDiskPostFinish,
        ]
    }

    fn get_all_state_and_event_combinations() -> Vec<StateAndEvent> {
        let mut all_state_and_event_combinations = Vec::<StateAndEvent>::new();
        for state in get_all_api_update_states() {
            let update_metadata_types = match state {
                ApiUpdateState::READY => vec![None],
                _ => {
                    vec![Some(UpdateMetadataType::ServiceBundle), Some(UpdateMetadataType::System)]
                }
            };

            for update_metadata_type in update_metadata_types {
                for event in get_all_events() {
                    let state_and_event = StateAndEvent {
                        state,
                        update_metadata_type: update_metadata_type.clone(),
                        event,
                    };
                    all_state_and_event_combinations.push(state_and_event.clone());
                }
            }
        }

        all_state_and_event_combinations
    }

    fn get_all_valid_state_transitions() -> Vec<StateTransition> {
        vec![
            StateTransition {
                source: StateAndEvent {
                    state: ApiUpdateState::READY,
                    update_metadata_type: None,
                    event: UpdateManagerEvent::PrepareRequest(get_service_bundle_metadata()),
                },
                target: ApiUpdateState::PREPARE,
            },
            StateTransition {
                source: StateAndEvent {
                    state: ApiUpdateState::READY,
                    update_metadata_type: None,
                    event: UpdateManagerEvent::PrepareRequest(get_system_metadata()),
                },
                target: ApiUpdateState::PREPARE,
            },
            StateTransition {
                source: StateAndEvent {
                    state: ApiUpdateState::PREPARE,
                    update_metadata_type: Some(UpdateMetadataType::ServiceBundle),
                    event: UpdateManagerEvent::ApexServiceSubmitSuccess(get_apex_info()),
                },
                target: ApiUpdateState::PREPARE_COMPLETE,
            },
            StateTransition {
                source: StateAndEvent {
                    state: ApiUpdateState::PREPARE,
                    update_metadata_type: Some(UpdateMetadataType::System),
                    event: UpdateManagerEvent::UpdateEngineCode(UPDATED_BUT_NOT_ACTIVE),
                },
                target: ApiUpdateState::PREPARE_COMPLETE,
            },
            StateTransition {
                source: StateAndEvent {
                    state: ApiUpdateState::PREPARE,
                    update_metadata_type: Some(UpdateMetadataType::System),
                    event: UpdateManagerEvent::RollbackRequest,
                },
                target: ApiUpdateState::PREPARE_CANCEL,
            },
            StateTransition {
                source: StateAndEvent {
                    state: ApiUpdateState::PREPARE,
                    update_metadata_type: Some(UpdateMetadataType::System),
                    event: UpdateManagerEvent::SuspendRequest,
                },
                target: ApiUpdateState::PREPARE_SUSPEND,
            },
            StateTransition {
                source: StateAndEvent {
                    state: ApiUpdateState::PREPARE,
                    update_metadata_type: Some(UpdateMetadataType::System),
                    event: UpdateManagerEvent::PowerOffPostFinish,
                },
                target: ApiUpdateState::PREPARE_SUSPEND,
            },
            StateTransition {
                source: StateAndEvent {
                    state: ApiUpdateState::PREPARE,
                    update_metadata_type: Some(UpdateMetadataType::System),
                    event: UpdateManagerEvent::SuspendToRamPostFinish,
                },
                target: ApiUpdateState::PREPARE_SUSPEND,
            },
            StateTransition {
                source: StateAndEvent {
                    state: ApiUpdateState::PREPARE,
                    update_metadata_type: Some(UpdateMetadataType::System),
                    event: UpdateManagerEvent::SuspendToDiskPostFinish,
                },
                target: ApiUpdateState::PREPARE_SUSPEND,
            },
            StateTransition {
                source: StateAndEvent {
                    state: ApiUpdateState::PREPARE,
                    update_metadata_type: Some(UpdateMetadataType::ServiceBundle),
                    event: UpdateManagerEvent::ApexServiceError(ERROR_STR.to_string()),
                },
                target: ApiUpdateState::PREPARE_FAILURE,
            },
            StateTransition {
                source: StateAndEvent {
                    state: ApiUpdateState::PREPARE,
                    update_metadata_type: Some(UpdateMetadataType::System),
                    event: UpdateManagerEvent::UpdateEngineError(ERROR_STR.to_string()),
                },
                target: ApiUpdateState::PREPARE_FAILURE,
            },
            StateTransition {
                source: StateAndEvent {
                    state: ApiUpdateState::PREPARE,
                    update_metadata_type: Some(UpdateMetadataType::System),
                    event: UpdateManagerEvent::UpdateEngineCode(SUCCESS),
                },
                target: ApiUpdateState::PREPARE_FAILURE,
            },
            StateTransition {
                source: StateAndEvent {
                    state: ApiUpdateState::PREPARE,
                    update_metadata_type: Some(UpdateMetadataType::System),
                    // NOTE: this is a duplicate of above. However this is necessary
                    // since the test will try all combinations of the given events.
                    event: UpdateManagerEvent::UpdateEngineCode(USER_CANCELED),
                },
                target: ApiUpdateState::PREPARE_FAILURE,
            },
            StateTransition {
                source: StateAndEvent {
                    state: ApiUpdateState::PREPARE_SUSPEND,
                    update_metadata_type: Some(UpdateMetadataType::System),
                    event: UpdateManagerEvent::UpdateEngineSuspended,
                },
                target: ApiUpdateState::PREPARE_SUSPEND_COMPLETE,
            },
            StateTransition {
                source: StateAndEvent {
                    state: ApiUpdateState::PREPARE_SUSPEND,
                    update_metadata_type: Some(UpdateMetadataType::System),
                    event: UpdateManagerEvent::UpdateEngineCode(UPDATED_BUT_NOT_ACTIVE),
                },
                target: ApiUpdateState::PREPARE_COMPLETE,
            },
            StateTransition {
                source: StateAndEvent {
                    state: ApiUpdateState::PREPARE_SUSPEND,
                    update_metadata_type: Some(UpdateMetadataType::System),
                    event: UpdateManagerEvent::UpdateEngineError(ERROR_STR.to_string()),
                },
                target: ApiUpdateState::PREPARE_FAILURE,
            },
            StateTransition {
                source: StateAndEvent {
                    state: ApiUpdateState::PREPARE_SUSPEND,
                    update_metadata_type: Some(UpdateMetadataType::System),
                    event: UpdateManagerEvent::UpdateEngineCode(SUCCESS),
                },
                target: ApiUpdateState::PREPARE_FAILURE,
            },
            StateTransition {
                source: StateAndEvent {
                    state: ApiUpdateState::PREPARE_SUSPEND,
                    update_metadata_type: Some(UpdateMetadataType::System),
                    // NOTE: this is a duplicate of above. However this is necessary
                    // since the test will try all combinations of the given events.
                    event: UpdateManagerEvent::UpdateEngineCode(USER_CANCELED),
                },
                target: ApiUpdateState::PREPARE_FAILURE,
            },
            StateTransition {
                source: StateAndEvent {
                    state: ApiUpdateState::PREPARE_SUSPEND_COMPLETE,
                    update_metadata_type: Some(UpdateMetadataType::System),
                    event: UpdateManagerEvent::UpdateEngineCode(UPDATED_BUT_NOT_ACTIVE),
                },
                target: ApiUpdateState::PREPARE_COMPLETE,
            },
            StateTransition {
                source: StateAndEvent {
                    state: ApiUpdateState::PREPARE_SUSPEND_COMPLETE,
                    update_metadata_type: Some(UpdateMetadataType::System),
                    event: UpdateManagerEvent::ResumeRequest,
                },
                target: ApiUpdateState::PREPARE,
            },
            StateTransition {
                source: StateAndEvent {
                    state: ApiUpdateState::PREPARE_SUSPEND_COMPLETE,
                    update_metadata_type: Some(UpdateMetadataType::System),
                    event: UpdateManagerEvent::RollbackRequest,
                },
                target: ApiUpdateState::PREPARE_CANCEL,
            },
            StateTransition {
                source: StateAndEvent {
                    state: ApiUpdateState::PREPARE_SUSPEND_COMPLETE,
                    update_metadata_type: Some(UpdateMetadataType::System),
                    event: UpdateManagerEvent::UpdateEngineCode(SUCCESS),
                },
                target: ApiUpdateState::PREPARE_FAILURE,
            },
            StateTransition {
                source: StateAndEvent {
                    state: ApiUpdateState::PREPARE_SUSPEND_COMPLETE,
                    update_metadata_type: Some(UpdateMetadataType::System),
                    // NOTE: this is a duplicate of above. However this is necessary
                    // since the test will try all combinations of the given events.
                    event: UpdateManagerEvent::UpdateEngineCode(USER_CANCELED),
                },
                target: ApiUpdateState::PREPARE_FAILURE,
            },
            StateTransition {
                source: StateAndEvent {
                    state: ApiUpdateState::PREPARE_FAILURE,
                    update_metadata_type: Some(UpdateMetadataType::System),
                    event: UpdateManagerEvent::RollbackRequest,
                },
                target: ApiUpdateState::PREPARE_ROLLBACK,
            },
            StateTransition {
                source: StateAndEvent {
                    state: ApiUpdateState::PREPARE_FAILURE,
                    update_metadata_type: Some(UpdateMetadataType::ServiceBundle),
                    event: UpdateManagerEvent::RollbackRequest,
                },
                target: ApiUpdateState::PREPARE_ROLLBACK,
            },
            StateTransition {
                source: StateAndEvent {
                    state: ApiUpdateState::PREPARE_COMPLETE,
                    update_metadata_type: Some(UpdateMetadataType::ServiceBundle),
                    event: UpdateManagerEvent::ActivateRequest,
                },
                target: ApiUpdateState::ACTIVATE_PRE_REBOOT,
            },
            StateTransition {
                source: StateAndEvent {
                    state: ApiUpdateState::PREPARE_COMPLETE,
                    update_metadata_type: Some(UpdateMetadataType::System),
                    event: UpdateManagerEvent::RollbackRequest,
                },
                target: ApiUpdateState::PREPARE_ROLLBACK,
            },
            StateTransition {
                source: StateAndEvent {
                    state: ApiUpdateState::PREPARE_COMPLETE,
                    update_metadata_type: Some(UpdateMetadataType::ServiceBundle),
                    event: UpdateManagerEvent::RollbackRequest,
                },
                target: ApiUpdateState::PREPARE_ROLLBACK,
            },
            StateTransition {
                source: StateAndEvent {
                    state: ApiUpdateState::PREPARE_COMPLETE,
                    update_metadata_type: Some(UpdateMetadataType::System),
                    event: UpdateManagerEvent::ActivateRequest,
                },
                target: ApiUpdateState::ACTIVATE_PRE_REBOOT,
            },
            StateTransition {
                source: StateAndEvent {
                    state: ApiUpdateState::PREPARE_CANCEL,
                    update_metadata_type: Some(UpdateMetadataType::System),
                    event: UpdateManagerEvent::UpdateEngineCode(USER_CANCELED),
                },
                target: ApiUpdateState::PREPARE_ROLLBACK,
            },
            StateTransition {
                source: StateAndEvent {
                    state: ApiUpdateState::PREPARE_CANCEL,
                    update_metadata_type: Some(UpdateMetadataType::System),
                    event: UpdateManagerEvent::UpdateEngineError(ERROR_STR.to_string()),
                },
                target: ApiUpdateState::PREPARE_ROLLBACK,
            },
            StateTransition {
                source: StateAndEvent {
                    state: ApiUpdateState::PREPARE_CANCEL,
                    update_metadata_type: Some(UpdateMetadataType::ServiceBundle),
                    event: UpdateManagerEvent::ApexServiceError(ERROR_STR.to_string()),
                },
                target: ApiUpdateState::PREPARE_ROLLBACK,
            },
            StateTransition {
                source: StateAndEvent {
                    state: ApiUpdateState::PREPARE_ROLLBACK,
                    update_metadata_type: Some(UpdateMetadataType::System),
                    event: UpdateManagerEvent::RollbackComplete,
                },
                target: ApiUpdateState::READY,
            },
            StateTransition {
                source: StateAndEvent {
                    state: ApiUpdateState::PREPARE_ROLLBACK,
                    update_metadata_type: Some(UpdateMetadataType::ServiceBundle),
                    event: UpdateManagerEvent::RollbackComplete,
                },
                target: ApiUpdateState::READY,
            },
            StateTransition {
                source: StateAndEvent {
                    state: ApiUpdateState::ACTIVATE_PRE_REBOOT,
                    update_metadata_type: Some(UpdateMetadataType::ServiceBundle),
                    event: UpdateManagerEvent::ApexServiceSuccess,
                },
                target: ApiUpdateState::ACTIVATE_PRE_REBOOT_COMPLETE,
            },
            StateTransition {
                source: StateAndEvent {
                    state: ApiUpdateState::ACTIVATE_PRE_REBOOT,
                    update_metadata_type: Some(UpdateMetadataType::ServiceBundle),
                    event: UpdateManagerEvent::ApexServiceError(ERROR_STR.to_string()),
                },
                target: ApiUpdateState::ACTIVATE_PRE_REBOOT_FAILURE,
            },
            StateTransition {
                source: StateAndEvent {
                    state: ApiUpdateState::ACTIVATE_PRE_REBOOT,
                    update_metadata_type: Some(UpdateMetadataType::ServiceBundle),
                    event: UpdateManagerEvent::VoldServiceError(ERROR_STR.to_string()),
                },
                target: ApiUpdateState::ACTIVATE_PRE_REBOOT_FAILURE,
            },
            StateTransition {
                source: StateAndEvent {
                    state: ApiUpdateState::ACTIVATE_PRE_REBOOT,
                    update_metadata_type: Some(UpdateMetadataType::System),
                    event: UpdateManagerEvent::UpdateEngineCode(SUCCESS),
                },
                target: ApiUpdateState::ACTIVATE_PRE_REBOOT_COMPLETE,
            },
            StateTransition {
                source: StateAndEvent {
                    state: ApiUpdateState::ACTIVATE_PRE_REBOOT,
                    update_metadata_type: Some(UpdateMetadataType::System),
                    // NOTE: any code other than SUCCESS will trigger a failure. Use
                    // UPDATED_BUT_NOT_ACTIVE as an example of an unexpected code.
                    event: UpdateManagerEvent::UpdateEngineCode(UPDATED_BUT_NOT_ACTIVE),
                },
                target: ApiUpdateState::ACTIVATE_PRE_REBOOT_FAILURE,
            },
            StateTransition {
                source: StateAndEvent {
                    state: ApiUpdateState::ACTIVATE_PRE_REBOOT,
                    update_metadata_type: Some(UpdateMetadataType::System),
                    // NOTE: this is a duplicate of above. However this is necessary
                    // since the test will try all combinations of the given events.
                    event: UpdateManagerEvent::UpdateEngineCode(USER_CANCELED),
                },
                target: ApiUpdateState::ACTIVATE_PRE_REBOOT_FAILURE,
            },
            StateTransition {
                source: StateAndEvent {
                    state: ApiUpdateState::ACTIVATE_PRE_REBOOT,
                    update_metadata_type: Some(UpdateMetadataType::System),
                    event: UpdateManagerEvent::UpdateEngineError(ERROR_STR.to_string()),
                },
                target: ApiUpdateState::ACTIVATE_PRE_REBOOT_FAILURE,
            },
            StateTransition {
                source: StateAndEvent {
                    state: ApiUpdateState::ACTIVATE_PRE_REBOOT_COMPLETE,
                    update_metadata_type: Some(UpdateMetadataType::System),
                    event: UpdateManagerEvent::ActivateSuccessful,
                },
                target: ApiUpdateState::ACTIVATE_POST_REBOOT_COMPLETE,
            },
            StateTransition {
                source: StateAndEvent {
                    state: ApiUpdateState::ACTIVATE_PRE_REBOOT_COMPLETE,
                    update_metadata_type: Some(UpdateMetadataType::ServiceBundle),
                    event: UpdateManagerEvent::ActivateSuccessful,
                },
                target: ApiUpdateState::ACTIVATE_POST_REBOOT_COMPLETE,
            },
            StateTransition {
                source: StateAndEvent {
                    state: ApiUpdateState::ACTIVATE_PRE_REBOOT_COMPLETE,
                    update_metadata_type: Some(UpdateMetadataType::System),
                    event: UpdateManagerEvent::Reset,
                },
                target: ApiUpdateState::READY,
            },
            StateTransition {
                source: StateAndEvent {
                    state: ApiUpdateState::ACTIVATE_PRE_REBOOT_COMPLETE,
                    update_metadata_type: Some(UpdateMetadataType::System),
                    event: UpdateManagerEvent::RollbackRequest,
                },
                target: ApiUpdateState::ACTIVATE_PRE_REBOOT_ROLLBACK,
            },
            StateTransition {
                source: StateAndEvent {
                    state: ApiUpdateState::ACTIVATE_PRE_REBOOT_COMPLETE,
                    update_metadata_type: Some(UpdateMetadataType::ServiceBundle),
                    event: UpdateManagerEvent::RollbackRequest,
                },
                target: ApiUpdateState::ACTIVATE_PRE_REBOOT_ROLLBACK,
            },
            StateTransition {
                source: StateAndEvent {
                    state: ApiUpdateState::ACTIVATE_PRE_REBOOT_COMPLETE,
                    update_metadata_type: Some(UpdateMetadataType::System),
                    event: UpdateManagerEvent::UpdateEngineError(ERROR_STR.to_string()),
                },
                target: ApiUpdateState::ACTIVATE_PRE_REBOOT_FAILURE,
            },
            StateTransition {
                source: StateAndEvent {
                    state: ApiUpdateState::ACTIVATE_PRE_REBOOT_COMPLETE,
                    update_metadata_type: Some(UpdateMetadataType::System),
                    event: UpdateManagerEvent::BootControlServiceError(ERROR_STR.to_string()),
                },
                target: ApiUpdateState::ACTIVATE_PRE_REBOOT_FAILURE,
            },
            StateTransition {
                source: StateAndEvent {
                    state: ApiUpdateState::ACTIVATE_PRE_REBOOT_COMPLETE,
                    update_metadata_type: Some(UpdateMetadataType::ServiceBundle),
                    event: UpdateManagerEvent::ApexServiceError(ERROR_STR.to_string()),
                },
                target: ApiUpdateState::ACTIVATE_PRE_REBOOT_FAILURE,
            },
            StateTransition {
                source: StateAndEvent {
                    state: ApiUpdateState::ACTIVATE_PRE_REBOOT_FAILURE,
                    update_metadata_type: Some(UpdateMetadataType::System),
                    event: UpdateManagerEvent::RollbackRequest,
                },
                target: ApiUpdateState::ACTIVATE_PRE_REBOOT_ROLLBACK,
            },
            StateTransition {
                source: StateAndEvent {
                    state: ApiUpdateState::ACTIVATE_PRE_REBOOT_FAILURE,
                    update_metadata_type: Some(UpdateMetadataType::ServiceBundle),
                    event: UpdateManagerEvent::RollbackRequest,
                },
                target: ApiUpdateState::ACTIVATE_PRE_REBOOT_ROLLBACK,
            },
            StateTransition {
                source: StateAndEvent {
                    state: ApiUpdateState::ACTIVATE_PRE_REBOOT_ROLLBACK,
                    update_metadata_type: Some(UpdateMetadataType::System),
                    event: UpdateManagerEvent::RollbackComplete,
                },
                target: ApiUpdateState::READY,
            },
            StateTransition {
                source: StateAndEvent {
                    state: ApiUpdateState::ACTIVATE_PRE_REBOOT_ROLLBACK,
                    update_metadata_type: Some(UpdateMetadataType::ServiceBundle),
                    event: UpdateManagerEvent::RollbackComplete,
                },
                target: ApiUpdateState::READY,
            },
            StateTransition {
                source: StateAndEvent {
                    state: ApiUpdateState::ACTIVATE_POST_REBOOT_COMPLETE,
                    update_metadata_type: Some(UpdateMetadataType::System),
                    event: UpdateManagerEvent::RollbackRequest,
                },
                target: ApiUpdateState::ACTIVATE_POST_REBOOT_ROLLBACK,
            },
            StateTransition {
                source: StateAndEvent {
                    state: ApiUpdateState::ACTIVATE_POST_REBOOT_COMPLETE,
                    update_metadata_type: Some(UpdateMetadataType::ServiceBundle),
                    event: UpdateManagerEvent::RollbackRequest,
                },
                target: ApiUpdateState::ACTIVATE_POST_REBOOT_ROLLBACK,
            },
            StateTransition {
                source: StateAndEvent {
                    state: ApiUpdateState::ACTIVATE_POST_REBOOT_COMPLETE,
                    update_metadata_type: Some(UpdateMetadataType::System),
                    event: UpdateManagerEvent::CommitRequest,
                },
                target: ApiUpdateState::COMMIT,
            },
            StateTransition {
                source: StateAndEvent {
                    state: ApiUpdateState::ACTIVATE_POST_REBOOT_COMPLETE,
                    update_metadata_type: Some(UpdateMetadataType::ServiceBundle),
                    event: UpdateManagerEvent::CommitRequest,
                },
                target: ApiUpdateState::COMMIT,
            },
            StateTransition {
                source: StateAndEvent {
                    state: ApiUpdateState::ACTIVATE_POST_REBOOT_COMPLETE,
                    update_metadata_type: Some(UpdateMetadataType::System),
                    event: UpdateManagerEvent::UpdateEngineError(ERROR_STR.to_string()),
                },
                target: ApiUpdateState::ACTIVATE_PRE_REBOOT_FAILURE,
            },
            StateTransition {
                source: StateAndEvent {
                    state: ApiUpdateState::ACTIVATE_POST_REBOOT_COMPLETE,
                    update_metadata_type: Some(UpdateMetadataType::ServiceBundle),
                    event: UpdateManagerEvent::ApexServiceError(ERROR_STR.to_string()),
                },
                target: ApiUpdateState::ACTIVATE_PRE_REBOOT_FAILURE,
            },
            StateTransition {
                source: StateAndEvent {
                    state: ApiUpdateState::ACTIVATE_POST_REBOOT_ROLLBACK,
                    update_metadata_type: Some(UpdateMetadataType::System),
                    event: UpdateManagerEvent::RollbackComplete,
                },
                target: ApiUpdateState::ACTIVATE_POST_REBOOT_ROLLBACK_COMPLETE,
            },
            StateTransition {
                source: StateAndEvent {
                    state: ApiUpdateState::ACTIVATE_POST_REBOOT_ROLLBACK,
                    update_metadata_type: Some(UpdateMetadataType::ServiceBundle),
                    event: UpdateManagerEvent::RollbackComplete,
                },
                target: ApiUpdateState::ACTIVATE_POST_REBOOT_ROLLBACK_COMPLETE,
            },
            StateTransition {
                source: StateAndEvent {
                    state: ApiUpdateState::ACTIVATE_POST_REBOOT_ROLLBACK_COMPLETE,
                    update_metadata_type: Some(UpdateMetadataType::System),
                    event: UpdateManagerEvent::RollbackComplete,
                },
                target: ApiUpdateState::READY,
            },
            StateTransition {
                source: StateAndEvent {
                    state: ApiUpdateState::ACTIVATE_POST_REBOOT_ROLLBACK_COMPLETE,
                    update_metadata_type: Some(UpdateMetadataType::ServiceBundle),
                    event: UpdateManagerEvent::RollbackComplete,
                },
                target: ApiUpdateState::READY,
            },
            StateTransition {
                source: StateAndEvent {
                    state: ApiUpdateState::COMMIT,
                    update_metadata_type: Some(UpdateMetadataType::System),
                    event: UpdateManagerEvent::CommitComplete,
                },
                target: ApiUpdateState::READY,
            },
            StateTransition {
                source: StateAndEvent {
                    state: ApiUpdateState::COMMIT,
                    update_metadata_type: Some(UpdateMetadataType::ServiceBundle),
                    event: UpdateManagerEvent::CommitComplete,
                },
                target: ApiUpdateState::READY,
            },
        ]
    }

    fn get_state_transition_failure_message(
        failed_valid_state_transitions: &[StateTransitionError],
        state_transitions_expected_to_fail: &[StateAndEventError],
    ) -> String {
        let mut failure_message = String::new();

        if !failed_valid_state_transitions.is_empty() {
            failure_message += "-------------------------------\n";
            failure_message += "Failed valid state transitions:\n";
            failure_message += "-------------------------------\n";
        }

        for t in failed_valid_state_transitions {
            failure_message += &format!(
                "    Source: {:?}({:?})\n",
                t.state_transition.source.state, t.state_transition.source.update_metadata_type
            );
            failure_message += &format!("    Event: {:?}\n", t.state_transition.source.event);
            failure_message += &format!("    Target: {:?}\n", t.state_transition.target);
            failure_message += &format!("    ERROR: {:?}\n", t.error);
            failure_message += "-------------------------------\n";
        }

        if !state_transitions_expected_to_fail.is_empty() {
            failure_message += "\n\n";
            failure_message += "------------------------------------\n";
            failure_message += "Succeeded invalid state transitions:\n";
            failure_message += "------------------------------------\n";
        }

        for t in state_transitions_expected_to_fail {
            failure_message += &format!(
                "    Source: {:?}({:?})\n",
                t.state_and_event.state, t.state_and_event.update_metadata_type
            );
            failure_message += &format!("    Event: {:?}\n", t.state_and_event.event);
            failure_message += &format!("    ERROR: {:?}\n", t.error);
            failure_message += "-------------------------------\n";
        }

        failure_message
    }

    #[test]
    fn check_state_transitions() {
        let mut all_state_and_event_combinations = get_all_state_and_event_combinations();
        let valid_state_transitions = get_all_valid_state_transitions();
        let mut failed_valid_state_transitions = Vec::<StateTransitionError>::new();

        for state_transition in &valid_state_transitions {
            let source = get_update_state(
                state_transition.source.state,
                state_transition.source.update_metadata_type.as_ref(),
            );
            let target_result = source.get_next_state(state_transition.source.event.clone());

            if let Err(e) = target_result {
                failed_valid_state_transitions.push(StateTransitionError {
                    state_transition: state_transition.clone(),
                    error: format!("Valid transition returned Error: {e:?}"),
                });
                continue;
            }

            let target = target_result.unwrap();
            if target.value() != state_transition.target {
                failed_valid_state_transitions.push(StateTransitionError {
                    state_transition: state_transition.clone(),
                    error: format!("Incorrect target state: {:?}", target.value()),
                });
                continue;
            }

            let expected_metadata = match state_transition.source.event {
                UpdateManagerEvent::PrepareRequest(ref metadata) => Some(metadata.clone()),
                UpdateManagerEvent::ApexServiceSubmitSuccess(ref apex_infos) => {
                    let mut metadata = source.metadata().unwrap().clone();

                    if let UpdateMetadata::ServiceBundle(ref mut service_bundle) = &mut metadata {
                        service_bundle.submitted_apex_info = apex_infos.clone();
                    } else {
                        panic!("Handling an ApexServiceSubmitSuccess event on a non ServiceBundle update");
                    }

                    Some(metadata)
                }
                _ => match target.value() {
                    ApiUpdateState::READY => None,
                    _ => source.metadata().cloned(),
                },
            };

            if target.metadata() != expected_metadata.as_ref() {
                failed_valid_state_transitions.push(StateTransitionError {
                    state_transition: state_transition.clone(),
                    error: format!("Incorrect metadata {:?}", target.metadata()),
                });
                continue;
            }

            // Removing this valid state transision from all_state_and_event_combinations will
            // eventually leave only the invalid state and event combination in the
            // all_state_and_event_combinations
            if !remove_element(&mut all_state_and_event_combinations, &state_transition.source) {
                panic!(
                    "The element {:?} was expected to be in the all_state_and_event_combinations Vec, however it was not found. This indicates there is a bug in the test that will not allow it to proceed correctly",
                    state_transition.source
                );
            }
        }

        let mut state_transitions_expected_to_fail = Vec::<StateAndEventError>::new();

        // All of the valid state transitions have been removed
        let invalid_state_and_event_combinations = all_state_and_event_combinations;

        for invalid in invalid_state_and_event_combinations {
            let source = get_update_state(invalid.state, invalid.update_metadata_type.as_ref());
            let target_result = source.get_next_state(invalid.event.clone());

            if let Ok(target) = target_result {
                state_transitions_expected_to_fail.push(StateAndEventError {
                    state_and_event: invalid.clone(),
                    error: format!("Got state: {:?}", target.value()),
                });
            }
        }

        let no_errors = failed_valid_state_transitions.is_empty()
            && state_transitions_expected_to_fail.is_empty();

        assert!(
            no_errors,
            "{}",
            get_state_transition_failure_message(
                &failed_valid_state_transitions,
                &state_transitions_expected_to_fail
            )
        );
    }

    #[test]
    fn ready_state_value_set() {
        // Given: a ready state.
        let ready = UpdateState::new();

        // Then: the update state should be READY.
        assert_eq!(ready.value(), ApiUpdateState::READY);
    }

    #[test]
    fn prepare_state_value_set() {
        // Given: a prepare state.
        let prepare =
            UpdateState::InProgress(Arc::new(Prepare { metadata: get_system_metadata() }));

        // Then: the update state should be PREPARE.
        assert_eq!(prepare.value(), ApiUpdateState::PREPARE);
    }

    #[test]
    fn prepare_metadata_value_set() {
        // Given: a prepare state.
        let state = Arc::new(Prepare { metadata: get_system_metadata() });

        // Then: the update metadata should be set.
        assert_eq!(state.metadata().clone(), get_system_metadata());
    }

    #[test]
    fn prepare_complete_state_value_set() {
        // Given: a prepare complete state.
        let prepare_complete =
            UpdateState::InProgress(Arc::new(PrepareComplete { metadata: get_system_metadata() }));

        // Then: the update state should be PREPARE_COMPLETE.
        assert_eq!(prepare_complete.value(), ApiUpdateState::PREPARE_COMPLETE);
    }

    #[test]
    fn prepare_complete_metadata_value_set() {
        // Given: a prepare_complete state.
        let state = Arc::new(PrepareComplete { metadata: get_system_metadata() });

        // Then: the update metadata should be set.
        assert_eq!(state.metadata().clone(), get_system_metadata());
    }

    #[test]
    fn activate_pre_reboot_state_value_set() {
        // Given: a activate pre reboot state.
        let activate_pre_reboot = UpdateState::InProgress(Arc::new(ActivatePreReboot {
            metadata: get_system_metadata(),
        }));

        // Then: the update state should be ACTIVATE_PRE_REBOOT.
        assert_eq!(activate_pre_reboot.value(), ApiUpdateState::ACTIVATE_PRE_REBOOT);
    }

    #[test]
    fn activate_pre_reboot_metadata_value_set() {
        // Given: a activate pre reboot state.
        let state = Arc::new(ActivatePreReboot { metadata: get_system_metadata() });

        // Then: the update metadata should be set.
        assert_eq!(state.metadata().clone(), get_system_metadata());
    }

    #[test]
    fn activate_pre_reboot_complete_state_value_set() {
        // Given: a activate pre reboot complete state.
        let activate_pre_reboot_complete =
            Arc::new(ActivatePreRebootComplete { metadata: get_system_metadata() });

        // Then: the update state should be ACTIVATE_PRE_REBOOT_COMPLETE.
        assert_eq!(
            activate_pre_reboot_complete.value(),
            ApiUpdateState::ACTIVATE_PRE_REBOOT_COMPLETE
        );
    }

    #[test]
    fn activate_pre_reboot_complete_metadata_value_set() {
        // Given: a activate pre-reboot complete state.
        let state = Arc::new(ActivatePreRebootComplete { metadata: get_system_metadata() });

        // Then: the update metadata should be set.
        assert_eq!(state.metadata().clone(), get_system_metadata());
    }

    #[test]
    fn all_states_with_system_save_and_load_succeeds() {
        // Given: a  state.
        for state in get_all_states(UpdateMetadataType::System) {
            let tmp_dir = tempdir().unwrap();
            let path = tmp_dir.path().join("testfile");
            println!("Test: {:?}", state.value());

            // When: ready save and load.
            state.save(&path).unwrap();
            let loaded_state = UpdateState::load(&path).unwrap();

            assert_eq!(state.value(), loaded_state.value());

            match state {
                UpdateState::Ready(_) => (),
                UpdateState::InProgress(state) => {
                    assert!(state.metadata().clone() == get_system_metadata())
                }
                UpdateState::Failed(fs) => assert!(
                    fs.state.metadata().clone() == get_system_metadata()
                        && fs.reason
                            == FailureReason::new(
                                ErrorCode::UpdateEngineError,
                                UPDATE_ENGINE_CODE.to_string(),
                            )
                ),
            }
            tmp_dir.close().unwrap();
        }
    }

    #[test]
    fn all_states_with_sb_save_and_load_succeeds() {
        // Given: a  state.
        for state in get_all_states(UpdateMetadataType::ServiceBundle) {
            let tmp_dir = tempdir().unwrap();
            let path = tmp_dir.path().join("testfile");

            // When: ready save and load.
            state.save(&path).unwrap();
            let loaded_state = UpdateState::load(&path).unwrap();

            assert_eq!(state.value(), loaded_state.value());

            let expected_metadata = get_metadata(state.value(), &UpdateMetadataType::ServiceBundle);
            match state {
                UpdateState::Ready(_) => (),
                UpdateState::InProgress(state) => {
                    assert!(state.metadata().clone() == expected_metadata)
                }
                UpdateState::Failed(fs) => assert!(
                    fs.state.metadata().clone() == expected_metadata
                        && fs.reason
                            == FailureReason::new(
                                ErrorCode::UpdateEngineError,
                                UPDATE_ENGINE_CODE.to_string(),
                            )
                ),
            }
            tmp_dir.close().unwrap();
        }
    }
}

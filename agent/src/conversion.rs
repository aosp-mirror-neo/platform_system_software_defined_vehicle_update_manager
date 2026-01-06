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

use crate::metadata::{
    ApexChildSession, ApexInfo, ServiceBundleUpdateMetadata, SystemUpdateMetadata, UpdateMetadata,
};
use crate::payload::{ServiceBundleUpdatePayload, SystemUpdatePayload, UpdatePayload};
use crate::update_manager_state::{
    ActivatePostRebootComplete, ActivatePostRebootRollback, ActivatePostRebootRollbackComplete,
    ActivatePreReboot, ActivatePreRebootComplete, ActivatePreRebootFailure,
    ActivatePreRebootRollback, Commit, FailureReason, FailureState, Prepare, PrepareCancel,
    PrepareComplete, PrepareFailure, PrepareRollback, PrepareSuspend, PrepareSuspendComplete,
    Ready, UpdateManagerEvent, UpdateState,
};
use apex_aidl_interface::aidl::android::apex::ApexInfo::ApexInfo as ApexServiceApexInfo;
use log::warn;
use protobuf::MessageField;
use sdv_status_rs::{SdvResult, SdvStatus, SdvStatusCode};
use std::sync::Arc;
use update_manager_api::error_code::ErrorCode;
use update_manager_api::failure_reason::FailureReason as FailureReasonProto;
use update_manager_api::payload::{
    ServiceBundleUpdatePayload as ServiceBundleUpdatePayloadProto,
    SystemUpdatePayload as SystemUpdatePayloadProto,
};
use update_manager_api::update_manager_context::update_manager_context::Metadata;
use update_manager_api::update_manager_context::{
    ApexChildSession as ApexChildSessionProto, ApexInfo as ApexInfoProto,
    ServiceBundleUpdateMetadata as ServiceBundleUpdateMetadataProto,
    ServiceBundleUpdatePayload as ServiceBundleUpdatePayloadContextProto,
    SystemUpdateMetadata as SystemUpdateMetadataProto,
    SystemUpdatePayload as SystemUpdatePayloadContextProto, UpdateManagerContext,
};
use update_manager_api::update_manager_service::get_status_response::Payload as GetStatusResponsePayload;
use update_manager_api::update_manager_service::prepare_request::Payload as PrepareRequestPayload;
use update_manager_api::update_manager_status::UpdateManagerStatus;
use update_manager_api::update_state::UpdateState as ApiUpdateState;

impl TryFrom<UpdateManagerContext> for UpdateState {
    type Error = SdvStatus;
    fn try_from(update_manager_context: UpdateManagerContext) -> Result<Self, Self::Error> {
        let state = match update_manager_context.state.enum_value_or_default() {
            ApiUpdateState::READY => UpdateState::Ready(Arc::new(Ready {})),
            ApiUpdateState::PREPARE => UpdateState::InProgress(Arc::new(Prepare {
                metadata: get_metadata(&update_manager_context)?,
            })),
            ApiUpdateState::PREPARE_SUSPEND => UpdateState::InProgress(Arc::new(PrepareSuspend {
                metadata: get_metadata(&update_manager_context)?,
            })),
            ApiUpdateState::PREPARE_SUSPEND_COMPLETE => {
                UpdateState::InProgress(Arc::new(PrepareSuspendComplete {
                    metadata: get_metadata(&update_manager_context)?,
                }))
            }
            ApiUpdateState::PREPARE_COMPLETE => {
                UpdateState::InProgress(Arc::new(PrepareComplete {
                    metadata: get_metadata(&update_manager_context)?,
                }))
            }
            ApiUpdateState::PREPARE_CANCEL => UpdateState::InProgress(Arc::new(PrepareCancel {
                metadata: get_metadata(&update_manager_context)?,
            })),
            ApiUpdateState::PREPARE_ROLLBACK => {
                UpdateState::InProgress(Arc::new(PrepareRollback {
                    metadata: get_metadata(&update_manager_context)?,
                }))
            }
            ApiUpdateState::ACTIVATE_PRE_REBOOT => {
                UpdateState::InProgress(Arc::new(ActivatePreReboot {
                    metadata: get_metadata(&update_manager_context)?,
                }))
            }
            ApiUpdateState::ACTIVATE_PRE_REBOOT_COMPLETE => {
                UpdateState::InProgress(Arc::new(ActivatePreRebootComplete {
                    metadata: get_metadata(&update_manager_context)?,
                }))
            }
            ApiUpdateState::ACTIVATE_PRE_REBOOT_ROLLBACK => {
                UpdateState::InProgress(Arc::new(ActivatePreRebootRollback {
                    metadata: get_metadata(&update_manager_context)?,
                }))
            }
            ApiUpdateState::ACTIVATE_POST_REBOOT_COMPLETE => {
                UpdateState::InProgress(Arc::new(ActivatePostRebootComplete {
                    metadata: get_metadata(&update_manager_context)?,
                }))
            }
            ApiUpdateState::ACTIVATE_POST_REBOOT_ROLLBACK => {
                UpdateState::InProgress(Arc::new(ActivatePostRebootRollback {
                    metadata: get_metadata(&update_manager_context)?,
                }))
            }
            ApiUpdateState::ACTIVATE_POST_REBOOT_ROLLBACK_COMPLETE => {
                UpdateState::InProgress(Arc::new(ActivatePostRebootRollbackComplete {
                    metadata: get_metadata(&update_manager_context)?,
                }))
            }
            ApiUpdateState::COMMIT => UpdateState::InProgress(Arc::new(Commit {
                metadata: get_metadata(&update_manager_context)?,
            })),
            ApiUpdateState::PREPARE_FAILURE => UpdateState::Failed(FailureState {
                state: Arc::new(PrepareFailure {
                    metadata: get_metadata(&update_manager_context)?,
                }),
                reason: get_failure_reason(&update_manager_context)?,
            }),
            ApiUpdateState::ACTIVATE_PRE_REBOOT_FAILURE => UpdateState::Failed(FailureState {
                state: Arc::new(ActivatePreRebootFailure {
                    metadata: get_metadata(&update_manager_context)?,
                }),
                reason: get_failure_reason(&update_manager_context)?,
            }),
        };
        Ok(state)
    }
}

fn get_failure_reason(update_manager_context: &UpdateManagerContext) -> SdvResult<FailureReason> {
    let e = Err(SdvStatus::with_message(
        SdvStatusCode::InvalidArgument,
        "Could not parse FailureReason.",
    ));
    let reason: FailureReasonProto =
        update_manager_context.failure_reason.clone().into_option().ok_or(e)?;
    Ok(reason.into())
}

fn get_metadata(update_manager_context: &UpdateManagerContext) -> SdvResult<UpdateMetadata> {
    let e = Err(SdvStatus::with_message(
        SdvStatusCode::InvalidArgument,
        "Could not parse UpdateMetadata",
    ));
    let metadata: UpdateMetadata = update_manager_context.metadata.clone().ok_or(e)?.try_into()?;
    Ok(metadata)
}

impl From<&UpdateState> for UpdateManagerContext {
    fn from(update_state: &UpdateState) -> Self {
        match update_state {
            UpdateState::Ready(state) => UpdateManagerContext {
                state: state.value().into(),
                metadata: None,
                ..Default::default()
            },
            UpdateState::InProgress(state) => UpdateManagerContext {
                state: state.value().into(),
                metadata: Some(state.metadata().into()),
                ..Default::default()
            },
            UpdateState::Failed(failure_state) => UpdateManagerContext {
                state: failure_state.state.value().into(),
                metadata: Some(failure_state.state.metadata().into()),
                failure_reason: MessageField::some(failure_state.reason.clone().into()),
                ..Default::default()
            },
        }
    }
}

impl From<&UpdateState> for UpdateManagerStatus {
    fn from(update_state: &UpdateState) -> Self {
        match update_state {
            UpdateState::Ready(state) => UpdateManagerStatus {
                update_state: state.value().into(),
                failure_reason: MessageField::none(),
                ..Default::default()
            },
            UpdateState::InProgress(state) => UpdateManagerStatus {
                update_state: state.value().into(),
                failure_reason: MessageField::none(),
                ..Default::default()
            },
            UpdateState::Failed(failure_state) => UpdateManagerStatus {
                update_state: failure_state.state.value().into(),
                failure_reason: MessageField::some(failure_state.reason.clone().into()),
                ..Default::default()
            },
        }
    }
}

impl From<FailureReasonProto> for FailureReason {
    fn from(failure_reason_proto: FailureReasonProto) -> Self {
        failure_reason_proto
            .error_code
            .enum_value()
            .map(|ec| FailureReason::new(ec, failure_reason_proto.error_message.clone()))
            .unwrap_or_else(|e| {
                let message =
                    format!(
                        "Unknown ErrorCode variant: {} with error message \"{}\". Treating as ErrorCode::Unspecified",
                        e, failure_reason_proto.error_message
                    );
                warn!("{message}");
                FailureReason::new(ErrorCode::Unspecified, message)
            })
    }
}

impl From<FailureReason> for FailureReasonProto {
    fn from(failure_reason: FailureReason) -> Self {
        FailureReasonProto {
            error_code: failure_reason.error_code.into(),
            error_message: failure_reason.error_message,
            ..Default::default()
        }
    }
}

impl From<UpdateManagerEvent> for FailureReason {
    fn from(update_manager_event: UpdateManagerEvent) -> Self {
        match update_manager_event {
            UpdateManagerEvent::UpdateEngineError(error_message) => {
                FailureReason::new(ErrorCode::UpdateEngineError, error_message)
            }
            UpdateManagerEvent::ApexServiceError(error_message) => {
                FailureReason::new(ErrorCode::ApexServiceError, error_message)
            }
            UpdateManagerEvent::BootControlServiceError(error_message) => {
                FailureReason::new(ErrorCode::BootControlServiceError, error_message)
            }
            UpdateManagerEvent::VoldServiceError(error_message) => {
                FailureReason::new(ErrorCode::VoldServiceError, error_message)
            }
            UpdateManagerEvent::UpdateEngineCode(error_code) => FailureReason::new(
                ErrorCode::UpdateEngineError,
                format!("Received error code in onStatusUpdate callback: {error_code}"),
            ),
            _ => FailureReason::new(
                ErrorCode::Unspecified,
                format!(
                    "The event {update_manager_event:?} does not correspond to a FailureReason"
                ),
            ),
        }
    }
}

impl TryFrom<PrepareRequestPayload> for UpdatePayload {
    type Error = SdvStatus;

    fn try_from(value: PrepareRequestPayload) -> Result<Self, Self::Error> {
        match value {
            PrepareRequestPayload::SystemUpdatePayload(request_system_update_payload) => {
                Ok(UpdatePayload::System(request_system_update_payload.try_into()?))
            }
            PrepareRequestPayload::ServiceBundleUpdatePayload(
                request_service_bundle_update_payload,
            ) => {
                Ok(UpdatePayload::ServiceBundle(request_service_bundle_update_payload.try_into()?))
            }
            _ => Err(SdvStatus::with_message(SdvStatusCode::InvalidArgument, "Malformed payload")),
        }
    }
}

impl From<UpdatePayload> for GetStatusResponsePayload {
    fn from(value: UpdatePayload) -> GetStatusResponsePayload {
        match value {
            UpdatePayload::System(system) => {
                GetStatusResponsePayload::SystemUpdatePayload(system.into())
            }
            UpdatePayload::ServiceBundle(service_bundle) => {
                GetStatusResponsePayload::ServiceBundleUpdatePayload(service_bundle.into())
            }
        }
    }
}

impl TryFrom<ServiceBundleUpdatePayloadProto> for ServiceBundleUpdatePayload {
    type Error = SdvStatus;

    fn try_from(
        value: ServiceBundleUpdatePayloadProto,
    ) -> Result<ServiceBundleUpdatePayload, Self::Error> {
        ServiceBundleUpdatePayload::new(value.apex_path, value.boot_attempts)
    }
}

impl From<ServiceBundleUpdatePayload> for ServiceBundleUpdatePayloadProto {
    fn from(value: ServiceBundleUpdatePayload) -> ServiceBundleUpdatePayloadProto {
        ServiceBundleUpdatePayloadProto {
            apex_path: value.apex_paths,
            boot_attempts: value.boot_attempts,
            ..Default::default()
        }
    }
}

impl TryFrom<ServiceBundleUpdatePayloadContextProto> for ServiceBundleUpdatePayload {
    type Error = SdvStatus;

    fn try_from(
        value: ServiceBundleUpdatePayloadContextProto,
    ) -> Result<ServiceBundleUpdatePayload, Self::Error> {
        ServiceBundleUpdatePayload::new(value.apex_path, value.boot_attempts)
    }
}

impl From<ServiceBundleUpdatePayload> for ServiceBundleUpdatePayloadContextProto {
    fn from(value: ServiceBundleUpdatePayload) -> ServiceBundleUpdatePayloadContextProto {
        ServiceBundleUpdatePayloadContextProto {
            apex_path: value.apex_paths,
            boot_attempts: value.boot_attempts,
            ..Default::default()
        }
    }
}

impl From<ApexInfoProto> for ApexInfo {
    fn from(value: ApexInfoProto) -> ApexInfo {
        ApexInfo { module_name: value.module_name, version_code: value.version_code }
    }
}

impl From<ApexInfo> for ApexInfoProto {
    fn from(value: ApexInfo) -> ApexInfoProto {
        ApexInfoProto {
            module_name: value.module_name,
            version_code: value.version_code,
            ..Default::default()
        }
    }
}

impl From<ApexServiceApexInfo> for ApexInfo {
    fn from(value: ApexServiceApexInfo) -> ApexInfo {
        ApexInfo { module_name: value.moduleName, version_code: value.versionCode }
    }
}

impl From<&ServiceBundleUpdateMetadata> for ServiceBundleUpdatePayload {
    fn from(value: &ServiceBundleUpdateMetadata) -> ServiceBundleUpdatePayload {
        ServiceBundleUpdatePayload {
            apex_paths: value.apex_child_sessions.iter().map(|x| x.apex_path.clone()).collect(),
            boot_attempts: value.boot_attempts,
        }
    }
}

impl TryFrom<Metadata> for UpdateMetadata {
    type Error = SdvStatus;
    fn try_from(metadata: Metadata) -> Result<Self, Self::Error> {
        let err = SdvStatus::with_message(
            SdvStatusCode::InvalidArgument,
            "Error deserializing metadata proto.",
        );
        match metadata {
            Metadata::SystemUpdateMetadata(metadata) => {
                let system_payload = SystemUpdateMetadata {
                    payload: metadata.payload.into_option().ok_or(Err(err))?.try_into()?,
                    initial_boot_slot: metadata.initial_boot_slot,
                };
                Ok(UpdateMetadata::System(system_payload))
            }
            Metadata::ServiceBundleUpdateMetadata(metadata) => {
                let sb_metadata = ServiceBundleUpdateMetadata::new(
                    metadata.apex_child_sessions.into_iter().map(|x| x.into()).collect(),
                    metadata.parent_session_id,
                    metadata.boot_attempts,
                    metadata.submitted_apex_info.into_iter().map(|x| x.into()).collect(),
                );
                Ok(UpdateMetadata::ServiceBundle(sb_metadata))
            }
            _ => Err(err),
        }
    }
}

impl From<&UpdateMetadata> for Metadata {
    fn from(metadata: &UpdateMetadata) -> Self {
        match metadata {
            UpdateMetadata::System(metadata) => {
                let system_payload = SystemUpdateMetadataProto {
                    payload: MessageField::some(metadata.payload.clone().into()),
                    initial_boot_slot: metadata.initial_boot_slot,
                    ..Default::default()
                };
                Metadata::SystemUpdateMetadata(system_payload)
            }
            UpdateMetadata::ServiceBundle(metadata) => {
                let sb_metadata = ServiceBundleUpdateMetadataProto {
                    apex_child_sessions: metadata
                        .apex_child_sessions
                        .clone()
                        .into_iter()
                        .map(|x| x.into())
                        .collect(),
                    parent_session_id: metadata.parent_session_id,
                    boot_attempts: metadata.boot_attempts,
                    submitted_apex_info: metadata
                        .submitted_apex_info
                        .clone()
                        .into_iter()
                        .map(|x| x.into())
                        .collect(),
                    ..Default::default()
                };
                Metadata::ServiceBundleUpdateMetadata(sb_metadata)
            }
        }
    }
}

impl From<&UpdateMetadata> for UpdatePayload {
    fn from(metadata: &UpdateMetadata) -> Self {
        match metadata {
            UpdateMetadata::System(system) => UpdatePayload::System(system.payload.clone()),
            UpdateMetadata::ServiceBundle(service_bundle) => {
                UpdatePayload::ServiceBundle(service_bundle.into())
            }
        }
    }
}

impl TryFrom<SystemUpdatePayloadProto> for SystemUpdatePayload {
    type Error = SdvStatus;

    fn try_from(value: SystemUpdatePayloadProto) -> Result<SystemUpdatePayload, Self::Error> {
        let SystemUpdatePayloadProto {
            payload,
            payload_offset,
            payload_size,
            header_key_value_pairs,
            ..
        } = value;
        let payload_offset = payload_offset.ok_or_else(|| {
            SdvStatus::with_message(SdvStatusCode::InvalidArgument, "Missing payload_offset")
        })?;
        let payload_size = payload_size.ok_or_else(|| {
            SdvStatus::with_message(SdvStatusCode::InvalidArgument, "Missing payload_size")
        })?;

        SystemUpdatePayload::new(&payload, payload_offset, payload_size, &header_key_value_pairs)
    }
}

impl From<SystemUpdatePayload> for SystemUpdatePayloadProto {
    fn from(value: SystemUpdatePayload) -> SystemUpdatePayloadProto {
        SystemUpdatePayloadProto {
            payload: value.payload,
            payload_offset: Some(value.payload_offset),
            payload_size: Some(value.payload_size),
            header_key_value_pairs: value.header_key_value_pairs,
            ..Default::default()
        }
    }
}

impl TryFrom<SystemUpdatePayloadContextProto> for SystemUpdatePayload {
    type Error = SdvStatus;

    fn try_from(
        value: SystemUpdatePayloadContextProto,
    ) -> Result<SystemUpdatePayload, Self::Error> {
        let SystemUpdatePayloadContextProto {
            payload,
            payload_offset,
            payload_size,
            header_key_value_pairs,
            ..
        } = value;

        SystemUpdatePayload::new(&payload, payload_offset, payload_size, &header_key_value_pairs)
    }
}

impl From<SystemUpdatePayload> for SystemUpdatePayloadContextProto {
    fn from(value: SystemUpdatePayload) -> SystemUpdatePayloadContextProto {
        SystemUpdatePayloadContextProto {
            payload: value.payload,
            payload_offset: value.payload_offset,
            payload_size: value.payload_size,
            header_key_value_pairs: value.header_key_value_pairs,
            ..Default::default()
        }
    }
}

impl From<ApexChildSessionProto> for ApexChildSession {
    fn from(value: ApexChildSessionProto) -> ApexChildSession {
        ApexChildSession { apex_path: value.apex_path, session_id: value.session_id }
    }
}

impl From<ApexChildSession> for ApexChildSessionProto {
    fn from(value: ApexChildSession) -> ApexChildSessionProto {
        ApexChildSessionProto {
            apex_path: value.apex_path,
            session_id: value.session_id,
            ..Default::default()
        }
    }
}

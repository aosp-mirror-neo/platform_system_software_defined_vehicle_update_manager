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
use crate::boot_control_service::BootControlService;
use crate::check_metadata;
use crate::metadata::{
    ServiceBundleUpdateMetadata, SystemUpdateMetadata, UpdateMetadata,
    APEX_SERVICE_SESSION_DIR_PREFIX,
};
use crate::ota_package::OtaPackage;
use crate::payload::{ServiceBundleUpdatePayload, SystemUpdatePayload, UpdatePayload};
use crate::state_machine::{StateMachine, SAVE_STATE_FILE};
use crate::subscriber_manager::SubscriberManager;
use crate::update_engine::{UpdateEngine, UpdateEngineClient, UpdateEngineMessage};
use crate::update_manager_state::{UpdateManagerEvent, UpdateManagerState};
use crate::vold::VoldService;
use crate::vpm::PowerStateClient;
use crate::vsidl_update_manager_listener_service_client_provider::UpdateManagerListenerServiceClientProvider;
use async_trait::async_trait;
use com_sdv_google_update_manager_update_manager_service_rpc::Interface as UpdateManagerService;
use google_sdv_vpm::aidl::google::sdv::vpm::PowerStateReport::PowerStateReport;
use log::{debug, error};
use protobuf::MessageField;
use sdv_comms::id::ServiceFqin;
use sdv_status_rs::{derive, SdvResult, SdvStatus, SdvStatusCode};
use std::collections::HashMap;
use std::fs;
use std::fs::DirEntry;
use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::mpsc::{unbounded_channel, UnboundedReceiver};
use update_manager_api::update_manager_listener_service::on_status_update_request::Status_update as StatusUpdate;
use update_manager_api::update_manager_listener_service::UpdateProgress;
use update_manager_api::update_manager_service::prepare_request::Payload as PrepareRequestPayload;
use update_manager_api::update_manager_service::{
    ActivateRequest, ActivateResponse, CommitRequest, CommitResponse, GetStatusRequest,
    GetStatusResponse, PrepareRequest, PrepareResponse, ResumeRequest, ResumeResponse,
    RollbackRequest, RollbackResponse, SubscribeToStatusUpdatesRequest,
    SubscribeToStatusUpdatesResponse, SuspendRequest, SuspendResponse, UninstallApexRequest,
    UninstallApexResponse, UnsubscribeFromStatusUpdatesRequest,
    UnsubscribeFromStatusUpdatesResponse,
};
use update_manager_api::update_state::UpdateState as ApiUpdateState;

const CARE_MAP_PERMISSIONS: u32 = 0o640;
const OTA_PACKAGE_PATH: &str = "/data/ota_package/";
const CARE_MAP_FILE_NAME: &str = "care_map.pb";

fn power_state_report_to_update_manager_event(
    value: PowerStateReport,
) -> Option<UpdateManagerEvent> {
    match value {
        PowerStateReport::POWER_OFF_POST_FINISH => Some(UpdateManagerEvent::PowerOffPostFinish),
        PowerStateReport::SUSPEND_TO_RAM_POST_FINISH => {
            Some(UpdateManagerEvent::SuspendToRamPostFinish)
        }
        PowerStateReport::SUSPEND_TO_DISK_POST_FINISH => {
            Some(UpdateManagerEvent::SuspendToDiskPostFinish)
        }
        _ => None,
    }
}

fn parse_session_id_from_directory(entry: Result<DirEntry, std::io::Error>) -> Option<usize> {
    let path = entry
        .map_err(|e| error!("Failed while reading the app staging directory: {e:?}"))
        .ok()?
        .path();

    path.file_name()
        .or_else(|| {
            error!("Path {} has invalid characters", path.display());
            None
        })?
        .to_str()
        .or_else(|| {
            error!("Path {} has a bad file name", path.display());
            None
        })?
        .strip_prefix(APEX_SERVICE_SESSION_DIR_PREFIX)?
        .parse()
        .ok()
}

/* This function finds the largest session id number in app_staging_dir and
 * returns a number one greater than this. For example, if the directory with
 * the largest session number was session_50, then this would return 51.*/
fn get_unused_session_id(app_staging_dir: &Path) -> SdvResult<usize> {
    let entry_read_dir = std::fs::read_dir(app_staging_dir)
        .map_err(derive!(SdvStatusCode::Internal, "Failed to read the app staging directory"))?;

    let greatest_session_id: usize = entry_read_dir.fold(0, |acc, entry| {
        if let Some(id) = parse_session_id_from_directory(entry) {
            if id > acc {
                return id;
            }
        }

        acc
    });

    if greatest_session_id == i32::MAX as usize {
        return Err(SdvStatus::with_message(
            SdvStatusCode::ResourceExhausted,
            "Could not find an available session id",
        ));
    }

    Ok(greatest_session_id + 1)
}

pub struct UpdateManagerInit {
    pub apex_service: Arc<dyn ApexService>,
    pub app_staging_dir: PathBuf,
    pub boot_control_service: Arc<dyn BootControlService>,
    pub power_state_client: Arc<dyn PowerStateClient>,
    pub power_state_report_receiver: UnboundedReceiver<PowerStateReport>,
    pub update_engine: Box<dyn UpdateEngine>,
    pub update_manager_listener_service_client_provider:
        Box<dyn UpdateManagerListenerServiceClientProvider>,
    pub vold_service: Arc<dyn VoldService>,
}

pub struct UpdateManagerEnvironment {
    pub apex_service: Arc<dyn ApexService>,
    pub app_staging_dir: PathBuf,
    pub boot_control_service: Arc<dyn BootControlService>,
    pub ota_package_dir: PathBuf,
    pub power_state_client: Arc<dyn PowerStateClient>,
    pub subscriber_manager: Arc<SubscriberManager>,
    pub update_engine_client: Arc<UpdateEngineClient>,
    pub vold_service: Arc<dyn VoldService>,
}

#[derive(Clone)]
pub struct UpdateManagerServiceImpl {
    env: Arc<UpdateManagerEnvironment>,
    state_machine: Arc<StateMachine>,
}

impl UpdateManagerServiceImpl {
    pub async fn new(init: UpdateManagerInit) -> SdvResult<Self> {
        Self::new_with_paths(init, Path::new(OTA_PACKAGE_PATH), Path::new(SAVE_STATE_FILE)).await
    }

    async fn new_with_paths(
        mut init: UpdateManagerInit,
        ota_package_path: &Path,
        save_state_path: &Path,
    ) -> SdvResult<Self> {
        let required_dirs = [&init.app_staging_dir, ota_package_path];

        // Fail early if a required directory is inaccessible
        for required_dir in required_dirs {
            required_dir
                .try_exists()
                .map_err(derive!(
                    SdvStatusCode::NotFound,
                    format!("Error accessing required directory {}", required_dir.display())
                ))
                .and_then(|accessible| match accessible {
                    true => Ok(()),
                    false => Err(SdvStatus::with_message(
                        SdvStatusCode::NotFound,
                        format!("{} is inaccessible", required_dir.display()),
                    )),
                })?;
        }

        let (update_engine_sender, mut update_engine_receiver) = unbounded_channel();

        // This timeout is an arbitrary value. It should provide enough time to receive a status
        // update from UpdateEngine without waiting forever, thus allowing the Update Manager to
        // start. Increase or decrease as needed. At this point the UpdateEngine binder interface
        // is already connected, so no need to include waiting for the interface in this timeout.
        const UPDATE_ENGINE_STATUS_TIMEOUT: u64 = 3;
        let initial_status_timeout = Duration::from_secs(UPDATE_ENGINE_STATUS_TIMEOUT);
        let update_engine_client = Arc::new(
            UpdateEngineClient::new(
                init.update_engine,
                update_engine_sender,
                initial_status_timeout,
            )
            .await?,
        );
        let subscriber_manager =
            Arc::new(SubscriberManager::new(init.update_manager_listener_service_client_provider));

        let env = Arc::new(UpdateManagerEnvironment {
            apex_service: init.apex_service,
            app_staging_dir: init.app_staging_dir,
            boot_control_service: init.boot_control_service,
            ota_package_dir: ota_package_path.to_path_buf(),
            power_state_client: init.power_state_client.clone(),
            subscriber_manager: subscriber_manager.clone(),
            update_engine_client,
            vold_service: init.vold_service,
        });

        let state_machine = Arc::new(StateMachine::new(env.clone(), save_state_path).await?);

        let update_engine_state_machine = state_machine.clone();
        let update_engine_subscriber_manager = subscriber_manager.clone();
        tokio::spawn(async move {
            while let Some(msg) = update_engine_receiver.recv().await {
                match msg {
                    UpdateEngineMessage::StatusUpdate { status_code, percentage } => {
                        debug!("UEM::StatusUpdate - {percentage}%, {status_code}");

                        let state = update_engine_state_machine.get_state().await;

                        let in_progress = {
                            const STATES_WITH_PROGRESS_EVENTS: [ApiUpdateState; 2] =
                                [ApiUpdateState::PREPARE, ApiUpdateState::ACTIVATE_PRE_REBOOT];
                            if STATES_WITH_PROGRESS_EVENTS.contains(&state.value()) {
                                state
                                    .metadata()
                                    .map(|metadata| check_metadata!(metadata, System))
                                    .unwrap_or(false)
                            } else {
                                false
                            }
                        };

                        if in_progress {
                            update_engine_subscriber_manager
                                .notify(StatusUpdate::UpdateProgress(UpdateProgress {
                                    status_code,
                                    percentage,
                                    ..Default::default()
                                }))
                                .await;
                        }
                    }
                    UpdateEngineMessage::PayloadApplicationComplete { error_code } => {
                        if let Err(e) = update_engine_state_machine
                            .handle_event(UpdateManagerEvent::UpdateEngineCode(error_code))
                        {
                            error!("Error when handling event UpdateEngineMessage::PayloadApplicationComplete({error_code}): {e:?}");
                        }
                    }
                }
            }
        });

        let vpm_state_machine = state_machine.clone();
        tokio::spawn(async move {
            while let Some(msg) = init.power_state_report_receiver.recv().await {
                if let Some(update_manager_event) = power_state_report_to_update_manager_event(msg)
                {
                    match vpm_state_machine
                        .handle_event_and_wait_for_response(update_manager_event.clone())
                        .await
                    {
                        // If the transition succeeds, the state is required to report that the
                        // PowerStartReport processing has completed. Otherwise, we do it here.
                        Ok(_) => continue,
                        Err(e) => error!(
                            "Error when getting new state due to {update_manager_event:?}: {e:?}"
                        ),
                    }
                }

                if let Err(e) = init.power_state_client.power_state_report_processing_complete() {
                    error!("Failed to indicate PowerStateReport processing as complete: {e:?}");
                }
            }
        });

        Ok(UpdateManagerServiceImpl { env, state_machine })
    }

    async fn prepare(&self, request: PrepareRequest) -> SdvResult<()> {
        let payload = request.payload.ok_or_else(|| {
            SdvStatus::with_message(SdvStatusCode::InvalidArgument, "Must supply a payload")
        })?;
        match payload {
            PrepareRequestPayload::SystemUpdatePayload(system_payload_proto) => {
                let ota_package = OtaPackage::with_zip_file(&system_payload_proto.payload)?;
                let offset =
                    system_payload_proto.payload_offset.unwrap_or(ota_package.payload_info.offset);
                let size =
                    system_payload_proto.payload_size.unwrap_or(ota_package.payload_info.size);

                let header_key_value_pairs =
                    if system_payload_proto.header_key_value_pairs.is_empty() {
                        ota_package.payload_info.header_key_value_pairs.clone()
                    } else {
                        system_payload_proto.header_key_value_pairs.clone()
                    };
                let care_map_data = ota_package
                    .extract_care_map()
                    .map_err(derive!(SdvStatusCode::Internal, "Failed to extract care_map"))?;

                let care_map_path = self.env.ota_package_dir.join(CARE_MAP_FILE_NAME);
                std::fs::write(&care_map_path, care_map_data)
                    .map_err(derive!(SdvStatusCode::Internal, "Failed to write care_map.pb"))?;

                // Set permissions to 640 (read/write for the owner, read only for the group, no
                // access for public).
                let perms = fs::Permissions::from_mode(CARE_MAP_PERMISSIONS);
                fs::set_permissions(&care_map_path, perms).map_err(derive!(
                    SdvStatusCode::Internal,
                    "Failed to set permissions for care_map.pb"
                ))?;

                let initial_boot_slot = self.env.boot_control_service.get_current_slot().await?;

                let system_payload = SystemUpdatePayload::new(
                    &system_payload_proto.payload,
                    offset,
                    size,
                    &header_key_value_pairs,
                )?;

                let metadata = SystemUpdateMetadata::new(system_payload, initial_boot_slot);
                let metadata = UpdateMetadata::System(metadata);
                self.state_machine
                    .handle_event_and_wait_for_response(UpdateManagerEvent::PrepareRequest(
                        metadata,
                    ))
                    .await
                    .map_err(|e| {
                        SdvStatus::with_message(
                            SdvStatusCode::Internal,
                            format!("Failed to transition state machine: {e:?}"),
                        )
                    })?;
            }
            PrepareRequestPayload::ServiceBundleUpdatePayload(service_bundle_payload) => {
                let session_id = get_unused_session_id(&self.env.app_staging_dir)?;

                let session_id: i32 = session_id.try_into().map_err(derive!(
                    SdvStatusCode::Internal,
                    "Failed to convert session_id to store in metadata"
                ))?;

                let local_service_bundle_payload: ServiceBundleUpdatePayload =
                    service_bundle_payload.try_into()?;

                let service_bundle_update_metadata =
                    ServiceBundleUpdateMetadata::from_payload_and_parent_session_id(
                        local_service_bundle_payload,
                        session_id,
                    )?;

                let metadata =
                    UpdateMetadata::ServiceBundle(service_bundle_update_metadata.clone());

                self.state_machine
                    .handle_event_and_wait_for_response(UpdateManagerEvent::PrepareRequest(
                        metadata,
                    ))
                    .await?;
            }
            _ => {
                return Err(SdvStatus::with_message(
                    SdvStatusCode::InvalidArgument,
                    "Unsupported payload type",
                ))
            }
        }
        Ok(())
    }

    async fn activate(&self) -> SdvResult<()> {
        self.state_machine
            .handle_event_and_wait_for_response(UpdateManagerEvent::ActivateRequest)
            .await
    }

    async fn commit(&self) -> SdvResult<()> {
        self.state_machine
            .handle_event_and_wait_for_response(UpdateManagerEvent::CommitRequest)
            .await
    }

    async fn rollback(&self) -> SdvResult<()> {
        self.state_machine
            .handle_event_and_wait_for_response(UpdateManagerEvent::RollbackRequest)
            .await
    }

    async fn suspend(&self) -> SdvResult<()> {
        self.state_machine
            .handle_event_and_wait_for_response(UpdateManagerEvent::SuspendRequest)
            .await
    }

    async fn resume(&self) -> SdvResult<()> {
        self.state_machine
            .handle_event_and_wait_for_response(UpdateManagerEvent::ResumeRequest)
            .await
    }
}

#[async_trait]
impl UpdateManagerService for UpdateManagerServiceImpl {
    async fn SubscribeToStatusUpdates(
        &self,
        caller_id: ServiceFqin,
        _request: &SubscribeToStatusUpdatesRequest,
    ) -> SdvResult<SubscribeToStatusUpdatesResponse> {
        let state = self.state_machine.get_state().await;
        let status_update = StatusUpdate::UpdateManagerStatus((&state).into());
        self.env
            .subscriber_manager
            .subscribe(
                caller_id.get_sdv_package_name(),
                caller_id.get_service_bundle_name(),
                status_update,
            )
            .await?;

        Ok(SubscribeToStatusUpdatesResponse::default())
    }

    async fn UnsubscribeFromStatusUpdates(
        &self,
        caller_id: ServiceFqin,
        _request: &UnsubscribeFromStatusUpdatesRequest,
    ) -> SdvResult<UnsubscribeFromStatusUpdatesResponse> {
        self.env
            .subscriber_manager
            .unsubscribe(caller_id.get_sdv_package_name(), caller_id.get_service_bundle_name())
            .await;

        Ok(UnsubscribeFromStatusUpdatesResponse::default())
    }

    async fn GetStatus(
        &self,
        _: ServiceFqin,
        _request: &GetStatusRequest,
    ) -> SdvResult<GetStatusResponse> {
        let update_state = self.state_machine.get_state().await;

        Ok(GetStatusResponse {
            update_manager_status: MessageField::some((&update_state).into()),
            payload: update_state.metadata().map(|m| UpdatePayload::from(m).into()),
            ..Default::default()
        })
    }

    async fn Prepare(
        &self,
        _: ServiceFqin,
        request: &PrepareRequest,
    ) -> SdvResult<PrepareResponse> {
        self.prepare(request.clone()).await?;

        Ok(PrepareResponse::default())
    }
    async fn Activate(
        &self,
        _: ServiceFqin,
        _request: &ActivateRequest,
    ) -> SdvResult<ActivateResponse> {
        self.activate().await?;

        Ok(ActivateResponse::default())
    }

    async fn Commit(&self, _: ServiceFqin, _request: &CommitRequest) -> SdvResult<CommitResponse> {
        self.commit().await?;

        Ok(CommitResponse::default())
    }

    async fn Rollback(
        &self,
        _: ServiceFqin,
        _request: &RollbackRequest,
    ) -> SdvResult<RollbackResponse> {
        self.rollback().await?;

        Ok(RollbackResponse::default())
    }

    async fn UninstallApex(
        &self,
        _: ServiceFqin,
        request: &UninstallApexRequest,
    ) -> SdvResult<UninstallApexResponse> {
        let all_active_apexes = self
            .env
            .apex_service
            .get_active_packages()
            .await
            .map_err(derive!(SdvStatusCode::Internal, "Failed to get the active APEXes"))?;
        let active_apex_map = HashMap::<
            String,
            apex_aidl_interface::aidl::android::apex::ApexInfo::ApexInfo,
        >::from_iter(
            all_active_apexes
                .into_iter()
                .map(|apex_info| (apex_info.moduleName.clone(), apex_info)),
        );

        let mut apex_paths = vec![];
        for apex_to_uninstall in &request.apexes {
            let Some(apex_info) = active_apex_map.get(&apex_to_uninstall.module_name) else {
                return SdvResult::Err(SdvStatus::with_message(
                    SdvStatusCode::Internal,
                    format!("Failed to find active apex {:?}", apex_to_uninstall.module_name),
                ));
            };
            if apex_info.versionCode == apex_to_uninstall.version_code {
                apex_paths.push(apex_info.modulePath.clone());
            } else {
                return SdvResult::Err(SdvStatus::with_message(
                    SdvStatusCode::Internal,
                    format!(
                        "Failed to find active version of {:?} for apex {:?}",
                        apex_to_uninstall.version_code, apex_to_uninstall.module_name
                    ),
                ));
            }
        }
        self.env.apex_service.unstage_packages(&apex_paths).await?;

        Ok(UninstallApexResponse::default())
    }

    async fn Suspend(
        &self,
        _: ServiceFqin,
        _request: &SuspendRequest,
    ) -> SdvResult<SuspendResponse> {
        self.suspend().await?;

        Ok(SuspendResponse::default())
    }

    async fn Resume(&self, _: ServiceFqin, _request: &ResumeRequest) -> SdvResult<ResumeResponse> {
        self.resume().await?;

        Ok(ResumeResponse::default())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::apex_service::MockApexService;
    use crate::boot_control_service::MockBootControlService;
    use crate::metadata::APEX_SERVICE_SESSION_DIR_PREFIX;
    use crate::payload::{ServiceBundleUpdatePayload, SystemUpdatePayload};
    use crate::update_engine::error_code::{SUCCESS, UPDATED_BUT_NOT_ACTIVE, USER_CANCELED};
    use crate::update_engine::status_code::IDLE;
    use crate::update_engine::{UpdateEngine, UpdateEngineCallbacks};
    use crate::vold::MockVoldService;
    use crate::vpm::MockPowerStateClient;
    use crate::vsidl_update_manager_listener_service_client_provider::{
        UpdateManagerListenerService, UpdateManagerListenerServiceInfo,
    };
    use apex_aidl_interface::aidl::android::apex::{
        ApexInfo::ApexInfo as ApexServiceApexInfo, ApexInfoList::ApexInfoList,
    };
    use std::sync::Mutex;
    use tempfile::{tempdir, TempDir};
    use tokio::sync::mpsc::UnboundedSender;
    use tokio::time::Duration;
    use update_manager_api::error_code::ErrorCode;
    use update_manager_api::payload::{
        ServiceBundleUpdatePayload as ServiceBundleUpdatePayloadProto,
        SystemUpdatePayload as SystemUpdatePayloadProto,
    };
    use update_manager_api::update_manager_listener_service::on_status_update_request::Status_update as StatusUpdate;
    use update_manager_api::update_manager_listener_service::{
        OnStatusUpdateRequest, OnStatusUpdateResponse,
    };
    use update_manager_api::update_manager_service::prepare_request::Payload as PrepareRequestPayload;
    use update_manager_api::update_manager_service::{
        uninstall_apex_request::ApexInfo, UninstallApexRequest,
    };
    use update_manager_api::update_state::UpdateState as ApiUpdateState;

    const PACKAGE: &str = "com.test";
    const BUNDLE: &str = "TestUpdateManagerClient";

    const STATE_CHANGE_TIMEOUT: Duration = Duration::from_secs(1);

    const SERVICE_BUNDLE_UPDATE_RETRIES: i32 = 2;
    // This value is used for the initial status code sent back from the FakeUpdateEngine
    const STATUS_UPDATE_PERCENTAGE_INIT: f32 = 0.0;
    const STATUS_UPDATE_PERCENTAGE_ACTIVE: f32 = 1.0;

    // This is only needs to be not IDLE. The exact value doesn't matter for the test.
    const DOWNLOADING: i32 = 3;

    pub enum SignedApexType {
        ValidSignedApex,
        BadSignedApex,
    }

    fn string_pairs_as_header_map(string_pairs: &[String]) -> HashMap<String, String> {
        let mut map = HashMap::new();

        for pair in string_pairs {
            let parts: Vec<&str> = pair.splitn(2, '=').collect();
            let key = parts[0].to_string();
            let value = parts[1].to_string();
            map.insert(key, value);
        }

        map
    }

    fn make_service_fqin() -> ServiceFqin {
        ServiceFqin::builder()
            .sdv_vm_name("vm")
            .sdv_package_name(PACKAGE)
            .service_bundle_name(BUNDLE)
            .service_instance_name("instance1")
            .build()
            .unwrap()
    }

    struct TempDirs {
        app_staging: TempDir,
        ota_package: TempDir,
        save_state: TempDir,
    }

    #[derive(Clone)]
    struct TempPaths {
        app_staging: PathBuf,
        ota_package: PathBuf,
        save_state: PathBuf,
    }

    impl TempDirs {
        fn new() -> Self {
            Self {
                app_staging: tempdir().unwrap(),
                ota_package: tempdir().unwrap(),
                save_state: tempdir().unwrap(),
            }
        }

        fn as_temp_paths(&self) -> TempPaths {
            TempPaths {
                app_staging: self.app_staging.path().to_path_buf(),
                ota_package: self.ota_package.path().to_path_buf(),
                save_state: self.save_state.path().to_path_buf(),
            }
        }
    }

    struct MockServices {
        apex_service: MockApexService,
        boot_control_service: MockBootControlService,
        power_state_client: MockPowerStateClient,
        update_engine: FakeUpdateEngine,
        vold_service: MockVoldService,
    }

    impl MockServices {
        fn new() -> Self {
            Self {
                apex_service: MockApexService::default(),
                boot_control_service: MockBootControlService::new(),
                power_state_client: MockPowerStateClient::new(),
                update_engine: FakeUpdateEngine::new(Arc::new(Mutex::new(
                    FakeUpdateEngineTestDetails::new(),
                ))),
                vold_service: MockVoldService::new(),
            }
        }
    }

    struct TestContext {
        callback_receiver: UnboundedReceiver<OnStatusUpdateRequestMessage>,
        fake_update_engine_test_details: Arc<Mutex<FakeUpdateEngineTestDetails>>,
        power_state_report_sender: UnboundedSender<PowerStateReport>,
        save_state_path: PathBuf,
    }

    impl TestContext {
        fn new(temp_paths: TempPaths, mock_services: MockServices) -> (Self, UpdateManagerInit) {
            let (fake_update_manager_listener_service_client_provider, callback_receiver) =
                FakeUpdateManagerListenerServiceClientProvider::new();
            let (power_state_report_sender, power_state_report_receiver) = unbounded_channel();
            let update_manager_listener_service_client_provider =
                Box::new(fake_update_manager_listener_service_client_provider);
            let fake_update_engine_test_details =
                mock_services.update_engine.get_fake_update_engine_test_details();

            (
                Self {
                    callback_receiver,
                    fake_update_engine_test_details,
                    power_state_report_sender,
                    save_state_path: temp_paths.save_state.join("state.bp").to_path_buf(),
                },
                UpdateManagerInit {
                    apex_service: Arc::new(mock_services.apex_service),
                    app_staging_dir: temp_paths.app_staging.to_path_buf(),
                    boot_control_service: Arc::new(mock_services.boot_control_service),
                    power_state_client: Arc::new(mock_services.power_state_client),
                    power_state_report_receiver,
                    update_engine: Box::new(mock_services.update_engine),
                    update_manager_listener_service_client_provider,
                    vold_service: Arc::new(mock_services.vold_service),
                },
            )
        }

        fn new_with_mocks(temp_paths: TempPaths) -> (Self, UpdateManagerInit) {
            Self::new(temp_paths, MockServices::new())
        }
    }

    fn make_service_bundle_update_payload(
        service_bundle_update_payload_path: &Path,
        signed_apex_type: SignedApexType,
        boot_attempts: i32,
    ) -> ServiceBundleUpdatePayload {
        let mut apex_paths = Vec::<String>::new();

        let test_apex_path = match signed_apex_type {
            SignedApexType::ValidSignedApex => Path::new("test_data/test.apex"),
            SignedApexType::BadSignedApex => Path::new("test_data/bad-signed-test.apk"),
        };

        let dest_path_buf = service_bundle_update_payload_path.join("test.apex");
        std::fs::copy(test_apex_path, &dest_path_buf).unwrap();
        apex_paths.push(dest_path_buf.to_str().unwrap().to_string());

        ServiceBundleUpdatePayload::new(apex_paths, boot_attempts).unwrap()
    }

    fn make_fake_apex_service_apex_info() -> Vec<ApexServiceApexInfo> {
        vec![
            ApexServiceApexInfo {
                moduleName: "my.app.apex".to_string(),
                versionCode: 100,
                ..Default::default()
            },
            ApexServiceApexInfo {
                moduleName: "my.lib.apex".to_string(),
                versionCode: 200,
                ..Default::default()
            },
        ]
    }

    fn make_fake_apex_info_list() -> ApexInfoList {
        ApexInfoList { apexInfos: make_fake_apex_service_apex_info() }
    }

    fn make_fake_update_headers() -> HashMap<String, String> {
        let mut header_key_value_pairs = HashMap::<String, String>::new();

        header_key_value_pairs.insert("key".to_string(), "value".to_string());
        header_key_value_pairs.insert("a".to_string(), "b".to_string());

        header_key_value_pairs
    }

    fn make_system_update_payload() -> SystemUpdatePayloadProto {
        let header_key_value_pairs = make_fake_update_headers();
        SystemUpdatePayloadProto {
            payload: std::path::absolute("test_data/ota_payload.zip")
                .unwrap()
                .into_os_string()
                .into_string()
                .unwrap(),
            payload_offset: Some(42),
            payload_size: Some(1337),
            header_key_value_pairs,
            ..Default::default()
        }
    }

    fn get_expected_system_update_payload_for_test_data(payload: &str) -> SystemUpdatePayload {
        SystemUpdatePayload::new(
            payload,
            4897,
            100,
            &make_update_engine_headers_from_requested_headers(
                HashMap::from_iter([
                    ("FILE_SIZE".to_string(), "243495040".to_string()),
                    (
                        "METADATA_HASH".to_string(),
                        "CpaqhRpsAYUbnDdixJaTm+niJ20re1+iFp/yEHsztBM=".to_string(),
                    ),
                    (
                        "FILE_HASH".to_string(),
                        "MS4DZvZ2wGQu6Jg9I8nL9RqnsOn/uO6hcUw4cv5PwZw=".to_string(),
                    ),
                    ("METADATA_SIZE".to_string(), "27907".to_string()),
                ]),
                false,
            ),
        )
        .unwrap()
    }

    fn make_update_engine_headers_from_requested_headers(
        requested_headers: HashMap<String, String>,
        switch_slot: bool,
    ) -> HashMap<String, String> {
        let switch_slot_on_reboot_value = match switch_slot {
            true => "1",
            false => "0",
        };

        // The Update Manager adds the "RUN_POST_INSTALL" and "SWITCH_SLOT_ON_REBOOT" keys
        // to the headers
        HashMap::from_iter(requested_headers.into_iter().chain([
            ("RUN_POST_INSTALL".to_string(), "1".to_string()),
            ("SWITCH_SLOT_ON_REBOOT".to_string(), switch_slot_on_reboot_value.to_string()),
        ]))
    }

    fn make_uninstall_apex_request(module_name: String, version_code: i64) -> UninstallApexRequest {
        UninstallApexRequest {
            apexes: vec![ApexInfo { module_name, version_code, ..Default::default() }],
            ..Default::default()
        }
    }

    fn fake_active_apex(
        mock_services: &mut MockServices,
        module_name: String,
        version_code: i64,
        module_path: String,
    ) {
        mock_services.apex_service.expect_get_active_packages().times(1).returning(move || {
            Ok(vec![ApexServiceApexInfo {
                moduleName: module_name.clone(),
                versionCode: version_code,
                modulePath: module_path.clone(),
                ..Default::default()
            }])
        });
    }

    async fn wait_for_successful_callback(
        callback_receiver: &mut UnboundedReceiver<OnStatusUpdateRequestMessage>,
        timeout: Duration,
    ) -> StatusUpdate {
        let message =
            tokio::time::timeout(timeout, callback_receiver.recv()).await.unwrap().unwrap();
        assert_eq!(&message.package, PACKAGE);
        assert_eq!(&message.bundle, BUNDLE);

        message.on_status_update_request.status_update.unwrap()
    }

    async fn wait_for_callback_with_state(
        callback_receiver: &mut UnboundedReceiver<OnStatusUpdateRequestMessage>,
        timeout: Duration,
        update_state: ApiUpdateState,
    ) {
        loop {
            let message =
                tokio::time::timeout(timeout, callback_receiver.recv()).await.unwrap().unwrap();
            assert_eq!(&message.package, PACKAGE);
            assert_eq!(&message.bundle, BUNDLE);

            let status_update = message.on_status_update_request.status_update.unwrap();

            if let StatusUpdate::UpdateManagerStatus(update_manager_status) = status_update {
                if matches!(
                    update_manager_status.update_state.enum_value(),
                    Ok(received_update_state) if received_update_state == update_state
                ) {
                    break;
                }
            }
        }
    }

    async fn subscribe_and_wait_for_state(
        ctx: &mut TestContext,
        service_impl: &UpdateManagerServiceImpl,
        update_state: ApiUpdateState,
    ) {
        let result = UpdateManagerService::SubscribeToStatusUpdates(
            service_impl,
            make_service_fqin(),
            &SubscribeToStatusUpdatesRequest::default(),
        )
        .await;

        assert!(result.is_ok());

        wait_for_callback_with_state(
            &mut ctx.callback_receiver,
            STATE_CHANGE_TIMEOUT,
            update_state,
        )
        .await;
    }

    async fn prepare_interrupted_update(
        temp_dirs: &TempDirs,
    ) -> (TestContext, SystemUpdatePayloadProto, UpdateManagerServiceImpl) {
        let mut mock_services = MockServices::new();
        mock_services.update_engine.set_complete_update(false);
        mock_services
            .boot_control_service
            .expect_get_current_slot()
            .times(1)
            .returning(|| Ok(/* initial_boot_slot=*/ 1));
        mock_services
            .power_state_client
            .expect_power_state_report_processing_complete()
            .times(1)
            .returning(|| Ok(()));

        let (mut ctx, init) = TestContext::new(temp_dirs.as_temp_paths(), mock_services);

        let service_impl = UpdateManagerServiceImpl::new_with_paths(
            init,
            temp_dirs.ota_package.path(),
            &ctx.save_state_path,
        )
        .await
        .unwrap();
        let requested_system_update_payload = make_system_update_payload();

        subscribe_and_wait_for_state(&mut ctx, &service_impl, ApiUpdateState::READY).await;

        UpdateManagerService::Prepare(
            &service_impl,
            make_service_fqin(),
            &PrepareRequest {
                payload: Some(PrepareRequestPayload::SystemUpdatePayload(
                    requested_system_update_payload.clone(),
                )),
                ..Default::default()
            },
        )
        .await
        .expect("Failure when calling UpdateManagerService::Prepare");

        // Wait for the prepare step to get started (but not finish)
        wait_for_successful_callback(&mut ctx.callback_receiver, STATE_CHANGE_TIMEOUT).await;
        wait_for_successful_callback(&mut ctx.callback_receiver, STATE_CHANGE_TIMEOUT).await;

        (ctx, requested_system_update_payload, service_impl)
    }

    struct FakeUpdateEngineTestDetails {
        system_update_payload: Option<SystemUpdatePayload>,
        cancel_called: bool,
        reset_status_called: bool,
        resume_called: bool,
        suspend_called: bool,
    }

    impl FakeUpdateEngineTestDetails {
        fn new() -> Self {
            Self {
                system_update_payload: None,
                cancel_called: false,
                reset_status_called: false,
                resume_called: false,
                suspend_called: false,
            }
        }
    }

    struct FakeUpdateEngine {
        callbacks: Mutex<Option<UpdateEngineCallbacks>>,
        complete_update: bool,
        fake_update_engine_test_details: Arc<Mutex<FakeUpdateEngineTestDetails>>,
    }

    impl FakeUpdateEngine {
        fn new(fake_update_engine_test_details: Arc<Mutex<FakeUpdateEngineTestDetails>>) -> Self {
            FakeUpdateEngine {
                callbacks: Mutex::new(None),
                complete_update: true,
                fake_update_engine_test_details,
            }
        }

        fn set_complete_update(&mut self, complete_update: bool) {
            self.complete_update = complete_update;
        }

        fn get_fake_update_engine_test_details(&self) -> Arc<Mutex<FakeUpdateEngineTestDetails>> {
            self.fake_update_engine_test_details.clone()
        }
    }

    #[async_trait]
    impl UpdateEngine for FakeUpdateEngine {
        async fn bind_callbacks(&self, new_callbacks: UpdateEngineCallbacks) -> SdvResult<()> {
            let mut callbacks = self.callbacks.lock().unwrap();

            match *callbacks {
                Some(_) => Err(SdvStatus::with_message(
                    SdvStatusCode::AlreadyExists,
                    "update_engine callback already bound",
                )),
                None => {
                    *callbacks = Some(new_callbacks);

                    callbacks.as_ref().unwrap().on_status_update(0, STATUS_UPDATE_PERCENTAGE_INIT);

                    Ok(())
                }
            }
        }

        async fn apply_payload_fd(
            &self,
            payload: &str,
            payload_offset: i64,
            payload_size: i64,
            header_key_value_pairs: &[String],
        ) -> SdvResult<()> {
            let switch_slot_on_reboot = {
                let mut fake_update_engine_test_details =
                    self.fake_update_engine_test_details.lock().unwrap();
                let header_key_value_pairs = string_pairs_as_header_map(header_key_value_pairs);
                let switch_slot_on_reboot =
                    header_key_value_pairs.get("SWITCH_SLOT_ON_REBOOT").unwrap() == "1";

                fake_update_engine_test_details.system_update_payload = Some(SystemUpdatePayload {
                    payload: payload.to_string(),
                    payload_offset,
                    payload_size,
                    header_key_value_pairs,
                });

                switch_slot_on_reboot
            };

            let callbacks = self.callbacks.lock().unwrap();

            let error_code = match switch_slot_on_reboot {
                true => SUCCESS,
                false => UPDATED_BUT_NOT_ACTIVE,
            };

            // In reality, update_engine sends many status updates as the process
            // progresses. This fake version will just send one to ensure it is delivered.
            let status = if self.complete_update { IDLE } else { DOWNLOADING };
            callbacks.as_ref().unwrap().on_status_update(status, STATUS_UPDATE_PERCENTAGE_ACTIVE);

            if self.complete_update {
                callbacks.as_ref().unwrap().on_payload_application_complete(error_code);
            }

            Ok(())
        }

        async fn cancel(&self) -> SdvResult<()> {
            {
                let mut fake_update_engine_test_details =
                    self.fake_update_engine_test_details.lock().unwrap();
                fake_update_engine_test_details.cancel_called = true;
            }

            let callbacks = self.callbacks.lock().unwrap();
            callbacks.as_ref().unwrap().on_payload_application_complete(USER_CANCELED);
            Ok(())
        }

        async fn reset_status(&self) -> SdvResult<()> {
            let mut fake_update_engine_test_details =
                self.fake_update_engine_test_details.lock().unwrap();
            fake_update_engine_test_details.reset_status_called = true;
            Ok(())
        }

        async fn suspend(&self) -> SdvResult<()> {
            let mut fake_update_engine_test_details =
                self.fake_update_engine_test_details.lock().unwrap();
            fake_update_engine_test_details.suspend_called = true;
            Ok(())
        }

        async fn resume(&self) -> SdvResult<()> {
            let mut fake_update_engine_test_details =
                self.fake_update_engine_test_details.lock().unwrap();
            fake_update_engine_test_details.resume_called = true;
            Ok(())
        }
    }

    #[derive(Debug)]
    struct OnStatusUpdateRequestMessage {
        package: String,
        bundle: String,
        on_status_update_request: OnStatusUpdateRequest,
    }

    struct FakeUpdateManagerListenerServiceClient {
        package: String,
        bundle: String,
        sender: UnboundedSender<OnStatusUpdateRequestMessage>,
    }

    impl FakeUpdateManagerListenerServiceClient {
        fn new(
            package: String,
            bundle: String,
            sender: UnboundedSender<OnStatusUpdateRequestMessage>,
        ) -> Self {
            FakeUpdateManagerListenerServiceClient { package, bundle, sender }
        }
    }

    #[async_trait]
    impl UpdateManagerListenerService for FakeUpdateManagerListenerServiceClient {
        async fn OnStatusUpdate(
            &self,
            on_status_update_request: &OnStatusUpdateRequest,
        ) -> SdvResult<OnStatusUpdateResponse> {
            self.sender
                .send(OnStatusUpdateRequestMessage {
                    package: self.package.clone(),
                    bundle: self.bundle.clone(),
                    on_status_update_request: on_status_update_request.clone(),
                })
                .unwrap();

            Ok(OnStatusUpdateResponse::default())
        }
    }

    struct FakeUpdateManagerListenerServiceClientProvider {
        sender: UnboundedSender<OnStatusUpdateRequestMessage>,
    }

    impl FakeUpdateManagerListenerServiceClientProvider {
        // The receiver can not be sent across threads, so return it from the constructor
        // so tests can wait for callbacks to be received.
        fn new() -> (Self, UnboundedReceiver<OnStatusUpdateRequestMessage>) {
            let (sender, receiver) = unbounded_channel();
            (FakeUpdateManagerListenerServiceClientProvider { sender }, receiver)
        }
    }

    #[async_trait]
    impl UpdateManagerListenerServiceClientProvider for FakeUpdateManagerListenerServiceClientProvider {
        async fn get_sdv_update_manager_update_manager_listener_service_info(
            &self,
            package: &str,
            bundle: &str,
        ) -> SdvResult<UpdateManagerListenerServiceInfo> {
            let fake_update_manager_listener_service_client =
                Box::new(FakeUpdateManagerListenerServiceClient::new(
                    package.to_string(),
                    bundle.to_string(),
                    self.sender.clone(),
                ));

            let registration_event_stream = Box::new(futures::stream::iter(vec![]))
                as Box<dyn futures::Stream<Item = sdv_mw_rs_comms::Availability> + Send>;

            Ok(UpdateManagerListenerServiceInfo {
                client: fake_update_manager_listener_service_client
                    as Box<dyn UpdateManagerListenerService>,
                registration_event_stream: Box::into_pin(registration_event_stream),
            })
        }
    }

    async fn assert_prepare_success(ctx: &mut TestContext) {
        // Wait for the prepare step to finish
        let update_state_start =
            wait_for_successful_callback(&mut ctx.callback_receiver, STATE_CHANGE_TIMEOUT).await;
        let update_progress =
            wait_for_successful_callback(&mut ctx.callback_receiver, STATE_CHANGE_TIMEOUT).await;
        let update_state_end =
            wait_for_successful_callback(&mut ctx.callback_receiver, STATE_CHANGE_TIMEOUT).await;

        if let StatusUpdate::UpdateManagerStatus(update_manager_status) = update_state_start {
            assert_eq!(
                update_manager_status.update_state.enum_value().unwrap(),
                ApiUpdateState::PREPARE
            );
        } else {
            panic!("Unexpected StatusUpdate for PREPARE_START: {:?}", update_state_start);
        }

        if let StatusUpdate::UpdateProgress(update_progress) = update_progress {
            assert_eq!(update_progress.status_code, IDLE);
            assert_eq!(update_progress.percentage, STATUS_UPDATE_PERCENTAGE_ACTIVE);
        } else {
            panic!("Unexpected StatusUpdate for PREPARE_PROGRESS: {update_progress:?}");
        }

        if let StatusUpdate::UpdateManagerStatus(update_manager_status) = update_state_end {
            assert_eq!(
                update_manager_status.update_state.enum_value().unwrap(),
                ApiUpdateState::PREPARE_COMPLETE
            );
        } else {
            panic!("Unexpected StatusUpdate for PREPARE_COMPLETE: {:?}", update_state_end);
        }
    }

    #[test]
    fn test_get_unused_session_id_empty_app_staging_directory_returns_1() {
        let app_staging_dir = tempdir().unwrap();

        let result = get_unused_session_id(app_staging_dir.path());

        assert!(result.is_ok(), "Failed to get unused session_id: {:?}", result.err().unwrap());
        assert_eq!(result.unwrap(), 1);
    }

    #[test]
    fn test_get_unused_session_id_session_1_exists_returns_2() {
        let app_staging_dir = tempdir().unwrap();
        std::fs::create_dir(
            app_staging_dir.path().join(format!("{APEX_SERVICE_SESSION_DIR_PREFIX}1")),
        )
        .unwrap();

        let result = get_unused_session_id(app_staging_dir.path());

        assert!(result.is_ok(), "Failed to get unused session_id: {:?}", result.err().unwrap());
        assert_eq!(result.unwrap(), 2);
    }

    #[test]
    fn test_get_unused_session_id_session_2_and_session_300_exists_returns_301() {
        let app_staging_dir = tempdir().unwrap();
        std::fs::create_dir(
            app_staging_dir.path().join(format!("{APEX_SERVICE_SESSION_DIR_PREFIX}2")),
        )
        .unwrap();
        std::fs::create_dir(
            app_staging_dir.path().join(format!("{APEX_SERVICE_SESSION_DIR_PREFIX}300")),
        )
        .unwrap();

        let result = get_unused_session_id(app_staging_dir.path());

        assert!(result.is_ok(), "Failed to get unused session_id: {:?}", result.err().unwrap());
        assert_eq!(result.unwrap(), 301);
    }

    #[tokio::test]
    async fn test_update_manager_service_impl_new_without_app_staging_dir_fails() {
        let temp_paths = TempPaths {
            app_staging: Path::new("/invalid/app_staging").to_path_buf(),
            ota_package: Path::new("/invalid/ota_package").to_path_buf(),
            save_state: Path::new("/invalid/save_state").to_path_buf(),
        };
        let ota_package_path = temp_paths.ota_package.clone();
        let (ctx, init) = TestContext::new_with_mocks(temp_paths);
        let service_impl_result =
            UpdateManagerServiceImpl::new_with_paths(init, &ota_package_path, &ctx.save_state_path)
                .await;
        assert!(service_impl_result.is_err());
    }

    #[tokio::test]
    async fn test_get_status_succeeds() {
        let temp_dirs = TempDirs::new();
        let (ctx, init) = TestContext::new_with_mocks(temp_dirs.as_temp_paths());
        let service_impl = UpdateManagerServiceImpl::new_with_paths(
            init,
            temp_dirs.ota_package.path(),
            &ctx.save_state_path,
        )
        .await
        .unwrap();

        let result = UpdateManagerService::GetStatus(
            &service_impl,
            make_service_fqin(),
            &GetStatusRequest::default(),
        )
        .await;

        assert!(result.is_ok());
        let response = result.unwrap();
        assert_eq!(response.update_manager_status.update_state.unwrap(), ApiUpdateState::READY);
    }

    #[tokio::test]
    async fn test_subscribe_to_status_updates_succeeds() {
        let temp_dirs = TempDirs::new();
        let (mut ctx, init) = TestContext::new_with_mocks(temp_dirs.as_temp_paths());
        let service_impl = UpdateManagerServiceImpl::new_with_paths(
            init,
            temp_dirs.ota_package.path(),
            &ctx.save_state_path,
        )
        .await
        .unwrap();

        subscribe_and_wait_for_state(&mut ctx, &service_impl, ApiUpdateState::READY).await;
    }

    #[tokio::test]
    async fn test_unsubscribe_from_status_updates_succeeds() {
        let temp_dirs = TempDirs::new();
        let (mut ctx, init) = TestContext::new_with_mocks(temp_dirs.as_temp_paths());
        let service_impl = UpdateManagerServiceImpl::new_with_paths(
            init,
            temp_dirs.ota_package.path(),
            &ctx.save_state_path,
        )
        .await
        .unwrap();

        subscribe_and_wait_for_state(&mut ctx, &service_impl, ApiUpdateState::READY).await;

        let result = UpdateManagerService::UnsubscribeFromStatusUpdates(
            &service_impl,
            make_service_fqin(),
            &UnsubscribeFromStatusUpdatesRequest::default(),
        )
        .await;

        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_system_update_prepare_with_relative_payload_path_fails() {
        // GIVEN that a system update with a relative payload path is present on the system
        let temp_dirs = TempDirs::new();
        let (mut ctx, init) = TestContext::new_with_mocks(temp_dirs.as_temp_paths());
        let service_impl = UpdateManagerServiceImpl::new_with_paths(
            init,
            temp_dirs.ota_package.path(),
            &ctx.save_state_path,
        )
        .await
        .unwrap();

        let mut system_update_payload = make_system_update_payload();
        system_update_payload.payload = "relative/path/to/ota_update.zip".to_string();
        // WHEN a prepare request is sent
        let result = UpdateManagerService::Prepare(
            &service_impl,
            make_service_fqin(),
            &PrepareRequest {
                payload: Some(PrepareRequestPayload::SystemUpdatePayload(system_update_payload)),
                ..Default::default()
            },
        )
        .await;

        // THEN the prepare request should fail
        assert!(result.is_err());

        let e = result.unwrap_err();
        assert_eq!(e.status_code(), SdvStatusCode::InvalidArgument);
        assert!(e.to_string().contains("The payload path must be an absolute path"));

        // Callbacks should only be sent if the service changes states. No state changes should
        // occur as the error is returned in the request response.
        assert!(ctx.callback_receiver.try_recv().is_err());
    }

    #[tokio::test]
    async fn test_system_update_prepare_with_negative_payload_offset_fails() {
        // GIVEN that a system update with a relative payload path is present on the system
        let temp_dirs = TempDirs::new();
        let mut mock_services = MockServices::new();
        mock_services
            .boot_control_service
            .expect_get_current_slot()
            .times(1)
            .returning(|| Ok(/* initial_boot_slot=*/ 1));

        let (mut ctx, init) = TestContext::new(temp_dirs.as_temp_paths(), mock_services);
        let service_impl = UpdateManagerServiceImpl::new_with_paths(
            init,
            temp_dirs.ota_package.path(),
            &ctx.save_state_path,
        )
        .await
        .unwrap();

        let mut system_update_payload = make_system_update_payload();
        system_update_payload.payload_offset = Some(-42);

        // WHEN a prepare request is sent
        let result = UpdateManagerService::Prepare(
            &service_impl,
            make_service_fqin(),
            &PrepareRequest {
                payload: Some(PrepareRequestPayload::SystemUpdatePayload(system_update_payload)),
                ..Default::default()
            },
        )
        .await;

        // THEN the prepare request should fail
        assert!(result.is_err());

        let e = result.unwrap_err();
        assert_eq!(e.status_code(), SdvStatusCode::InvalidArgument);
        assert!(e.to_string().contains("The payload_offset must be non-negative"));

        // Callbacks should only be sent if the service changes states. No state changes should
        // occur as the error is returned in the request response.
        assert!(ctx.callback_receiver.try_recv().is_err());
    }

    #[tokio::test]
    async fn test_system_update_prepare_with_negative_payload_size_fails() {
        // GIVEN that a system update with a relative payload path is present on the system
        let temp_dirs = TempDirs::new();
        let mut mock_services = MockServices::new();
        mock_services
            .boot_control_service
            .expect_get_current_slot()
            .times(1)
            .returning(|| Ok(/* initial_boot_slot=*/ 1));

        let (mut ctx, init) = TestContext::new(temp_dirs.as_temp_paths(), mock_services);
        let service_impl = UpdateManagerServiceImpl::new_with_paths(
            init,
            temp_dirs.ota_package.path(),
            &ctx.save_state_path,
        )
        .await
        .unwrap();

        let mut system_update_payload = make_system_update_payload();
        system_update_payload.payload_size = Some(-1337);

        // WHEN a prepare request is sent
        let result = UpdateManagerService::Prepare(
            &service_impl,
            make_service_fqin(),
            &PrepareRequest {
                payload: Some(PrepareRequestPayload::SystemUpdatePayload(system_update_payload)),
                ..Default::default()
            },
        )
        .await;

        // THEN the prepare request should fail
        assert!(result.is_err());
        let e = result.unwrap_err();
        assert_eq!(e.status_code(), SdvStatusCode::InvalidArgument);
        assert!(e.to_string().contains("The payload_size must be non-negative"));

        // Callbacks should only be sent if the service changes states. No state changes should
        // occur as the error is returned in the request response.
        assert!(ctx.callback_receiver.try_recv().is_err());
    }

    #[tokio::test]
    async fn test_system_update_rollback_during_prepare_cancels_and_succeeds() {
        // GIVEN that a valid system update is being prepared
        let temp_dirs = TempDirs::new();
        let mut mock_services = MockServices::new();
        mock_services.update_engine.set_complete_update(false);
        mock_services
            .boot_control_service
            .expect_get_current_slot()
            .times(1)
            .returning(|| Ok(/* initial_boot_slot=*/ 1));

        let (mut ctx, init) = TestContext::new(temp_dirs.as_temp_paths(), mock_services);
        let service_impl = UpdateManagerServiceImpl::new_with_paths(
            init,
            temp_dirs.ota_package.path(),
            &ctx.save_state_path,
        )
        .await
        .unwrap();

        let requested_system_update_payload = make_system_update_payload();

        subscribe_and_wait_for_state(&mut ctx, &service_impl, ApiUpdateState::READY).await;

        UpdateManagerService::Prepare(
            &service_impl,
            make_service_fqin(),
            &PrepareRequest {
                payload: Some(PrepareRequestPayload::SystemUpdatePayload(
                    requested_system_update_payload.clone(),
                )),
                ..Default::default()
            },
        )
        .await
        .expect("Failure when calling UpdateManagerService::Prepare");

        // Wait for the prepare step to get started (but not finish)
        wait_for_successful_callback(&mut ctx.callback_receiver, STATE_CHANGE_TIMEOUT).await;
        wait_for_successful_callback(&mut ctx.callback_receiver, STATE_CHANGE_TIMEOUT).await;

        // WHEN a Rollback() request is sent
        let rollback_result = UpdateManagerService::Rollback(
            &service_impl,
            make_service_fqin(),
            &RollbackRequest::default(),
        )
        .await;

        let update_state_cancel =
            wait_for_successful_callback(&mut ctx.callback_receiver, STATE_CHANGE_TIMEOUT).await;
        let update_state_rollback =
            wait_for_successful_callback(&mut ctx.callback_receiver, STATE_CHANGE_TIMEOUT).await;
        let update_state_ready =
            wait_for_successful_callback(&mut ctx.callback_receiver, STATE_CHANGE_TIMEOUT).await;

        // THEN the Rollback() request should succeed and the update should be cancelled
        assert!(rollback_result.is_ok());

        if let StatusUpdate::UpdateManagerStatus(update_manager_status) = update_state_cancel {
            assert_eq!(
                update_manager_status.update_state.enum_value().unwrap(),
                ApiUpdateState::PREPARE_CANCEL
            );
        } else {
            panic!("Unexpected StatusUpdate: {update_state_cancel:?}");
        }

        if let StatusUpdate::UpdateManagerStatus(update_manager_status) = update_state_rollback {
            assert_eq!(
                update_manager_status.update_state.enum_value().unwrap(),
                ApiUpdateState::PREPARE_ROLLBACK
            );
        } else {
            panic!("Unexpected StatusUpdate: {update_state_rollback:?}");
        }

        if let StatusUpdate::UpdateManagerStatus(update_manager_status) = update_state_ready {
            assert_eq!(
                update_manager_status.update_state.enum_value().unwrap(),
                ApiUpdateState::READY
            );
        } else {
            panic!("Unexpected StatusUpdate: {update_state_ready:?}");
        }

        // Callbacks should only be sent if the service changes states. After the above state
        // changes, no more state changes should occur. Verify this by ensuring no pending
        // callbacks exist.
        assert!(ctx.callback_receiver.try_recv().is_err());

        let expected_header_key_value_pairs = make_update_engine_headers_from_requested_headers(
            requested_system_update_payload.header_key_value_pairs.clone(),
            false,
        );

        let expected_system_update_payload = SystemUpdatePayload {
            payload: requested_system_update_payload.payload.clone(),
            payload_offset: requested_system_update_payload.payload_offset.unwrap(),
            payload_size: requested_system_update_payload.payload_size.unwrap(),
            header_key_value_pairs: expected_header_key_value_pairs,
        };

        // Place the following in scope block so we don't hold the mutex for the rest of the test
        {
            let fake_update_engine_test_details =
                ctx.fake_update_engine_test_details.lock().unwrap();
            let system_update_payload =
                fake_update_engine_test_details.system_update_payload.as_ref().unwrap();
            assert_eq!(*system_update_payload, expected_system_update_payload);

            assert!(fake_update_engine_test_details.cancel_called);
            assert!(fake_update_engine_test_details.reset_status_called);
        }
    }

    #[tokio::test]
    async fn test_system_update_api_suspend_resume_during_prepare_succeeds() {
        // GIVEN that a valid system update is being prepared
        let temp_dirs = TempDirs::new();
        let (mut ctx, requested_system_update_payload, service_impl) =
            prepare_interrupted_update(&temp_dirs).await;

        // WHEN a suspend request is sent followed by a resume request
        let suspend_result = UpdateManagerService::Suspend(
            &service_impl,
            make_service_fqin(),
            &SuspendRequest::default(),
        )
        .await;

        let update_state_suspend =
            wait_for_successful_callback(&mut ctx.callback_receiver, STATE_CHANGE_TIMEOUT).await;

        let update_state_suspend_complete =
            wait_for_successful_callback(&mut ctx.callback_receiver, STATE_CHANGE_TIMEOUT).await;

        let resume_result = UpdateManagerService::Resume(
            &service_impl,
            make_service_fqin(),
            &ResumeRequest::default(),
        )
        .await;

        let update_state_resume =
            wait_for_successful_callback(&mut ctx.callback_receiver, STATE_CHANGE_TIMEOUT).await;

        // THEN the update should be suspended then resumed
        assert!(suspend_result.is_ok());
        assert!(resume_result.is_ok());

        if let StatusUpdate::UpdateManagerStatus(update_manager_status) = update_state_suspend {
            assert_eq!(
                update_manager_status.update_state.enum_value().unwrap(),
                ApiUpdateState::PREPARE_SUSPEND
            );
        } else {
            panic!("Unexpected StatusUpdate: {update_state_suspend:?}");
        }

        if let StatusUpdate::UpdateManagerStatus(update_manager_status) =
            update_state_suspend_complete
        {
            assert_eq!(
                update_manager_status.update_state.enum_value().unwrap(),
                ApiUpdateState::PREPARE_SUSPEND_COMPLETE
            );
        } else {
            panic!("Unexpected StatusUpdate: {update_state_suspend_complete:?}");
        }

        if let StatusUpdate::UpdateManagerStatus(update_manager_status) = update_state_resume {
            assert_eq!(
                update_manager_status.update_state.enum_value().unwrap(),
                ApiUpdateState::PREPARE
            );
        } else {
            panic!("Unexpected StatusUpdate: {update_state_resume:?}");
        }

        // Callbacks should only be sent if the service changes states. After the above state
        // changes, no more state changes should occur. Verify this by ensuring no pending
        // callbacks exist.
        assert!(ctx.callback_receiver.try_recv().is_err());

        let expected_header_key_value_pairs = make_update_engine_headers_from_requested_headers(
            requested_system_update_payload.header_key_value_pairs.clone(),
            false,
        );
        let expected_system_update_payload = SystemUpdatePayload {
            payload: requested_system_update_payload.payload.clone(),
            payload_offset: requested_system_update_payload.payload_offset.unwrap(),
            payload_size: requested_system_update_payload.payload_size.unwrap(),
            header_key_value_pairs: expected_header_key_value_pairs,
        };

        // Place the following in scope block so we don't hold the mutex for the rest of the test
        {
            let fake_update_engine_test_details =
                ctx.fake_update_engine_test_details.lock().unwrap();
            let system_update_payload =
                fake_update_engine_test_details.system_update_payload.as_ref().unwrap();
            assert_eq!(*system_update_payload, expected_system_update_payload);

            assert!(fake_update_engine_test_details.suspend_called);
            assert!(fake_update_engine_test_details.resume_called);
        }
    }

    #[tokio::test]
    async fn test_system_update_vpm_suspend_resume_during_prepare_succeeds() {
        // GIVEN that a valid system update is being prepared
        let temp_dirs = TempDirs::new();
        let (mut ctx, requested_system_update_payload, _service_impl) =
            prepare_interrupted_update(&temp_dirs).await;

        // WHEN VPM indicates the VPM is suspending and then resuming
        ctx.power_state_report_sender.send(PowerStateReport::SUSPEND_TO_RAM_POST_FINISH).unwrap();

        let update_state_suspend =
            wait_for_successful_callback(&mut ctx.callback_receiver, STATE_CHANGE_TIMEOUT).await;

        let update_state_suspend_complete =
            wait_for_successful_callback(&mut ctx.callback_receiver, STATE_CHANGE_TIMEOUT).await;

        ctx.power_state_report_sender.send(PowerStateReport::SUSPEND_TO_RAM_EXIT).unwrap();

        // THEN the update should be suspended but not resumed
        if let StatusUpdate::UpdateManagerStatus(update_manager_status) = update_state_suspend {
            assert_eq!(
                update_manager_status.update_state.enum_value().unwrap(),
                ApiUpdateState::PREPARE_SUSPEND
            );
        } else {
            panic!("Unexpected StatusUpdate: {update_state_suspend:?}");
        }

        if let StatusUpdate::UpdateManagerStatus(update_manager_status) =
            update_state_suspend_complete
        {
            assert_eq!(
                update_manager_status.update_state.enum_value().unwrap(),
                ApiUpdateState::PREPARE_SUSPEND_COMPLETE
            );
        } else {
            panic!("Unexpected StatusUpdate: {update_state_suspend_complete:?}");
        }

        // Callbacks should only be sent if the service changes states. After the above state
        // changes, no more state changes should occur. Verify this by ensuring no pending
        // callbacks exist.
        assert!(ctx.callback_receiver.try_recv().is_err());

        let expected_header_key_value_pairs = make_update_engine_headers_from_requested_headers(
            requested_system_update_payload.header_key_value_pairs.clone(),
            false,
        );
        let expected_system_update_payload = SystemUpdatePayload {
            payload: requested_system_update_payload.payload.clone(),
            payload_offset: requested_system_update_payload.payload_offset.unwrap(),
            payload_size: requested_system_update_payload.payload_size.unwrap(),
            header_key_value_pairs: expected_header_key_value_pairs,
        };

        // Place the following in scope block so we don't hold the mutex for the rest of the test
        {
            let fake_update_engine_test_details =
                ctx.fake_update_engine_test_details.lock().unwrap();
            let system_update_payload =
                fake_update_engine_test_details.system_update_payload.as_ref().unwrap();
            assert_eq!(*system_update_payload, expected_system_update_payload);

            assert!(fake_update_engine_test_details.suspend_called);
            assert!(!fake_update_engine_test_details.resume_called);
        }
    }

    #[tokio::test]
    async fn test_system_update_prepare_succeeds() {
        // GIVEN that a valid system update is present on the system
        let temp_dirs = TempDirs::new();
        let mut mock_services = MockServices::new();
        mock_services
            .boot_control_service
            .expect_get_current_slot()
            .times(1)
            .returning(|| Ok(/* initial_boot_slot=*/ 1));
        mock_services
            .power_state_client
            .expect_power_state_report_processing_complete()
            .times(1)
            .returning(|| Ok(()));

        let (mut ctx, init) = TestContext::new(temp_dirs.as_temp_paths(), mock_services);
        let service_impl = UpdateManagerServiceImpl::new_with_paths(
            init,
            temp_dirs.ota_package.path(),
            &ctx.save_state_path,
        )
        .await
        .unwrap();

        let requested_system_update_payload = make_system_update_payload();

        subscribe_and_wait_for_state(&mut ctx, &service_impl, ApiUpdateState::READY).await;

        // WHEN a prepare request is sent
        let result = UpdateManagerService::Prepare(
            &service_impl,
            make_service_fqin(),
            &PrepareRequest {
                payload: Some(PrepareRequestPayload::SystemUpdatePayload(
                    requested_system_update_payload.clone(),
                )),
                ..Default::default()
            },
        )
        .await;

        // Wait for the prepare step to finish
        let update_state_start =
            wait_for_successful_callback(&mut ctx.callback_receiver, STATE_CHANGE_TIMEOUT).await;
        let update_progress =
            wait_for_successful_callback(&mut ctx.callback_receiver, STATE_CHANGE_TIMEOUT).await;
        let update_state_end =
            wait_for_successful_callback(&mut ctx.callback_receiver, STATE_CHANGE_TIMEOUT).await;

        // THEN the prepare request should succeed and the UpdateEngine system API
        // should be called with the correct payload details.
        assert!(result.is_ok());

        if let StatusUpdate::UpdateManagerStatus(update_manager_status) = update_state_start {
            assert_eq!(
                update_manager_status.update_state.enum_value().unwrap(),
                ApiUpdateState::PREPARE
            );
        } else {
            panic!("Unexpected StatusUpdate: {update_state_start:?}");
        }

        if let StatusUpdate::UpdateProgress(update_progress) = update_progress {
            assert_eq!(update_progress.status_code, IDLE);
            assert_eq!(update_progress.percentage, STATUS_UPDATE_PERCENTAGE_ACTIVE);
        } else {
            panic!("Unexpected StatusUpdate: {update_progress:?}");
        }

        if let StatusUpdate::UpdateManagerStatus(update_manager_status) = update_state_end {
            assert_eq!(
                update_manager_status.update_state.enum_value().unwrap(),
                ApiUpdateState::PREPARE_COMPLETE
            );
        } else {
            panic!("Unexpected StatusUpdate: {update_state_end:?}");
        }

        // Callbacks should only be sent if the service changes states. After the above state
        // changes, no more state changes should occur. Verify this by ensuring no pending
        // callbacks exist.
        assert!(ctx.callback_receiver.try_recv().is_err());

        let expected_header_key_value_pairs = make_update_engine_headers_from_requested_headers(
            requested_system_update_payload.header_key_value_pairs.clone(),
            false,
        );
        let expected_system_update_payload = SystemUpdatePayload {
            payload: requested_system_update_payload.payload.clone(),
            payload_offset: requested_system_update_payload.payload_offset.unwrap(),
            payload_size: requested_system_update_payload.payload_size.unwrap(),
            header_key_value_pairs: expected_header_key_value_pairs,
        };
        // Place the following in scope block so we don't hold the mutex for the rest of the test
        {
            let fake_update_engine_test_details =
                ctx.fake_update_engine_test_details.lock().unwrap();
            let system_update_payload =
                fake_update_engine_test_details.system_update_payload.as_ref().unwrap();
            assert_eq!(*system_update_payload, expected_system_update_payload);
        }
    }

    #[tokio::test]
    async fn test_system_update_prepare_with_missing_offset_and_size_succeeds() {
        // GIVEN that a valid system update is present on the system
        let temp_dirs = TempDirs::new();
        let mut mock_services = MockServices::new();
        mock_services
            .boot_control_service
            .expect_get_current_slot()
            .times(1)
            .returning(|| Ok(/* initial_boot_slot=*/ 1));
        mock_services
            .power_state_client
            .expect_power_state_report_processing_complete()
            .times(1)
            .returning(|| Ok(()));

        let (mut ctx, init) = TestContext::new(temp_dirs.as_temp_paths(), mock_services);
        let service_impl = UpdateManagerServiceImpl::new_with_paths(
            init,
            temp_dirs.ota_package.path(),
            &ctx.save_state_path,
        )
        .await
        .unwrap();
        let mut requested_system_update_payload = make_system_update_payload();
        requested_system_update_payload.payload_offset = None;
        requested_system_update_payload.payload_size = None;
        requested_system_update_payload.header_key_value_pairs.clear();

        subscribe_and_wait_for_state(&mut ctx, &service_impl, ApiUpdateState::READY).await;

        // WHEN a prepare request is sent
        let result = UpdateManagerService::Prepare(
            &service_impl,
            make_service_fqin(),
            &PrepareRequest {
                payload: Some(PrepareRequestPayload::SystemUpdatePayload(
                    requested_system_update_payload.clone(),
                )),
                ..Default::default()
            },
        )
        .await;

        // Wait for the prepare step to finish
        let update_state_start =
            wait_for_successful_callback(&mut ctx.callback_receiver, STATE_CHANGE_TIMEOUT).await;
        let update_progress =
            wait_for_successful_callback(&mut ctx.callback_receiver, STATE_CHANGE_TIMEOUT).await;
        let update_state_end =
            wait_for_successful_callback(&mut ctx.callback_receiver, STATE_CHANGE_TIMEOUT).await;

        // THEN the prepare request should succeed and the UpdateEngine system API
        // should be called with the correct payload details.
        assert!(result.is_ok());

        if let StatusUpdate::UpdateManagerStatus(update_manager_status) = update_state_start {
            assert_eq!(
                update_manager_status.update_state.enum_value().unwrap(),
                ApiUpdateState::PREPARE
            );
        } else {
            panic!("Unexpected StatusUpdate: {update_state_start:?}");
        }

        if let StatusUpdate::UpdateProgress(update_progress) = update_progress {
            assert_eq!(update_progress.status_code, 0);
            assert_eq!(update_progress.percentage, 1.0);
        } else {
            panic!("Unexpected StatusUpdate: {update_progress:?}");
        }

        if let StatusUpdate::UpdateManagerStatus(update_manager_status) = update_state_end {
            assert_eq!(
                update_manager_status.update_state.enum_value().unwrap(),
                ApiUpdateState::PREPARE_COMPLETE
            );
        } else {
            panic!("Unexpected StatusUpdate: {update_state_end:?}");
        }

        // Callbacks should only be sent if the service changes states. After the above state
        // changes, no more state changes should occur. Verify this by ensuring no pending
        // callbacks exist.
        assert!(ctx.callback_receiver.try_recv().is_err());

        let expected_system_update_payload = get_expected_system_update_payload_for_test_data(
            &requested_system_update_payload.payload,
        );

        // Place the following in scope block so we don't hold the mutex for the rest of the test
        {
            let fake_update_engine_test_details =
                ctx.fake_update_engine_test_details.lock().unwrap();
            let system_update_payload =
                fake_update_engine_test_details.system_update_payload.as_ref().unwrap();
            assert_eq!(*system_update_payload, expected_system_update_payload);
        }
    }

    #[tokio::test]
    async fn test_system_update_activate_succeeds() {
        // GIVEN that a valid system update has been prepared successfully
        let temp_dirs = TempDirs::new();
        let mut mock_services = MockServices::new();
        mock_services
            .boot_control_service
            .expect_get_current_slot()
            .times(1)
            .returning(|| Ok(/* initial_boot_slot=*/ 1));
        mock_services
            .power_state_client
            .expect_power_state_report_processing_complete()
            .times(1)
            .returning(|| Ok(()));

        let (mut ctx, init) = TestContext::new(temp_dirs.as_temp_paths(), mock_services);
        let service_impl = UpdateManagerServiceImpl::new_with_paths(
            init,
            temp_dirs.ota_package.path(),
            &ctx.save_state_path,
        )
        .await
        .unwrap();

        let requested_system_update_payload = make_system_update_payload();

        subscribe_and_wait_for_state(&mut ctx, &service_impl, ApiUpdateState::READY).await;

        UpdateManagerService::Prepare(
            &service_impl,
            make_service_fqin(),
            &PrepareRequest {
                payload: Some(PrepareRequestPayload::SystemUpdatePayload(
                    requested_system_update_payload.clone(),
                )),
                ..Default::default()
            },
        )
        .await
        .expect("Failure when calling UpdateManagerService::Prepare");

        assert_prepare_success(&mut ctx).await;

        // Clear the system update payload to ensure we're testing the arguments that were passed
        // to UpdateEngine during the activate request, not the prepare request.
        {
            let mut fake_update_engine_test_details =
                ctx.fake_update_engine_test_details.lock().unwrap();
            fake_update_engine_test_details.system_update_payload = None;
        }

        // WHEN an activate request is sent
        let result = UpdateManagerService::Activate(
            &service_impl,
            make_service_fqin(),
            &ActivateRequest::default(),
        )
        .await;

        // Wait for the activate pre-reboot step to finish
        let update_state_start =
            wait_for_successful_callback(&mut ctx.callback_receiver, STATE_CHANGE_TIMEOUT).await;
        let update_progress =
            wait_for_successful_callback(&mut ctx.callback_receiver, STATE_CHANGE_TIMEOUT).await;
        let update_state_end =
            wait_for_successful_callback(&mut ctx.callback_receiver, STATE_CHANGE_TIMEOUT).await;

        // THEN the activate request should succeed and the UpdateEngine system API
        // should be called with the correct payload details.
        assert!(result.is_ok());

        if let StatusUpdate::UpdateManagerStatus(update_manager_status) = update_state_start {
            assert_eq!(
                update_manager_status.update_state.enum_value().unwrap(),
                ApiUpdateState::ACTIVATE_PRE_REBOOT
            );
        } else {
            panic!("Unexpected StatusUpdate: {update_state_start:?}");
        }

        if let StatusUpdate::UpdateProgress(update_progress) = update_progress {
            assert_eq!(update_progress.status_code, IDLE);
            assert_eq!(update_progress.percentage, STATUS_UPDATE_PERCENTAGE_ACTIVE);
        } else {
            panic!("Unexpected StatusUpdate: {update_progress:?}");
        }

        if let StatusUpdate::UpdateManagerStatus(update_manager_status) = update_state_end {
            assert_eq!(
                update_manager_status.update_state.enum_value().unwrap(),
                ApiUpdateState::ACTIVATE_PRE_REBOOT_COMPLETE
            );
        } else {
            panic!("Unexpected StatusUpdate: {update_state_end:?}");
        }

        // Callbacks should only be sent if the service changes states. After the above state
        // changes, no more state changes should occur. Verify this by ensuring no pending
        // callbacks exist.
        assert!(ctx.callback_receiver.try_recv().is_err());

        let expected_header_key_value_pairs = make_update_engine_headers_from_requested_headers(
            requested_system_update_payload.header_key_value_pairs.clone(),
            true,
        );
        let expected_system_update_payload = SystemUpdatePayload {
            payload: requested_system_update_payload.payload.to_string(),
            payload_offset: requested_system_update_payload.payload_offset.unwrap(),
            payload_size: requested_system_update_payload.payload_size.unwrap(),
            header_key_value_pairs: expected_header_key_value_pairs,
        };

        // Place the following in scope block so we don't hold the mutex for the rest of the test
        {
            let fake_update_engine_test_details =
                ctx.fake_update_engine_test_details.lock().unwrap();
            let system_update_payload =
                fake_update_engine_test_details.system_update_payload.as_ref().unwrap();
            assert_eq!(*system_update_payload, expected_system_update_payload);
        }
    }

    #[tokio::test]
    async fn test_system_update_activate_rollback_succeeds() {
        // GIVEN that a valid system update has been activated successfully
        let mut mock_services = MockServices::new();
        mock_services
            .boot_control_service
            .expect_get_current_slot()
            .times(1)
            .returning(|| Ok(/* initial_boot_slot=*/ 1));
        mock_services
            .power_state_client
            .expect_power_state_report_processing_complete()
            .times(1)
            .returning(|| Ok(()));
        mock_services
            .boot_control_service
            .expect_revert_to_slot()
            .withf(|&slot| slot == 1) // Expect argument to be 1
            .times(1)
            .returning(|_| Ok(()));

        let temp_dirs = TempDirs::new();
        let (mut ctx, init) = TestContext::new(temp_dirs.as_temp_paths(), mock_services);
        let service_impl = UpdateManagerServiceImpl::new_with_paths(
            init,
            temp_dirs.ota_package.path(),
            &ctx.save_state_path,
        )
        .await
        .unwrap();

        let requested_system_update_payload = make_system_update_payload();

        subscribe_and_wait_for_state(&mut ctx, &service_impl, ApiUpdateState::READY).await;

        UpdateManagerService::Prepare(
            &service_impl,
            make_service_fqin(),
            &PrepareRequest {
                payload: Some(PrepareRequestPayload::SystemUpdatePayload(
                    requested_system_update_payload.clone(),
                )),
                ..Default::default()
            },
        )
        .await
        .expect("Failure when calling UpdateManagerService::Prepare");

        assert_prepare_success(&mut ctx).await;

        UpdateManagerService::Activate(
            &service_impl,
            make_service_fqin(),
            &ActivateRequest::default(),
        )
        .await
        .expect("Failure when calling UpdateManagerService::Activate");

        wait_for_successful_callback(&mut ctx.callback_receiver, STATE_CHANGE_TIMEOUT).await;

        wait_for_successful_callback(&mut ctx.callback_receiver, STATE_CHANGE_TIMEOUT).await;

        wait_for_successful_callback(&mut ctx.callback_receiver, STATE_CHANGE_TIMEOUT).await;
        // WHEN the Rollback() request is sent.
        let rollback_result = UpdateManagerService::Rollback(
            &service_impl,
            make_service_fqin(),
            &RollbackRequest::default(),
        )
        .await;

        let update_state_rollback =
            wait_for_successful_callback(&mut ctx.callback_receiver, STATE_CHANGE_TIMEOUT).await;
        if let StatusUpdate::UpdateManagerStatus(update_manager_status) = update_state_rollback {
            assert_eq!(
                update_manager_status.update_state.enum_value().unwrap(),
                ApiUpdateState::ACTIVATE_PRE_REBOOT_ROLLBACK
            );
        } else {
            panic!("Unexpected StatusUpdate: {update_state_rollback:?}");
        }
        // After Rollback() should eventually return to READY
        let update_state_ready =
            wait_for_successful_callback(&mut ctx.callback_receiver, STATE_CHANGE_TIMEOUT).await;
        if let StatusUpdate::UpdateManagerStatus(update_manager_status) = update_state_ready {
            assert_eq!(
                update_manager_status.update_state.enum_value().unwrap(),
                ApiUpdateState::READY
            );
        } else {
            panic!("Unexpected StatusUpdate: {update_state_ready:?}");
        }
        // THEN the Rollback() request should succeed.
        assert!(rollback_result.is_ok());

        // Callbacks should only be sent if the service changes states. After the above state
        // changes, no more state changes should occur. Verify this by ensuring no pending
        // callbacks exist.
        assert!(ctx.callback_receiver.try_recv().is_err());

        // Verify that reset_status() was called.
        let fake_update_engine_test_details = ctx.fake_update_engine_test_details.lock().unwrap();
        assert!(fake_update_engine_test_details.reset_status_called);
    }

    #[tokio::test]
    async fn test_activate_pre_reboot_complete_opposite_slot_bootable() {
        let temp_dirs = TempDirs::new();
        let temp_paths = temp_dirs.as_temp_paths();
        let ota_package_path = temp_paths.ota_package.clone();
        let mut mock_services = MockServices::new();
        let opposite_slot = 0;

        mock_services.boot_control_service.expect_get_current_slot().times(1).returning(|| Ok(1));
        mock_services
            .power_state_client
            .expect_power_state_report_processing_complete()
            .times(1)
            .returning(|| Ok(()));

        {
            let (mut ctx, init) = TestContext::new(temp_paths.clone(), mock_services);

            let service_impl = UpdateManagerServiceImpl::new_with_paths(
                init,
                &ota_package_path,
                &ctx.save_state_path,
            )
            .await
            .unwrap();

            let requested_system_update_payload = make_system_update_payload();

            subscribe_and_wait_for_state(&mut ctx, &service_impl, ApiUpdateState::READY).await;

            UpdateManagerService::Prepare(
                &service_impl,
                make_service_fqin(),
                &PrepareRequest {
                    payload: Some(PrepareRequestPayload::SystemUpdatePayload(
                        requested_system_update_payload.clone(),
                    )),
                    ..Default::default()
                },
            )
            .await
            .expect("Failure when calling UpdateManagerService::Prepare");

            assert_prepare_success(&mut ctx).await;

            let result = UpdateManagerService::Activate(
                &service_impl,
                make_service_fqin(),
                &ActivateRequest::default(),
            )
            .await;

            // Wait for the activate pre-reboot step to finish
            let update_state_start =
                wait_for_successful_callback(&mut ctx.callback_receiver, STATE_CHANGE_TIMEOUT)
                    .await;
            let update_progress =
                wait_for_successful_callback(&mut ctx.callback_receiver, STATE_CHANGE_TIMEOUT)
                    .await;
            let update_state_end =
                wait_for_successful_callback(&mut ctx.callback_receiver, STATE_CHANGE_TIMEOUT)
                    .await;
            assert!(result.is_ok());

            if let StatusUpdate::UpdateManagerStatus(update_manager_status) = update_state_start {
                assert_eq!(
                    update_manager_status.update_state.enum_value().unwrap(),
                    ApiUpdateState::ACTIVATE_PRE_REBOOT
                );
            } else {
                panic!("Unexpected StatusUpdate: {update_state_start:?}");
            }

            if let StatusUpdate::UpdateProgress(update_progress) = update_progress {
                assert_eq!(update_progress.status_code, IDLE);
                assert_eq!(update_progress.percentage, STATUS_UPDATE_PERCENTAGE_ACTIVE);
            } else {
                panic!("Unexpected StatusUpdate: {update_progress:?}");
            }

            if let StatusUpdate::UpdateManagerStatus(update_manager_status) = update_state_end {
                assert_eq!(
                    update_manager_status.update_state.enum_value().unwrap(),
                    ApiUpdateState::ACTIVATE_PRE_REBOOT_COMPLETE
                );
            } else {
                panic!("Unexpected StatusUpdate: {update_state_end:?}");
            }
        }

        let ota_package_path = temp_paths.ota_package.clone();
        let mut new_mock_services = MockServices::new();
        new_mock_services
            .boot_control_service
            .expect_get_current_slot()
            .times(1)
            .returning(|| Ok(1));
        new_mock_services
            .boot_control_service
            .expect_is_slot_bootable()
            .times(1)
            .withf(move |&slot| slot == opposite_slot)
            .returning(|_| Ok(true));
        let (ctx, init) = TestContext::new(temp_paths.clone(), new_mock_services);

        let service_impl =
            UpdateManagerServiceImpl::new_with_paths(init, &ota_package_path, &ctx.save_state_path)
                .await
                .unwrap();

        let status_result = UpdateManagerService::GetStatus(
            &service_impl,
            make_service_fqin(),
            &GetStatusRequest::default(),
        )
        .await
        .unwrap();

        assert_eq!(
            status_result.update_manager_status.update_state.unwrap(),
            ApiUpdateState::ACTIVATE_PRE_REBOOT_COMPLETE
        );
    }

    #[tokio::test]
    async fn test_activate_pre_reboot_complete_opposite_slot_not_bootable() {
        let temp_dirs = TempDirs::new();
        let temp_paths = temp_dirs.as_temp_paths();
        let ota_package_path = temp_paths.ota_package.clone();
        let mut mock_services = MockServices::new();
        let opposite_slot = 0;

        mock_services.boot_control_service.expect_get_current_slot().times(1).returning(|| Ok(1));
        mock_services
            .power_state_client
            .expect_power_state_report_processing_complete()
            .times(1)
            .returning(|| Ok(()));

        {
            let (mut ctx, init) = TestContext::new(temp_paths.clone(), mock_services);

            let service_impl = UpdateManagerServiceImpl::new_with_paths(
                init,
                &ota_package_path,
                &ctx.save_state_path,
            )
            .await
            .unwrap();

            let requested_system_update_payload = make_system_update_payload();

            subscribe_and_wait_for_state(&mut ctx, &service_impl, ApiUpdateState::READY).await;

            UpdateManagerService::Prepare(
                &service_impl,
                make_service_fqin(),
                &PrepareRequest {
                    payload: Some(PrepareRequestPayload::SystemUpdatePayload(
                        requested_system_update_payload.clone(),
                    )),
                    ..Default::default()
                },
            )
            .await
            .expect("Failure when calling UpdateManagerService::Prepare");

            assert_prepare_success(&mut ctx).await;

            let result = UpdateManagerService::Activate(
                &service_impl,
                make_service_fqin(),
                &ActivateRequest::default(),
            )
            .await;

            //   Wait for the activate pre-reboot step to finish
            let update_state_start =
                wait_for_successful_callback(&mut ctx.callback_receiver, STATE_CHANGE_TIMEOUT)
                    .await;
            let update_progress =
                wait_for_successful_callback(&mut ctx.callback_receiver, STATE_CHANGE_TIMEOUT)
                    .await;
            let update_state_end =
                wait_for_successful_callback(&mut ctx.callback_receiver, STATE_CHANGE_TIMEOUT)
                    .await;
            assert!(result.is_ok());

            if let StatusUpdate::UpdateManagerStatus(update_manager_status) = update_state_start {
                assert_eq!(
                    update_manager_status.update_state.enum_value().unwrap(),
                    ApiUpdateState::ACTIVATE_PRE_REBOOT
                );
            } else {
                panic!("Unexpected StatusUpdate: {update_state_start:?}");
            }

            if let StatusUpdate::UpdateProgress(update_progress) = update_progress {
                assert_eq!(update_progress.status_code, IDLE);
                assert_eq!(update_progress.percentage, STATUS_UPDATE_PERCENTAGE_ACTIVE);
            } else {
                panic!("Unexpected StatusUpdate: {update_progress:?}");
            }

            if let StatusUpdate::UpdateManagerStatus(update_manager_status) = update_state_end {
                assert_eq!(
                    update_manager_status.update_state.enum_value().unwrap(),
                    ApiUpdateState::ACTIVATE_PRE_REBOOT_COMPLETE
                )
            } else {
                panic!("Unexpected StatusUpdate: {update_state_end:?}");
            }
        }

        let ota_package_path = temp_paths.ota_package.clone();
        let mut new_mock_services = MockServices::new();
        new_mock_services
            .boot_control_service
            .expect_get_current_slot()
            .times(1)
            .returning(|| Ok(1));
        new_mock_services
            .boot_control_service
            .expect_is_slot_bootable()
            .times(1)
            .withf(move |&slot| slot == opposite_slot)
            .returning(|_| Ok(false));
        let (ctx, init) = TestContext::new(temp_paths.clone(), new_mock_services);

        let service_impl =
            UpdateManagerServiceImpl::new_with_paths(init, &ota_package_path, &ctx.save_state_path)
                .await
                .unwrap();

        let status_result = UpdateManagerService::GetStatus(
            &service_impl,
            make_service_fqin(),
            &GetStatusRequest::default(),
        )
        .await
        .unwrap();

        assert_eq!(
            status_result.update_manager_status.update_state.unwrap(),
            ApiUpdateState::ACTIVATE_PRE_REBOOT_FAILURE
        );

        let failure_reason = status_result
            .update_manager_status
            .failure_reason
            .as_ref()
            .expect("No failure_reason in UpdateManagerStatus");

        assert_eq!(
            failure_reason.error_code.enum_value().expect("No error_code in FailureReason"),
            ErrorCode::UpdateEngineError,
        );

        assert_eq!(
            failure_reason.error_message,
            "Booted from the same slot after restart".to_string(),
        );
    }

    #[tokio::test]
    async fn test_service_bundle_prepare_with_invalid_boot_attempts_fails() {
        // GIVEN that a service bundle update with 0 boot attempts is present on the system
        let temp_dirs = TempDirs::new();
        let mock_services = MockServices::new();

        let (mut ctx, init) = TestContext::new(temp_dirs.as_temp_paths(), mock_services);
        let service_impl = UpdateManagerServiceImpl::new_with_paths(
            init,
            temp_dirs.ota_package.path(),
            &ctx.save_state_path,
        )
        .await
        .unwrap();

        let service_bundle_dir = tempdir().unwrap();
        let mut requested_service_bundle_update_payload = make_service_bundle_update_payload(
            service_bundle_dir.path(),
            SignedApexType::ValidSignedApex,
            SERVICE_BUNDLE_UPDATE_RETRIES,
        );
        // This needs to be done as a separate step as constructing the update payload with an
        // invalid value triggers an error.
        requested_service_bundle_update_payload.boot_attempts = 0;

        // WHEN a prepare request is sent
        let result = UpdateManagerService::Prepare(
            &service_impl,
            make_service_fqin(),
            &PrepareRequest {
                payload: Some(PrepareRequestPayload::ServiceBundleUpdatePayload(
                    requested_service_bundle_update_payload.into(),
                )),
                ..Default::default()
            },
        )
        .await;

        // THEN the prepare request should fail
        assert!(result.is_err());

        let e = result.unwrap_err();
        assert_eq!(e.status_code(), SdvStatusCode::InvalidArgument);
        assert!(e.to_string().contains("The number of boot attempts must be greater than 0"));

        // Callbacks should only be sent if the service changes states. No state changes should
        // occur as the error is returned in the request response.
        assert!(ctx.callback_receiver.try_recv().is_err());
    }

    #[tokio::test]
    async fn test_service_bundle_update_with_relative_apex_path_prepare_fails() {
        // GIVEN that a service bundle update with a relative APEX path is present on the system
        let temp_dirs = TempDirs::new();
        let mock_services = MockServices::new();

        let (mut ctx, init) = TestContext::new(temp_dirs.as_temp_paths(), mock_services);
        let service_impl = UpdateManagerServiceImpl::new_with_paths(
            init,
            temp_dirs.ota_package.path(),
            &ctx.save_state_path,
        )
        .await
        .unwrap();

        // WHEN a prepare request is sent
        let result = UpdateManagerService::Prepare(
            &service_impl,
            make_service_fqin(),
            &PrepareRequest {
                payload: Some(PrepareRequestPayload::ServiceBundleUpdatePayload(
                    ServiceBundleUpdatePayloadProto {
                        apex_path: vec!["relative/path/to/a.apex".to_string()],
                        boot_attempts: 2,
                        ..Default::default()
                    },
                )),
                ..Default::default()
            },
        )
        .await;

        // THEN the prepare request should fail
        assert!(result.is_err());

        // Callbacks should only be sent if the service changes states. No state changes should
        // occur as the error is returned in the request response.
        assert!(ctx.callback_receiver.try_recv().is_err());
    }

    #[tokio::test]
    async fn test_service_bundle_update_prepare_succeeds() {
        // GIVEN that a valid service_bundle update is present on the system
        let mut mock_services = MockServices::new();
        const EXPECTED_SESSION_ID: i32 = 1;
        let expected_child_session_ids = vec![2];

        mock_services
            .apex_service
            .expect_submit_staged_session()
            .times(1)
            .withf(move |session_id, child_session_ids| {
                *session_id == EXPECTED_SESSION_ID
                    && child_session_ids == expected_child_session_ids
            })
            .returning(|_, _| Ok(make_fake_apex_info_list()));

        let temp_dirs = TempDirs::new();

        let (mut ctx, init) = TestContext::new(temp_dirs.as_temp_paths(), mock_services);
        let service_impl = UpdateManagerServiceImpl::new_with_paths(
            init,
            temp_dirs.ota_package.path(),
            &ctx.save_state_path,
        )
        .await
        .unwrap();

        let service_bundle_dir = tempdir().unwrap();

        let requested_service_bundle_update_payload = make_service_bundle_update_payload(
            service_bundle_dir.path(),
            SignedApexType::ValidSignedApex,
            SERVICE_BUNDLE_UPDATE_RETRIES,
        );

        subscribe_and_wait_for_state(&mut ctx, &service_impl, ApiUpdateState::READY).await;

        // WHEN a prepare request is sent
        let result = UpdateManagerService::Prepare(
            &service_impl,
            make_service_fqin(),
            &PrepareRequest {
                payload: Some(PrepareRequestPayload::ServiceBundleUpdatePayload(
                    requested_service_bundle_update_payload.clone().into(),
                )),
                ..Default::default()
            },
        )
        .await;

        // Wait for the prepare step to finish
        let update_state_start =
            wait_for_successful_callback(&mut ctx.callback_receiver, STATE_CHANGE_TIMEOUT).await;
        let update_state_end =
            wait_for_successful_callback(&mut ctx.callback_receiver, STATE_CHANGE_TIMEOUT).await;

        // THEN the prepare request should succeed and the ApexService API
        // should be called with the correct payload details.
        assert!(result.is_ok());

        if let StatusUpdate::UpdateManagerStatus(update_manager_status) = update_state_start {
            assert_eq!(
                update_manager_status.update_state.enum_value().unwrap(),
                ApiUpdateState::PREPARE
            );
        } else {
            panic!("Unexpected StatusUpdate: {update_state_start:?}");
        }

        if let StatusUpdate::UpdateManagerStatus(update_manager_status) = update_state_end {
            assert_eq!(
                update_manager_status.update_state.enum_value().unwrap(),
                ApiUpdateState::PREPARE_COMPLETE
            );
        } else {
            panic!("Unexpected StatusUpdate: {update_state_end:?}");
        }

        // Callbacks should only be sent if the service changes states. After the above state
        // changes, no more state changes should occur. Verify this by ensuring no pending
        // callbacks exist.
        assert!(ctx.callback_receiver.try_recv().is_err());

        let app_staging_path = temp_dirs.app_staging.path();
        let parent_session_dir = app_staging_path.join("session_1");
        assert!(parent_session_dir.exists());

        for (index, apex_path) in
            requested_service_bundle_update_payload.apex_paths.iter().enumerate()
        {
            let apex_file_name = Path::new(apex_path).file_name().unwrap().to_str().unwrap();
            let session_dir = app_staging_path.join(format!(
                "{}{}",
                APEX_SERVICE_SESSION_DIR_PREFIX,
                EXPECTED_SESSION_ID + (index as i32) + 1
            ));
            assert!(session_dir.exists());
            let copied_apex_path = session_dir.as_path().join(apex_file_name);
            assert!(copied_apex_path.exists());

            let original_contents = tokio::fs::read(apex_path).await.unwrap();
            let copied_contents = tokio::fs::read(copied_apex_path).await.unwrap();
            assert_eq!(original_contents, copied_contents);
        }
    }

    #[tokio::test]
    async fn test_service_bundle_update_prepare_fails_with_bad_signed_apex() {
        // GIVEN that an invalid service_bundle update (caused by a bad signed APEX)
        // is present on the system
        let mock_services = MockServices::new();

        let temp_dirs = TempDirs::new();

        let (mut ctx, init) = TestContext::new(temp_dirs.as_temp_paths(), mock_services);
        let service_impl = UpdateManagerServiceImpl::new_with_paths(
            init,
            temp_dirs.ota_package.path(),
            &ctx.save_state_path,
        )
        .await
        .unwrap();

        let service_bundle_dir = tempdir().unwrap();

        let requested_service_bundle_update_payload = make_service_bundle_update_payload(
            service_bundle_dir.path(),
            SignedApexType::BadSignedApex,
            SERVICE_BUNDLE_UPDATE_RETRIES,
        );

        subscribe_and_wait_for_state(&mut ctx, &service_impl, ApiUpdateState::READY).await;

        // WHEN a prepare request is sent
        let result = UpdateManagerService::Prepare(
            &service_impl,
            make_service_fqin(),
            &PrepareRequest {
                payload: Some(PrepareRequestPayload::ServiceBundleUpdatePayload(
                    requested_service_bundle_update_payload.clone().into(),
                )),
                ..Default::default()
            },
        )
        .await;

        // Wait for the prepare step to finish
        let update_state_start =
            wait_for_successful_callback(&mut ctx.callback_receiver, STATE_CHANGE_TIMEOUT).await;
        let update_state_end =
            wait_for_successful_callback(&mut ctx.callback_receiver, STATE_CHANGE_TIMEOUT).await;

        // THEN the prepare request should fail as the APEX is not signed properly.
        assert!(result.is_ok());

        if let StatusUpdate::UpdateManagerStatus(update_manager_status) = update_state_start {
            assert_eq!(
                update_manager_status.update_state.enum_value().unwrap(),
                ApiUpdateState::PREPARE
            );
        } else {
            panic!("Unexpected StatusUpdate: {update_state_start:?}");
        }

        if let StatusUpdate::UpdateManagerStatus(update_manager_status) = update_state_end {
            assert_eq!(
                update_manager_status.update_state.enum_value().unwrap(),
                ApiUpdateState::PREPARE_FAILURE
            );
        } else {
            panic!("Unexpected StatusUpdate: {update_state_end:?}");
        }
    }

    #[tokio::test]
    async fn test_service_bundle_update_activate_succeeds() {
        // GIVEN that a valid service bundle update has been prepared successfully
        let mut mock_services = MockServices::new();
        const EXPECTED_SESSION_ID: i32 = 1;
        let expected_child_session_ids = vec![2];

        mock_services
            .apex_service
            .expect_submit_staged_session()
            .times(1)
            .withf(move |session_id, child_session_ids| {
                *session_id == EXPECTED_SESSION_ID
                    && child_session_ids == expected_child_session_ids
            })
            .returning(|_, _| Ok(make_fake_apex_info_list()));

        mock_services
            .apex_service
            .expect_mark_staged_session_ready()
            .times(1)
            .withf(move |session_id| *session_id == EXPECTED_SESSION_ID)
            .returning(|_| Ok(()));

        mock_services
            .vold_service
            .expect_start_checkpoint()
            .times(1)
            .withf(move |retry| *retry == SERVICE_BUNDLE_UPDATE_RETRIES)
            .returning(|_| Ok(()));

        let temp_dirs = TempDirs::new();

        let (mut ctx, init) = TestContext::new(temp_dirs.as_temp_paths(), mock_services);
        let service_impl = UpdateManagerServiceImpl::new_with_paths(
            init,
            temp_dirs.ota_package.path(),
            &ctx.save_state_path,
        )
        .await
        .unwrap();

        let service_bundle_dir = tempdir().unwrap();
        let requested_service_bundle_update_payload = make_service_bundle_update_payload(
            service_bundle_dir.path(),
            SignedApexType::ValidSignedApex,
            SERVICE_BUNDLE_UPDATE_RETRIES,
        );

        subscribe_and_wait_for_state(&mut ctx, &service_impl, ApiUpdateState::READY).await;

        UpdateManagerService::Prepare(
            &service_impl,
            make_service_fqin(),
            &PrepareRequest {
                payload: Some(PrepareRequestPayload::ServiceBundleUpdatePayload(
                    requested_service_bundle_update_payload.clone().into(),
                )),
                ..Default::default()
            },
        )
        .await
        .unwrap();

        // Wait for the prepare step to finish
        wait_for_successful_callback(&mut ctx.callback_receiver, STATE_CHANGE_TIMEOUT).await;
        wait_for_successful_callback(&mut ctx.callback_receiver, STATE_CHANGE_TIMEOUT).await;

        // WHEN an activate request is sent
        let result = UpdateManagerService::Activate(
            &service_impl,
            make_service_fqin(),
            &ActivateRequest::default(),
        )
        .await;

        // Wait for the activate pre-reboot step to finish
        let update_state_start =
            wait_for_successful_callback(&mut ctx.callback_receiver, STATE_CHANGE_TIMEOUT).await;
        let update_state_end =
            wait_for_successful_callback(&mut ctx.callback_receiver, STATE_CHANGE_TIMEOUT).await;

        // THEN the activate request should succeed and the ApexService API
        // should be called with the correct payload details.
        assert!(result.is_ok());

        if let StatusUpdate::UpdateManagerStatus(update_manager_status) = update_state_start {
            assert_eq!(
                update_manager_status.update_state.enum_value().unwrap(),
                ApiUpdateState::ACTIVATE_PRE_REBOOT
            );
        } else {
            panic!("Unexpected StatusUpdate: {update_state_start:?}");
        }

        if let StatusUpdate::UpdateManagerStatus(update_manager_status) = update_state_end {
            assert_eq!(
                update_manager_status.update_state.enum_value().unwrap(),
                ApiUpdateState::ACTIVATE_PRE_REBOOT_COMPLETE
            );
        } else {
            panic!("Unexpected StatusUpdate: {update_state_end:?}");
        }

        // Callbacks should only be sent if the service changes states. After the above state
        // changes, no more state changes should occur. Verify this by ensuring no pending
        // callbacks exist.
        assert!(ctx.callback_receiver.try_recv().is_err());
    }

    #[tokio::test]
    async fn test_service_bundle_update_activate_succeeds_after_reboot() {
        use apex_aidl_interface::aidl::android::apex::ApexSessionInfo::ApexSessionInfo;
        let temp_dirs = TempDirs::new();
        let temp_paths = temp_dirs.as_temp_paths();
        let ota_package_path = temp_paths.ota_package.clone();
        let mut mock_services = MockServices::new();
        const EXPECTED_SESSION_ID: i32 = 1;
        let expected_child_session_ids = vec![2];

        mock_services
            .apex_service
            .expect_submit_staged_session()
            .times(1)
            .withf(move |session_id, child_session_ids| {
                *session_id == EXPECTED_SESSION_ID
                    && child_session_ids == expected_child_session_ids
            })
            .returning(|_, _| Ok(make_fake_apex_info_list()));

        mock_services
            .apex_service
            .expect_mark_staged_session_ready()
            .times(1)
            .withf(move |session_id| *session_id == EXPECTED_SESSION_ID)
            .returning(|_| Ok(()));

        mock_services
            .vold_service
            .expect_start_checkpoint()
            .times(1)
            .withf(move |retry| *retry == SERVICE_BUNDLE_UPDATE_RETRIES)
            .returning(|_| Ok(()));

        {
            let (mut ctx, init) = TestContext::new(temp_paths.clone(), mock_services);

            let service_impl = UpdateManagerServiceImpl::new_with_paths(
                init,
                &ota_package_path,
                &ctx.save_state_path,
            )
            .await
            .unwrap();

            let service_bundle_dir = tempdir().unwrap();
            let requested_service_bundle_update_payload = make_service_bundle_update_payload(
                service_bundle_dir.path(),
                SignedApexType::ValidSignedApex,
                SERVICE_BUNDLE_UPDATE_RETRIES,
            );

            subscribe_and_wait_for_state(&mut ctx, &service_impl, ApiUpdateState::READY).await;

            UpdateManagerService::Prepare(
                &service_impl,
                make_service_fqin(),
                &PrepareRequest {
                    payload: Some(PrepareRequestPayload::ServiceBundleUpdatePayload(
                        requested_service_bundle_update_payload.clone().into(),
                    )),
                    ..Default::default()
                },
            )
            .await
            .expect("Failure when calling UpdateManagerService::Prepare");

            wait_for_successful_callback(&mut ctx.callback_receiver, STATE_CHANGE_TIMEOUT).await;
            wait_for_successful_callback(&mut ctx.callback_receiver, STATE_CHANGE_TIMEOUT).await;

            let result = UpdateManagerService::Activate(
                &service_impl,
                make_service_fqin(),
                &ActivateRequest::default(),
            )
            .await;

            // Wait for the activate pre-reboot step to finish
            let update_state_start =
                wait_for_successful_callback(&mut ctx.callback_receiver, STATE_CHANGE_TIMEOUT)
                    .await;
            let update_state_end =
                wait_for_successful_callback(&mut ctx.callback_receiver, STATE_CHANGE_TIMEOUT)
                    .await;
            assert!(result.is_ok());

            if let StatusUpdate::UpdateManagerStatus(update_manager_status) = update_state_start {
                assert_eq!(
                    update_manager_status.update_state.enum_value().unwrap(),
                    ApiUpdateState::ACTIVATE_PRE_REBOOT
                );
            } else {
                panic!("Unexpected StatusUpdate: {update_state_start:?}");
            }

            if let StatusUpdate::UpdateManagerStatus(update_manager_status) = update_state_end {
                assert_eq!(
                    update_manager_status.update_state.enum_value().unwrap(),
                    ApiUpdateState::ACTIVATE_PRE_REBOOT_COMPLETE
                );
            } else {
                panic!("Unexpected StatusUpdate: {update_state_end:?}");
            }
        }

        let ota_package_path = temp_paths.ota_package.clone();
        let mut new_mock_services = MockServices::new();
        new_mock_services
            .apex_service
            .expect_get_staged_session_info()
            .times(1)
            .withf(|&session_id| session_id == EXPECTED_SESSION_ID)
            .returning(|_| {
                Ok(ApexSessionInfo {
                    sessionId: EXPECTED_SESSION_ID,
                    isActivated: true,
                    ..Default::default()
                })
            });
        let (ctx, init) = TestContext::new(temp_paths.clone(), new_mock_services);
        let service_impl =
            UpdateManagerServiceImpl::new_with_paths(init, &ota_package_path, &ctx.save_state_path)
                .await
                .unwrap();

        let status_result = UpdateManagerService::GetStatus(
            &service_impl,
            make_service_fqin(),
            &GetStatusRequest::default(),
        )
        .await
        .unwrap();

        assert_eq!(
            status_result.update_manager_status.update_state.unwrap(),
            ApiUpdateState::ACTIVATE_POST_REBOOT_COMPLETE
        );
    }

    #[tokio::test]
    async fn test_commit_service_bundle_succeeds() {
        use apex_aidl_interface::aidl::android::apex::ApexSessionInfo::ApexSessionInfo;
        let temp_dirs = TempDirs::new();
        let temp_paths = temp_dirs.as_temp_paths();
        let ota_package_path = temp_paths.ota_package.clone();
        let mut mock_services = MockServices::new();
        const EXPECTED_SESSION_ID: i32 = 1;
        let expected_child_session_ids = vec![2];

        mock_services
            .apex_service
            .expect_submit_staged_session()
            .times(1)
            .withf(move |session_id, child_session_ids| {
                *session_id == EXPECTED_SESSION_ID
                    && child_session_ids == expected_child_session_ids
            })
            .returning(|_, _| Ok(make_fake_apex_info_list()));

        mock_services
            .apex_service
            .expect_mark_staged_session_ready()
            .times(1)
            .withf(move |session_id| *session_id == EXPECTED_SESSION_ID)
            .returning(|_| Ok(()));

        mock_services
            .vold_service
            .expect_start_checkpoint()
            .times(1)
            .withf(move |retry| *retry == SERVICE_BUNDLE_UPDATE_RETRIES)
            .returning(|_| Ok(()));

        {
            let (mut ctx, init) = TestContext::new(temp_paths.clone(), mock_services);

            let service_impl = UpdateManagerServiceImpl::new_with_paths(
                init,
                &ota_package_path,
                &ctx.save_state_path,
            )
            .await
            .unwrap();

            let service_bundle_dir = tempdir().unwrap();

            let requested_service_bundle_update_payload = make_service_bundle_update_payload(
                service_bundle_dir.path(),
                SignedApexType::ValidSignedApex,
                SERVICE_BUNDLE_UPDATE_RETRIES,
            );

            subscribe_and_wait_for_state(&mut ctx, &service_impl, ApiUpdateState::READY).await;

            UpdateManagerService::Prepare(
                &service_impl,
                make_service_fqin(),
                &PrepareRequest {
                    payload: Some(PrepareRequestPayload::ServiceBundleUpdatePayload(
                        requested_service_bundle_update_payload.clone().into(),
                    )),
                    ..Default::default()
                },
            )
            .await
            .expect("Failure when calling UpdateManagerService::Prepare");

            wait_for_successful_callback(&mut ctx.callback_receiver, STATE_CHANGE_TIMEOUT).await;
            wait_for_successful_callback(&mut ctx.callback_receiver, STATE_CHANGE_TIMEOUT).await;

            let result = UpdateManagerService::Activate(
                &service_impl,
                make_service_fqin(),
                &ActivateRequest::default(),
            )
            .await;

            // Wait for the activate pre-reboot step to finish
            let update_state_start =
                wait_for_successful_callback(&mut ctx.callback_receiver, STATE_CHANGE_TIMEOUT)
                    .await;
            let update_state_end =
                wait_for_successful_callback(&mut ctx.callback_receiver, STATE_CHANGE_TIMEOUT)
                    .await;
            assert!(result.is_ok());

            if let StatusUpdate::UpdateManagerStatus(update_manager_status) = update_state_start {
                assert_eq!(
                    update_manager_status.update_state.enum_value().unwrap(),
                    ApiUpdateState::ACTIVATE_PRE_REBOOT
                );
            } else {
                panic!("Unexpected StatusUpdate: {update_state_start:?}");
            }

            if let StatusUpdate::UpdateManagerStatus(update_manager_status) = update_state_end {
                assert_eq!(
                    update_manager_status.update_state.enum_value().unwrap(),
                    ApiUpdateState::ACTIVATE_PRE_REBOOT_COMPLETE
                );
            } else {
                panic!("Unexpected StatusUpdate: {update_state_end:?}");
            }
        }

        let ota_package_path = temp_paths.ota_package.clone();
        let mut new_mock_services = MockServices::new();
        new_mock_services
            .apex_service
            .expect_get_staged_session_info()
            .times(1)
            .withf(|&session_id| session_id == EXPECTED_SESSION_ID)
            .returning(|_| {
                Ok(ApexSessionInfo {
                    sessionId: EXPECTED_SESSION_ID,
                    isActivated: true,
                    ..Default::default()
                })
            });

        new_mock_services
            .apex_service
            .expect_mark_staged_session_successful()
            .withf(|&session_id| i64::from(session_id) == EXPECTED_SESSION_ID as i64)
            .times(1)
            .returning(|_| Ok(()));
        new_mock_services.vold_service.expect_commit_changes().times(1).returning(|| Ok(()));
        new_mock_services.apex_service.expect_mark_boot_successful().times(1).returning(|| Ok(()));

        let (mut ctx, init) = TestContext::new(temp_paths.clone(), new_mock_services);
        let service_impl =
            UpdateManagerServiceImpl::new_with_paths(init, &ota_package_path, &ctx.save_state_path)
                .await
                .unwrap();

        subscribe_and_wait_for_state(
            &mut ctx,
            &service_impl,
            ApiUpdateState::ACTIVATE_POST_REBOOT_COMPLETE,
        )
        .await;

        UpdateManagerService::Commit(&service_impl, make_service_fqin(), &Default::default())
            .await
            .expect("Failure when calling UpdateManagerService::Commit");

        let commit_state =
            wait_for_successful_callback(&mut ctx.callback_receiver, STATE_CHANGE_TIMEOUT).await;

        if let StatusUpdate::UpdateManagerStatus(update_manager_status) = commit_state {
            assert_eq!(
                update_manager_status.update_state.enum_value().unwrap(),
                ApiUpdateState::COMMIT
            );
        } else {
            panic!("Unexpected StatusUpdate: {commit_state:?}");
        }

        let ready_state_update =
            wait_for_successful_callback(&mut ctx.callback_receiver, STATE_CHANGE_TIMEOUT).await;
        if let StatusUpdate::UpdateManagerStatus(update_manager_status) = ready_state_update {
            assert_eq!(
                update_manager_status.update_state.enum_value().unwrap(),
                ApiUpdateState::READY
            );
        } else {
            panic!("Expected READY status update, got {ready_state_update:?}");
        }
    }

    #[tokio::test]
    async fn test_system_update_activate_succeeds_after_reboot() {
        let temp_dirs = TempDirs::new();
        let temp_paths = temp_dirs.as_temp_paths();
        let ota_package_path = temp_paths.ota_package.clone();
        let mut mock_services = MockServices::new();
        mock_services
            .boot_control_service
            .expect_get_current_slot()
            .times(1)
            .returning(|| Ok(/* initial_boot_slot=*/ 1));
        mock_services
            .power_state_client
            .expect_power_state_report_processing_complete()
            .times(1)
            .returning(|| Ok(()));

        {
            let (mut ctx, init) = TestContext::new(temp_paths.clone(), mock_services);

            let service_impl = UpdateManagerServiceImpl::new_with_paths(
                init,
                &ota_package_path,
                &ctx.save_state_path,
            )
            .await
            .unwrap();

            let requested_system_update_payload = make_system_update_payload();

            subscribe_and_wait_for_state(&mut ctx, &service_impl, ApiUpdateState::READY).await;

            UpdateManagerService::Prepare(
                &service_impl,
                make_service_fqin(),
                &PrepareRequest {
                    payload: Some(PrepareRequestPayload::SystemUpdatePayload(
                        requested_system_update_payload.clone(),
                    )),
                    ..Default::default()
                },
            )
            .await
            .expect("Failure when calling UpdateManagerService::Prepare");

            assert_prepare_success(&mut ctx).await;

            let result = UpdateManagerService::Activate(
                &service_impl,
                make_service_fqin(),
                &ActivateRequest::default(),
            )
            .await;

            // Wait for the activate pre-reboot step to finish
            let update_state_start =
                wait_for_successful_callback(&mut ctx.callback_receiver, STATE_CHANGE_TIMEOUT)
                    .await;
            let update_progress =
                wait_for_successful_callback(&mut ctx.callback_receiver, STATE_CHANGE_TIMEOUT)
                    .await;
            let update_state_end =
                wait_for_successful_callback(&mut ctx.callback_receiver, STATE_CHANGE_TIMEOUT)
                    .await;
            assert!(result.is_ok());

            if let StatusUpdate::UpdateManagerStatus(update_manager_status) = update_state_start {
                assert_eq!(
                    update_manager_status.update_state.enum_value().unwrap(),
                    ApiUpdateState::ACTIVATE_PRE_REBOOT
                );
            } else {
                panic!("Unexpected StatusUpdate: {update_state_start:?}");
            }

            if let StatusUpdate::UpdateProgress(update_progress) = update_progress {
                assert_eq!(update_progress.status_code, IDLE);
                assert_eq!(update_progress.percentage, STATUS_UPDATE_PERCENTAGE_ACTIVE);
            } else {
                panic!("Unexpected StatusUpdate: {update_progress:?}");
            }

            if let StatusUpdate::UpdateManagerStatus(update_manager_status) = update_state_end {
                assert_eq!(
                    update_manager_status.update_state.enum_value().unwrap(),
                    ApiUpdateState::ACTIVATE_PRE_REBOOT_COMPLETE
                );
            } else {
                panic!("Unexpected StatusUpdate: {update_state_end:?}");
            }
        }

        let ota_package_path = temp_paths.ota_package.clone();
        let mut new_mock_services = MockServices::new();
        new_mock_services
            .boot_control_service
            .expect_get_current_slot()
            .times(1)
            .returning(|| Ok(0));

        new_mock_services
            .boot_control_service
            .expect_is_slot_marked_successful()
            .times(1)
            .returning(|_| Ok(false));
        let (ctx, init) = TestContext::new(temp_paths.clone(), new_mock_services);
        let service_impl =
            UpdateManagerServiceImpl::new_with_paths(init, &ota_package_path, &ctx.save_state_path)
                .await
                .unwrap();

        let status_result = UpdateManagerService::GetStatus(
            &service_impl,
            make_service_fqin(),
            &GetStatusRequest::default(),
        )
        .await
        .unwrap();

        assert_eq!(
            status_result.update_manager_status.update_state.unwrap(),
            ApiUpdateState::ACTIVATE_POST_REBOOT_COMPLETE
        );
    }

    #[tokio::test]
    async fn test_system_update_activate_slot_mismatch_succeeds_after_reboot() {
        let temp_dirs = TempDirs::new();
        let temp_paths = temp_dirs.as_temp_paths();
        let ota_package_path = temp_paths.ota_package.clone();
        let mut mock_services = MockServices::new();
        mock_services
            .boot_control_service
            .expect_get_current_slot()
            .times(1)
            .returning(|| Ok(/* initial_boot_slot=*/ 1));
        mock_services
            .power_state_client
            .expect_power_state_report_processing_complete()
            .times(1)
            .returning(|| Ok(()));

        {
            let (mut ctx, init) = TestContext::new(temp_paths.clone(), mock_services);

            let service_impl = UpdateManagerServiceImpl::new_with_paths(
                init,
                &ota_package_path,
                &ctx.save_state_path,
            )
            .await
            .unwrap();

            let requested_system_update_payload = make_system_update_payload();

            subscribe_and_wait_for_state(&mut ctx, &service_impl, ApiUpdateState::READY).await;

            UpdateManagerService::Prepare(
                &service_impl,
                make_service_fqin(),
                &PrepareRequest {
                    payload: Some(PrepareRequestPayload::SystemUpdatePayload(
                        requested_system_update_payload.clone(),
                    )),
                    ..Default::default()
                },
            )
            .await
            .expect("Failure when calling UpdateManagerService::Prepare");

            assert_prepare_success(&mut ctx).await;

            let result = UpdateManagerService::Activate(
                &service_impl,
                make_service_fqin(),
                &ActivateRequest::default(),
            )
            .await;
            // Wait for the activate pre-reboot step to finish
            let update_state_start =
                wait_for_successful_callback(&mut ctx.callback_receiver, STATE_CHANGE_TIMEOUT)
                    .await;
            let update_progress =
                wait_for_successful_callback(&mut ctx.callback_receiver, STATE_CHANGE_TIMEOUT)
                    .await;
            let update_state_end =
                wait_for_successful_callback(&mut ctx.callback_receiver, STATE_CHANGE_TIMEOUT)
                    .await;
            assert!(result.is_ok());

            if let StatusUpdate::UpdateManagerStatus(update_manager_status) = update_state_start {
                assert_eq!(
                    update_manager_status.update_state.enum_value().unwrap(),
                    ApiUpdateState::ACTIVATE_PRE_REBOOT
                );
            } else {
                panic!("Unexpected StatusUpdate: {update_state_start:?}");
            }
            if let StatusUpdate::UpdateProgress(update_progress) = update_progress {
                assert_eq!(update_progress.status_code, IDLE);
                assert_eq!(update_progress.percentage, STATUS_UPDATE_PERCENTAGE_ACTIVE);
            } else {
                panic!("Unexpected StatusUpdate: {update_progress:?}");
            }

            if let StatusUpdate::UpdateManagerStatus(update_manager_status) = update_state_end {
                assert_eq!(
                    update_manager_status.update_state.enum_value().unwrap(),
                    ApiUpdateState::ACTIVATE_PRE_REBOOT_COMPLETE
                );
            } else {
                panic!("Unexpected StatusUpdate: {update_state_end:?}");
            }
        }

        let ota_package_path = temp_paths.ota_package.clone();
        let mut new_mock_services = MockServices::new();
        new_mock_services
            .boot_control_service
            .expect_get_current_slot()
            .times(1)
            .returning(|| Ok(0));

        new_mock_services
            .boot_control_service
            .expect_is_slot_marked_successful()
            .times(1)
            .returning(|_| Ok(true));

        let (ctx, init) = TestContext::new(temp_paths.clone(), new_mock_services);
        let service_impl =
            UpdateManagerServiceImpl::new_with_paths(init, &ota_package_path, &ctx.save_state_path)
                .await
                .unwrap();

        let status_result = UpdateManagerService::GetStatus(
            &service_impl,
            make_service_fqin(),
            &GetStatusRequest::default(),
        )
        .await
        .unwrap();

        assert_eq!(
            status_result.update_manager_status.update_state.unwrap(),
            ApiUpdateState::READY
        );
    }

    #[tokio::test]
    async fn test_system_update_activate_fails_after_reboot_same_slot() {
        let temp_dirs = TempDirs::new();
        let temp_paths = temp_dirs.as_temp_paths();
        let ota_package_path = temp_paths.ota_package.clone();
        let mut mock_services = MockServices::new();
        let opposite_slot = 0;

        mock_services
            .boot_control_service
            .expect_get_current_slot()
            .times(1)
            .returning(|| Ok(/* initial_boot_slot=*/ 1));
        mock_services
            .power_state_client
            .expect_power_state_report_processing_complete()
            .times(1)
            .returning(|| Ok(()));

        {
            let (mut ctx, init) = TestContext::new(temp_paths.clone(), mock_services);

            let service_impl = UpdateManagerServiceImpl::new_with_paths(
                init,
                &ota_package_path,
                &ctx.save_state_path,
            )
            .await
            .unwrap();

            let requested_system_update_payload = make_system_update_payload();

            subscribe_and_wait_for_state(&mut ctx, &service_impl, ApiUpdateState::READY).await;

            UpdateManagerService::Prepare(
                &service_impl,
                make_service_fqin(),
                &PrepareRequest {
                    payload: Some(PrepareRequestPayload::SystemUpdatePayload(
                        requested_system_update_payload.clone(),
                    )),
                    ..Default::default()
                },
            )
            .await
            .expect("Failure when calling UpdateManagerService::Prepare");

            assert_prepare_success(&mut ctx).await;

            let result = UpdateManagerService::Activate(
                &service_impl,
                make_service_fqin(),
                &ActivateRequest::default(),
            )
            .await;

            // Wait for the activate pre-reboot step to finish
            let update_state_start =
                wait_for_successful_callback(&mut ctx.callback_receiver, STATE_CHANGE_TIMEOUT)
                    .await;
            let update_progress =
                wait_for_successful_callback(&mut ctx.callback_receiver, STATE_CHANGE_TIMEOUT)
                    .await;
            let update_state_end =
                wait_for_successful_callback(&mut ctx.callback_receiver, STATE_CHANGE_TIMEOUT)
                    .await;
            assert!(result.is_ok());

            if let StatusUpdate::UpdateManagerStatus(update_manager_status) = update_state_start {
                assert_eq!(
                    update_manager_status.update_state.enum_value().unwrap(),
                    ApiUpdateState::ACTIVATE_PRE_REBOOT
                );
            } else {
                panic!("Unexpected StatusUpdate: {update_state_start:?}");
            }

            if let StatusUpdate::UpdateProgress(update_progress) = update_progress {
                assert_eq!(update_progress.status_code, IDLE);
                assert_eq!(update_progress.percentage, STATUS_UPDATE_PERCENTAGE_ACTIVE);
            } else {
                panic!("Unexpected StatusUpdate: {update_progress:?}");
            }

            if let StatusUpdate::UpdateManagerStatus(update_manager_status) = update_state_end {
                assert_eq!(
                    update_manager_status.update_state.enum_value().unwrap(),
                    ApiUpdateState::ACTIVATE_PRE_REBOOT_COMPLETE
                );
            } else {
                panic!("Unexpected StatusUpdate: {update_state_end:?}");
            }
        }

        let ota_package_path = temp_paths.ota_package.clone();
        let mut new_mock_services = MockServices::new();
        new_mock_services
            .boot_control_service
            .expect_get_current_slot()
            .times(1)
            .returning(|| Ok(1)); // Still on the initial slot (1) after reboot -> Failure

        new_mock_services
            .boot_control_service
            .expect_is_slot_bootable()
            .times(1)
            .withf(move |&slot| slot == opposite_slot)
            .returning(|_| Ok(true));

        let (ctx, init) = TestContext::new(temp_paths.clone(), new_mock_services);
        let service_impl =
            UpdateManagerServiceImpl::new_with_paths(init, &ota_package_path, &ctx.save_state_path)
                .await
                .unwrap();

        let status_result = UpdateManagerService::GetStatus(
            &service_impl,
            make_service_fqin(),
            &GetStatusRequest::default(),
        )
        .await
        .unwrap();

        assert_eq!(
            status_result.update_manager_status.update_state.unwrap(),
            ApiUpdateState::ACTIVATE_PRE_REBOOT_COMPLETE
        );
    }

    // TODO b/382136641 Add unit tests for rollback and commit
    #[tokio::test]
    async fn test_service_bundle_update_activate_rollback_transitions_to_rollback_state() {
        // GIVEN that a valid service bundle update has been prepared successfully
        let mut mock_services = MockServices::new();
        const EXPECTED_SESSION_ID: i32 = 1;
        let expected_child_session_ids = vec![2];

        mock_services
            .apex_service
            .expect_submit_staged_session()
            .times(1)
            .withf(move |session_id, child_session_ids| {
                *session_id == EXPECTED_SESSION_ID
                    && child_session_ids == expected_child_session_ids
            })
            .returning(|_, _| Ok(make_fake_apex_info_list()));

        mock_services
            .apex_service
            .expect_mark_staged_session_ready()
            .times(1)
            .withf(move |session_id| *session_id == EXPECTED_SESSION_ID)
            .returning(|_| Ok(()));

        mock_services
            .vold_service
            .expect_start_checkpoint()
            .times(1)
            .withf(move |retry| *retry == SERVICE_BUNDLE_UPDATE_RETRIES)
            .returning(|_| Ok(()));

        mock_services.apex_service.expect_revert_active_sessions().times(1).returning(|| Ok(()));
        mock_services
            .vold_service
            .expect_start_checkpoint()
            .withf(|&retry| retry == 0)
            .times(1)
            .returning(|_| Ok(()));

        let temp_dirs = TempDirs::new();

        let (mut ctx, init) = TestContext::new(temp_dirs.as_temp_paths(), mock_services);
        let service_impl = UpdateManagerServiceImpl::new_with_paths(
            init,
            temp_dirs.ota_package.path(),
            &ctx.save_state_path,
        )
        .await
        .unwrap();
        let service_bundle_dir = tempdir().unwrap();
        let requested_service_bundle_update_payload = make_service_bundle_update_payload(
            service_bundle_dir.path(),
            SignedApexType::ValidSignedApex,
            SERVICE_BUNDLE_UPDATE_RETRIES,
        );

        subscribe_and_wait_for_state(&mut ctx, &service_impl, ApiUpdateState::READY).await;

        UpdateManagerService::Prepare(
            &service_impl,
            make_service_fqin(),
            &PrepareRequest {
                payload: Some(PrepareRequestPayload::ServiceBundleUpdatePayload(
                    requested_service_bundle_update_payload.clone().into(),
                )),
                ..Default::default()
            },
        )
        .await
        .unwrap();

        wait_for_successful_callback(&mut ctx.callback_receiver, STATE_CHANGE_TIMEOUT).await;
        wait_for_successful_callback(&mut ctx.callback_receiver, STATE_CHANGE_TIMEOUT).await;

        UpdateManagerService::Activate(
            &service_impl,
            make_service_fqin(),
            &ActivateRequest::default(),
        )
        .await
        .expect("Failure when calling UpdateManagerService::Activate");

        wait_for_successful_callback(&mut ctx.callback_receiver, STATE_CHANGE_TIMEOUT).await;
        wait_for_successful_callback(&mut ctx.callback_receiver, STATE_CHANGE_TIMEOUT).await;

        let rollback_result = UpdateManagerService::Rollback(
            &service_impl,
            make_service_fqin(),
            &RollbackRequest::default(),
        )
        .await;

        let update_state_rollback =
            wait_for_successful_callback(&mut ctx.callback_receiver, STATE_CHANGE_TIMEOUT).await;
        if let StatusUpdate::UpdateManagerStatus(update_manager_status) = update_state_rollback {
            assert_eq!(
                update_manager_status.update_state.enum_value().unwrap(),
                ApiUpdateState::ACTIVATE_PRE_REBOOT_ROLLBACK
            );
        } else {
            panic!("Unexpected StatusUpdate: {update_state_rollback:?}");
        }
        // After Rollback() should eventually return to READY
        let update_state_ready =
            wait_for_successful_callback(&mut ctx.callback_receiver, STATE_CHANGE_TIMEOUT).await;
        if let StatusUpdate::UpdateManagerStatus(update_manager_status) = update_state_ready {
            assert_eq!(
                update_manager_status.update_state.enum_value().unwrap(),
                ApiUpdateState::READY
            );
        } else {
            panic!("Unexpected StatusUpdate: {update_state_ready:?}");
        }

        assert!(rollback_result.is_ok());
        assert!(
            temp_dirs.app_staging.path().read_dir().unwrap().next().is_none(),
            "The app staging directory is not empty"
        );
    }

    #[tokio::test]
    async fn test_uninstall_apex_succeeds() {
        let temp_dirs = TempDirs::new();
        let mut mock_services = MockServices::new();
        let module_name = String::from("any.name");
        let version_code = 1;
        let module_path = String::from("some/random/path");
        fake_active_apex(
            &mut mock_services,
            module_name.clone(),
            version_code,
            module_path.clone(),
        );
        mock_services
            .apex_service
            .expect_unstage_packages()
            .withf(move |paths| paths[0] == module_path)
            .times(1)
            .returning(|_| Ok(()));

        let (ctx, init) = TestContext::new(temp_dirs.as_temp_paths(), mock_services);
        let service_impl = UpdateManagerServiceImpl::new_with_paths(
            init,
            temp_dirs.ota_package.path(),
            &ctx.save_state_path,
        )
        .await
        .unwrap();

        let apex_uninstall_request = make_uninstall_apex_request(module_name, version_code);

        let result = UpdateManagerService::UninstallApex(
            &service_impl,
            make_service_fqin(),
            &apex_uninstall_request,
        )
        .await;

        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_uninstall_apex_unstage_fails() {
        let temp_dirs = TempDirs::new();
        let mut mock_services = MockServices::new();
        let module_name = String::from("any.name");
        let version_code = 1;
        let module_path = String::from("some/random/path");
        fake_active_apex(
            &mut mock_services,
            module_name.clone(),
            version_code,
            module_path.clone(),
        );
        mock_services
            .apex_service
            .expect_unstage_packages()
            .withf(move |paths| paths[0] == module_path)
            .times(1)
            .returning(|_| {
                Err(SdvStatus::with_message(SdvStatusCode::NotFound, "uninstall apex failed"))
            });

        let (ctx, init) = TestContext::new(temp_dirs.as_temp_paths(), mock_services);
        let service_impl = UpdateManagerServiceImpl::new_with_paths(
            init,
            temp_dirs.ota_package.path(),
            &ctx.save_state_path,
        )
        .await
        .unwrap();

        let apex_uninstall_request = make_uninstall_apex_request(module_name, version_code);

        let result = UpdateManagerService::UninstallApex(
            &service_impl,
            make_service_fqin(),
            &apex_uninstall_request,
        )
        .await;

        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("uninstall apex failed"));
    }

    #[tokio::test]
    async fn test_uninstall_apex_get_active_fails() {
        let temp_dirs = TempDirs::new();
        let mut mock_services = MockServices::new();
        let module_name = String::from("any.name");
        let version_code = 1;
        mock_services
            .apex_service
            .expect_get_active_packages()
            .times(1)
            .returning(|| Err(SdvStatus::new(SdvStatusCode::NotFound)));

        let (ctx, init) = TestContext::new(temp_dirs.as_temp_paths(), mock_services);
        let service_impl = UpdateManagerServiceImpl::new_with_paths(
            init,
            temp_dirs.ota_package.path(),
            &ctx.save_state_path,
        )
        .await
        .unwrap();

        let apex_uninstall_request = make_uninstall_apex_request(module_name, version_code);

        let result = UpdateManagerService::UninstallApex(
            &service_impl,
            make_service_fqin(),
            &apex_uninstall_request,
        )
        .await;

        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("Failed to get the active APEXes"));
    }

    #[tokio::test]
    async fn test_uninstall_apex_module_name_fails() {
        let temp_dirs = TempDirs::new();
        let mut mock_services = MockServices::new();
        let module_name = String::from("any.name");
        let version_code = 1;
        let module_path = String::from("some/random/path");
        fake_active_apex(
            &mut mock_services,
            String::from("other.module.name"),
            version_code,
            module_path.clone(),
        );

        let (ctx, init) = TestContext::new(temp_dirs.as_temp_paths(), mock_services);
        let service_impl = UpdateManagerServiceImpl::new_with_paths(
            init,
            temp_dirs.ota_package.path(),
            &ctx.save_state_path,
        )
        .await
        .unwrap();

        let apex_uninstall_request = make_uninstall_apex_request(module_name, version_code);

        let result = UpdateManagerService::UninstallApex(
            &service_impl,
            make_service_fqin(),
            &apex_uninstall_request,
        )
        .await;

        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("Failed to find active apex"));
    }

    #[tokio::test]
    async fn test_uninstall_apex_version_code_fails() {
        let temp_dirs = TempDirs::new();
        let mut mock_services = MockServices::new();
        let module_name = String::from("any.name");
        let version_code = 1;
        let module_path = String::from("some/random/path");
        fake_active_apex(&mut mock_services, module_name.clone(), 100, module_path.clone());

        let (ctx, init) = TestContext::new(temp_dirs.as_temp_paths(), mock_services);
        let service_impl = UpdateManagerServiceImpl::new_with_paths(
            init,
            temp_dirs.ota_package.path(),
            &ctx.save_state_path,
        )
        .await
        .unwrap();

        let apex_uninstall_request = make_uninstall_apex_request(module_name, version_code);

        let result = UpdateManagerService::UninstallApex(
            &service_impl,
            make_service_fqin(),
            &apex_uninstall_request,
        )
        .await;

        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("Failed to find active version"));
    }

    #[tokio::test]
    async fn test_activate_post_reboot_complete_check_system_update() {
        let temp_dirs = TempDirs::new();
        let temp_paths = temp_dirs.as_temp_paths();
        let ota_package_path = temp_paths.ota_package.clone();
        {
            let mut mock_services = MockServices::new();
            mock_services
                .boot_control_service
                .expect_get_current_slot()
                .times(1)
                .returning(|| Ok(/* initial_boot_slot=*/ 1));
            mock_services
                .power_state_client
                .expect_power_state_report_processing_complete()
                .times(1)
                .returning(|| Ok(()));

            let (mut ctx, init) = TestContext::new(temp_paths.clone(), mock_services);

            let service_impl = UpdateManagerServiceImpl::new_with_paths(
                init,
                &ota_package_path,
                &ctx.save_state_path,
            )
            .await
            .unwrap();

            let requested_system_update_payload = make_system_update_payload();

            subscribe_and_wait_for_state(&mut ctx, &service_impl, ApiUpdateState::READY).await;

            UpdateManagerService::Prepare(
                &service_impl,
                make_service_fqin(),
                &PrepareRequest {
                    payload: Some(PrepareRequestPayload::SystemUpdatePayload(
                        requested_system_update_payload.clone(),
                    )),
                    ..Default::default()
                },
            )
            .await
            .expect("Failure when calling UpdateManagerService::Prepare");

            assert_prepare_success(&mut ctx).await;

            let result = UpdateManagerService::Activate(
                &service_impl,
                make_service_fqin(),
                &ActivateRequest::default(),
            )
            .await;

            let update_state_start =
                wait_for_successful_callback(&mut ctx.callback_receiver, STATE_CHANGE_TIMEOUT)
                    .await;
            let update_progress =
                wait_for_successful_callback(&mut ctx.callback_receiver, STATE_CHANGE_TIMEOUT)
                    .await;
            let update_state_end =
                wait_for_successful_callback(&mut ctx.callback_receiver, STATE_CHANGE_TIMEOUT)
                    .await;
            assert!(result.is_ok());

            if let StatusUpdate::UpdateManagerStatus(update_manager_status) = update_state_start {
                assert_eq!(
                    update_manager_status.update_state.enum_value().unwrap(),
                    ApiUpdateState::ACTIVATE_PRE_REBOOT
                );
            } else {
                panic!("Unexpected StatusUpdate: {update_state_start:?}");
            }

            if let StatusUpdate::UpdateProgress(update_progress) = update_progress {
                assert_eq!(update_progress.status_code, IDLE);
                assert_eq!(update_progress.percentage, STATUS_UPDATE_PERCENTAGE_ACTIVE);
            } else {
                panic!("Unexpected StatusUpdate: {update_progress:?}");
            }

            if let StatusUpdate::UpdateManagerStatus(update_manager_status) = update_state_end {
                assert_eq!(
                    update_manager_status.update_state.enum_value().unwrap(),
                    ApiUpdateState::ACTIVATE_PRE_REBOOT_COMPLETE
                );
            } else {
                panic!("Unexpected StatusUpdate: {update_state_end:?}");
            }
        }

        let ota_package_path = temp_paths.ota_package.clone();
        {
            let mut new_mock_services = MockServices::new();
            new_mock_services
                .boot_control_service
                .expect_get_current_slot()
                .times(1)
                .returning(|| Ok(0));

            new_mock_services
                .boot_control_service
                .expect_is_slot_marked_successful()
                .times(1)
                .returning(|_| Ok(false));

            let (ctx, init) = TestContext::new(temp_paths.clone(), new_mock_services);

            UpdateManagerServiceImpl::new_with_paths(init, &ota_package_path, &ctx.save_state_path)
                .await
                .unwrap();
        }

        let ota_package_path = temp_paths.ota_package.clone();
        {
            let mut new_mock_services = MockServices::new();
            new_mock_services
                .boot_control_service
                .expect_get_current_slot()
                .times(1)
                .returning(|| Ok(0));

            let (ctx, init) = TestContext::new(temp_paths.clone(), new_mock_services);

            let service_impl = UpdateManagerServiceImpl::new_with_paths(
                init,
                &ota_package_path,
                &ctx.save_state_path,
            )
            .await
            .unwrap();

            let status_result = UpdateManagerService::GetStatus(
                &service_impl,
                make_service_fqin(),
                &GetStatusRequest::default(),
            )
            .await
            .unwrap();

            assert_eq!(
                status_result.update_manager_status.update_state.unwrap(),
                ApiUpdateState::ACTIVATE_POST_REBOOT_COMPLETE
            );
        }
    }

    #[tokio::test]
    async fn test_activate_post_reboot_complete_check_service_bundle_update() {
        use apex_aidl_interface::aidl::android::apex::ApexSessionInfo::ApexSessionInfo;
        let temp_dirs = TempDirs::new();
        let temp_paths = temp_dirs.as_temp_paths();
        let ota_package_path = temp_paths.ota_package.clone();
        const EXPECTED_SESSION_ID: i32 = 1;
        let expected_child_session_ids = vec![2];

        {
            let mut mock_services = MockServices::new();
            mock_services
                .apex_service
                .expect_submit_staged_session()
                .times(1)
                .withf(move |session_id, child_session_ids| {
                    *session_id == EXPECTED_SESSION_ID
                        && child_session_ids == expected_child_session_ids
                })
                .returning(|_, _| Ok(make_fake_apex_info_list()));

            mock_services
                .apex_service
                .expect_mark_staged_session_ready()
                .times(1)
                .withf(move |session_id| *session_id == EXPECTED_SESSION_ID)
                .returning(|_| Ok(()));

            mock_services
                .vold_service
                .expect_start_checkpoint()
                .times(1)
                .withf(move |retry| *retry == SERVICE_BUNDLE_UPDATE_RETRIES)
                .returning(|_| Ok(()));

            let (mut ctx, init) = TestContext::new(temp_paths.clone(), mock_services);

            let service_impl = UpdateManagerServiceImpl::new_with_paths(
                init,
                &ota_package_path,
                &ctx.save_state_path,
            )
            .await
            .unwrap();

            let service_bundle_dir = tempdir().unwrap();

            let requested_service_bundle_update_payload = make_service_bundle_update_payload(
                service_bundle_dir.path(),
                SignedApexType::ValidSignedApex,
                SERVICE_BUNDLE_UPDATE_RETRIES,
            );

            subscribe_and_wait_for_state(&mut ctx, &service_impl, ApiUpdateState::READY).await;

            UpdateManagerService::Prepare(
                &service_impl,
                make_service_fqin(),
                &PrepareRequest {
                    payload: Some(PrepareRequestPayload::ServiceBundleUpdatePayload(
                        requested_service_bundle_update_payload.clone().into(),
                    )),
                    ..Default::default()
                },
            )
            .await
            .expect("Failure when calling UpdateManagerService::Prepare");

            wait_for_successful_callback(&mut ctx.callback_receiver, STATE_CHANGE_TIMEOUT).await;
            wait_for_successful_callback(&mut ctx.callback_receiver, STATE_CHANGE_TIMEOUT).await;

            let result = UpdateManagerService::Activate(
                &service_impl,
                make_service_fqin(),
                &ActivateRequest::default(),
            )
            .await;

            // Wait for the activate pre-reboot step to finish
            let update_state_start =
                wait_for_successful_callback(&mut ctx.callback_receiver, STATE_CHANGE_TIMEOUT)
                    .await;
            let update_state_end =
                wait_for_successful_callback(&mut ctx.callback_receiver, STATE_CHANGE_TIMEOUT)
                    .await;
            assert!(result.is_ok());

            if let StatusUpdate::UpdateManagerStatus(update_manager_status) = update_state_start {
                assert_eq!(
                    update_manager_status.update_state.enum_value().unwrap(),
                    ApiUpdateState::ACTIVATE_PRE_REBOOT
                );
            } else {
                panic!("Unexpected StatusUpdate: {update_state_start:?}");
            }

            if let StatusUpdate::UpdateManagerStatus(update_manager_status) = update_state_end {
                assert_eq!(
                    update_manager_status.update_state.enum_value().unwrap(),
                    ApiUpdateState::ACTIVATE_PRE_REBOOT_COMPLETE
                );
            } else {
                panic!("Unexpected StatusUpdate: {update_state_end:?}");
            }
        }

        let ota_package_path = temp_paths.ota_package.clone();
        {
            let mut new_mock_services = MockServices::new();

            new_mock_services
                .apex_service
                .expect_get_staged_session_info()
                .times(1)
                .withf(|&session_id| session_id == EXPECTED_SESSION_ID)
                .returning(|_| {
                    Ok(ApexSessionInfo {
                        sessionId: EXPECTED_SESSION_ID,
                        isActivated: true,
                        ..Default::default()
                    })
                });

            let (ctx, init) = TestContext::new(temp_paths.clone(), new_mock_services);

            let _service_impl = UpdateManagerServiceImpl::new_with_paths(
                init,
                &ota_package_path,
                &ctx.save_state_path,
            )
            .await
            .unwrap();
        }

        let ota_package_path = temp_paths.ota_package.clone();
        {
            let mut new_mock_services = MockServices::new();

            new_mock_services
                .apex_service
                .expect_get_active_packages()
                .times(1)
                .returning(|| Ok(make_fake_apex_service_apex_info()));

            let (ctx, init) = TestContext::new(temp_paths.clone(), new_mock_services);

            let service_impl = UpdateManagerServiceImpl::new_with_paths(
                init,
                &ota_package_path,
                &ctx.save_state_path,
            )
            .await
            .unwrap();

            let status_result = UpdateManagerService::GetStatus(
                &service_impl,
                make_service_fqin(),
                &GetStatusRequest::default(),
            )
            .await
            .unwrap();

            assert_eq!(
                status_result.update_manager_status.update_state.unwrap(),
                ApiUpdateState::ACTIVATE_POST_REBOOT_COMPLETE
            );
        }
    }

    #[tokio::test]
    async fn test_activate_post_reboot_rollback_complete_system_update_success_recovery() {
        let temp_dirs = TempDirs::new();
        let temp_paths = temp_dirs.as_temp_paths();
        let ota_package_path = temp_paths.ota_package.clone();

        // Scope 1: Pre-Reboot Activation
        {
            let mut mock_services = MockServices::new();
            mock_services
                .boot_control_service
                .expect_get_current_slot()
                .times(1)
                .returning(|| Ok(1));

            mock_services
                .power_state_client
                .expect_power_state_report_processing_complete()
                .times(1)
                .returning(|| Ok(()));

            let (mut ctx, init) = TestContext::new(temp_paths.clone(), mock_services);
            let service_impl = UpdateManagerServiceImpl::new_with_paths(
                init,
                &ota_package_path,
                &ctx.save_state_path,
            )
            .await
            .unwrap();

            let requested_system_update_payload = make_system_update_payload();

            subscribe_and_wait_for_state(&mut ctx, &service_impl, ApiUpdateState::READY).await;

            UpdateManagerService::Prepare(
                &service_impl,
                make_service_fqin(),
                &PrepareRequest {
                    payload: Some(PrepareRequestPayload::SystemUpdatePayload(
                        requested_system_update_payload.clone(),
                    )),
                    ..Default::default()
                },
            )
            .await
            .expect("Failure when calling UpdateManagerService::Prepare");

            assert_prepare_success(&mut ctx).await;

            UpdateManagerService::Activate(
                &service_impl,
                make_service_fqin(),
                &ActivateRequest::default(),
            )
            .await
            .expect("Failure when calling UpdateManagerService::Activate");

            wait_for_successful_callback(&mut ctx.callback_receiver, STATE_CHANGE_TIMEOUT).await;
            wait_for_successful_callback(&mut ctx.callback_receiver, STATE_CHANGE_TIMEOUT).await;
            wait_for_successful_callback(&mut ctx.callback_receiver, STATE_CHANGE_TIMEOUT).await;

            let status_result = UpdateManagerService::GetStatus(
                &service_impl,
                make_service_fqin(),
                &GetStatusRequest::default(),
            )
            .await
            .unwrap();

            assert_eq!(
                status_result.update_manager_status.update_state.unwrap(),
                ApiUpdateState::ACTIVATE_PRE_REBOOT_COMPLETE
            );
        }
        // --- Scope 2: Post-Reboot Activation and Trigger Rollback
        let ota_package_path = temp_paths.ota_package.clone();
        {
            let mut mock_services_post_reboot_activate = MockServices::new();

            // new_slot = 0 (system successfully booted to new slot)
            mock_services_post_reboot_activate
                .boot_control_service
                .expect_get_current_slot()
                .times(1)
                .returning(|| Ok(0));

            mock_services_post_reboot_activate
                .boot_control_service
                .expect_is_slot_marked_successful()
                .withf(|&slot| slot == 0)
                .times(1)
                .returning(|_| Ok(false));

            // Rollback to initial_slot = 1
            mock_services_post_reboot_activate
                .boot_control_service
                .expect_revert_to_slot()
                .withf(|&slot| slot == 1)
                .times(1)
                .returning(|_| Ok(()));

            let (ctx, init) =
                TestContext::new(temp_paths.clone(), mock_services_post_reboot_activate);

            let service_impl_post_reboot = UpdateManagerServiceImpl::new_with_paths(
                init,
                &ota_package_path,
                &ctx.save_state_path,
            )
            .await
            .unwrap();

            let status_pre_rollback = UpdateManagerService::GetStatus(
                &service_impl_post_reboot,
                make_service_fqin(),
                &GetStatusRequest::default(),
            )
            .await
            .unwrap();

            assert_eq!(
                status_pre_rollback.update_manager_status.update_state.unwrap(),
                ApiUpdateState::ACTIVATE_POST_REBOOT_COMPLETE
            );

            UpdateManagerService::Rollback(
                &service_impl_post_reboot,
                make_service_fqin(),
                &RollbackRequest::default(),
            )
            .await
            .expect("Failure when calling UpdateManagerService::Rollback");

            let status_post_rollback = UpdateManagerService::GetStatus(
                &service_impl_post_reboot,
                make_service_fqin(),
                &GetStatusRequest::default(),
            )
            .await
            .unwrap();

            assert_eq!(
                status_post_rollback.update_manager_status.update_state.unwrap(),
                ApiUpdateState::ACTIVATE_POST_REBOOT_ROLLBACK_COMPLETE
            );

            let fake_update_engine_test_details =
                ctx.fake_update_engine_test_details.lock().unwrap();
            assert!(fake_update_engine_test_details.reset_status_called);

            // At this point, the state is ACTIVATE_POST_REBOOT_ROLLBACK_COMPLETE.
            // Persisting this state here, and allowing 'service_impl_post_reboot' to go out of scope,
            // simulates the update manager service shutting down after completing its rollback actions.
            // The next scope will simulate its restart after the VM Reboot.
        } // End of Scope 2

        // Scope 3: Post-Rollback Reboot/Restart to test recovery logic
        let ota_package_path = temp_paths.ota_package.clone();
        {
            let mut mock_services_restart = MockServices::new();

            // initial_slot = 1
            mock_services_restart
                .boot_control_service
                .expect_get_current_slot()
                .times(1)
                .returning(|| Ok(1));

            let (ctx, init) = TestContext::new(temp_paths.clone(), mock_services_restart);

            let service_impl_final = UpdateManagerServiceImpl::new_with_paths(
                init,
                &ota_package_path,
                &ctx.save_state_path,
            )
            .await
            .unwrap();

            let status_final_result = UpdateManagerService::GetStatus(
                &service_impl_final,
                make_service_fqin(),
                &GetStatusRequest::default(),
            )
            .await
            .unwrap();

            assert_eq!(
                status_final_result.update_manager_status.update_state.unwrap(),
                ApiUpdateState::READY
            );
        } // End of Scope 3
    }

    #[tokio::test]
    async fn test_activate_post_reboot_rollback_complete_system_update_resume_state() {
        let temp_dirs = TempDirs::new();
        let temp_paths = temp_dirs.as_temp_paths();
        let ota_package_path = temp_paths.ota_package.clone();

        // Scope 1: Pre-Reboot Activation (UNCHANGED from success_recovery test)
        {
            let mut mock_services = MockServices::new();
            mock_services
                .boot_control_service
                .expect_get_current_slot()
                .times(1)
                .returning(|| Ok(1)); // Hardcoded initial_slot = 1

            mock_services
                .power_state_client
                .expect_power_state_report_processing_complete()
                .times(1)
                .returning(|| Ok(()));

            let (mut ctx, init) = TestContext::new(temp_paths.clone(), mock_services);
            let service_impl = UpdateManagerServiceImpl::new_with_paths(
                init,
                &ota_package_path,
                &ctx.save_state_path,
            )
            .await
            .unwrap();

            let requested_system_update_payload = make_system_update_payload(); // System update

            subscribe_and_wait_for_state(&mut ctx, &service_impl, ApiUpdateState::READY).await;

            UpdateManagerService::Prepare(
                &service_impl,
                make_service_fqin(),
                &PrepareRequest {
                    payload: Some(PrepareRequestPayload::SystemUpdatePayload(
                        requested_system_update_payload.clone(),
                    )),
                    ..Default::default()
                },
            )
            .await
            .expect("Failure when calling UpdateManagerService::Prepare");

            assert_prepare_success(&mut ctx).await;

            UpdateManagerService::Activate(
                &service_impl,
                make_service_fqin(),
                &ActivateRequest::default(),
            )
            .await
            .expect("Failure when calling UpdateManagerService::Activate");

            wait_for_successful_callback(&mut ctx.callback_receiver, STATE_CHANGE_TIMEOUT).await;
            wait_for_successful_callback(&mut ctx.callback_receiver, STATE_CHANGE_TIMEOUT).await;

            let status_result = UpdateManagerService::GetStatus(
                &service_impl,
                make_service_fqin(),
                &GetStatusRequest::default(),
            )
            .await
            .unwrap();

            assert_eq!(
                status_result.update_manager_status.update_state.unwrap(),
                ApiUpdateState::ACTIVATE_PRE_REBOOT_COMPLETE
            );
            // State is implicitly saved here (ACTIVATE_PRE_REBOOT_COMPLETE)
        } // End of Scope 1

        // Scope 2: Post-Reboot Activation and Trigger Rollback (UNCHANGED from success_recovery test)
        let ota_package_path = temp_paths.ota_package.clone();
        {
            let mut mock_services_post_reboot_activate = MockServices::new();

            // Mocks for initial service start in post-reboot:
            mock_services_post_reboot_activate
                .boot_control_service
                .expect_get_current_slot()
                .times(1)
                .returning(|| Ok(0)); // Successfully booted to new slot (0)

            mock_services_post_reboot_activate
                .boot_control_service
                .expect_is_slot_marked_successful()
                .withf(|&slot| slot == 0)
                .times(1)
                .returning(|_| Ok(false));

            // Mocks for Rollback process:
            mock_services_post_reboot_activate
                .boot_control_service
                .expect_revert_to_slot()
                .withf(|&slot| slot == 1) // Revert to initial slot (1)
                .times(1)
                .returning(|_| Ok(()));

            let (ctx, init) = TestContext::new(
                temp_paths.clone(), // Use original temp_dirs for ctx creation
                mock_services_post_reboot_activate,
            );

            let service_impl_post_reboot = UpdateManagerServiceImpl::new_with_paths(
                init,
                &ota_package_path,
                &ctx.save_state_path,
            )
            .await
            .unwrap();

            // At this point, the service should be in ACTIVATE_POST_REBOOT_COMPLETE
            let status_pre_rollback = UpdateManagerService::GetStatus(
                &service_impl_post_reboot,
                make_service_fqin(),
                &GetStatusRequest::default(),
            )
            .await
            .unwrap();

            assert_eq!(
                status_pre_rollback.update_manager_status.update_state.unwrap(),
                ApiUpdateState::ACTIVATE_POST_REBOOT_COMPLETE
            );

            // Trigger Rollback for System Update
            UpdateManagerService::Rollback(
                &service_impl_post_reboot,
                make_service_fqin(),
                &RollbackRequest::default(),
            )
            .await
            .expect("Failure when calling UpdateManagerService::Rollback");

            // The state should now be ACTIVATE_POST_REBOOT_ROLLBACK_COMPLETE
            let status_post_rollback = UpdateManagerService::GetStatus(
                &service_impl_post_reboot,
                make_service_fqin(),
                &GetStatusRequest::default(),
            )
            .await
            .unwrap();

            assert_eq!(
                status_post_rollback.update_manager_status.update_state.unwrap(),
                ApiUpdateState::ACTIVATE_POST_REBOOT_ROLLBACK_COMPLETE
            );

            let fake_update_engine_test_details =
                ctx.fake_update_engine_test_details.lock().unwrap();
            assert!(fake_update_engine_test_details.reset_status_called);
            // State is implicitly persisted here (ACTIVATE_POST_REBOOT_ROLLBACK_COMPLETE)
        } // End of Scope 2

        // Scope 3: Post-Rollback Reboot/Restart to test recovery logic (MODIFIED)
        let ota_package_path = temp_paths.ota_package.clone();
        {
            let mut mock_services_restart = MockServices::new();

            // ********************************************************************************
            // Mocks for `check_system_update_status_and_recover` for ActivatePostRebootRollbackComplete
            // We want `current_slot != initial_slot` to be TRUE to hit the `None` branch.
            // ********************************************************************************

            // Mock get_current_slot to return a slot *different* from initial_slot (1)
            // This makes `current_slot != initial_slot` true (0 != 1)
            mock_services_restart
                .boot_control_service
                .expect_get_current_slot()
                .times(1)
                .returning(|| Ok(0)); // Hardcoded 0, different from 1

            let (ctx, init) = TestContext::new(temp_paths.clone(), mock_services_restart);

            let service_impl_final = UpdateManagerServiceImpl::new_with_paths(
                init,
                &ota_package_path,
                &ctx.save_state_path,
            )
            .await
            .unwrap();

            let status_final_result = UpdateManagerService::GetStatus(
                &service_impl_final,
                make_service_fqin(),
                &GetStatusRequest::default(),
            )
            .await
            .unwrap();

            // ********************************************************************************
            // ASSERTION: Expected state should remain ACTIVATE_POST_REBOOT_ROLLBACK_COMPLETE
            // ********************************************************************************
            assert_eq!(
                status_final_result.update_manager_status.update_state.unwrap(),
                ApiUpdateState::ACTIVATE_POST_REBOOT_ROLLBACK_COMPLETE
            );
        } // End of Scope 3
    }

    #[tokio::test]
    async fn test_activate_post_reboot_rollback_complete_system_update_get_current_slot_error() {
        let temp_dirs = TempDirs::new();
        let temp_paths = temp_dirs.as_temp_paths();
        let ota_package_path = temp_paths.ota_package.clone();

        // Scope 1: Pre-Reboot Activation
        {
            let mut mock_services = MockServices::new();
            mock_services
                .boot_control_service
                .expect_get_current_slot()
                .times(1)
                .returning(|| Ok(1));

            mock_services
                .power_state_client
                .expect_power_state_report_processing_complete()
                .times(1)
                .returning(|| Ok(()));

            let (mut ctx, init) = TestContext::new(temp_paths.clone(), mock_services);
            let service_impl = UpdateManagerServiceImpl::new_with_paths(
                init,
                &ota_package_path,
                &ctx.save_state_path,
            )
            .await
            .unwrap();

            let requested_system_update_payload = make_system_update_payload();

            subscribe_and_wait_for_state(&mut ctx, &service_impl, ApiUpdateState::READY).await;

            UpdateManagerService::Prepare(
                &service_impl,
                make_service_fqin(),
                &PrepareRequest {
                    payload: Some(PrepareRequestPayload::SystemUpdatePayload(
                        requested_system_update_payload.clone(),
                    )),
                    ..Default::default()
                },
            )
            .await
            .expect("Failure when calling UpdateManagerService::Prepare");

            assert_prepare_success(&mut ctx).await;

            UpdateManagerService::Activate(
                &service_impl,
                make_service_fqin(),
                &ActivateRequest::default(),
            )
            .await
            .expect("Failure when calling UpdateManagerService::Activate");

            wait_for_successful_callback(&mut ctx.callback_receiver, STATE_CHANGE_TIMEOUT).await;
            wait_for_successful_callback(&mut ctx.callback_receiver, STATE_CHANGE_TIMEOUT).await;
            wait_for_successful_callback(&mut ctx.callback_receiver, STATE_CHANGE_TIMEOUT).await;

            let status_result = UpdateManagerService::GetStatus(
                &service_impl,
                make_service_fqin(),
                &GetStatusRequest::default(),
            )
            .await
            .unwrap();

            assert_eq!(
                status_result.update_manager_status.update_state.unwrap(),
                ApiUpdateState::ACTIVATE_PRE_REBOOT_COMPLETE
            );
        }
        // Scope 2: Post-Reboot Activation and Trigger Rollback
        let ota_package_path = temp_paths.ota_package.clone();
        {
            let mut mock_services_post_reboot_activate = MockServices::new();

            // new_slot = 0 (system successfully booted to new slot)
            mock_services_post_reboot_activate
                .boot_control_service
                .expect_get_current_slot()
                .times(1)
                .returning(|| Ok(0));

            mock_services_post_reboot_activate
                .boot_control_service
                .expect_is_slot_marked_successful()
                .withf(|&slot| slot == 0)
                .times(1)
                .returning(|_| Ok(false));

            // Rollback to initial_slot = 1
            mock_services_post_reboot_activate
                .boot_control_service
                .expect_revert_to_slot()
                .withf(|&slot| slot == 1)
                .times(1)
                .returning(|_| Ok(()));

            let (ctx, init) =
                TestContext::new(temp_paths.clone(), mock_services_post_reboot_activate);
            let service_impl_post_reboot = UpdateManagerServiceImpl::new_with_paths(
                init,
                &ota_package_path,
                &ctx.save_state_path,
            )
            .await
            .unwrap();

            // At this point, service_impl_post_reboot should be in ACTIVATE_POST_REBOOT_COMPLETE
            // due to loading the saved state and successful check_system_update_status_and_recover.
            let status_pre_rollback = UpdateManagerService::GetStatus(
                &service_impl_post_reboot,
                make_service_fqin(),
                &GetStatusRequest::default(),
            )
            .await
            .unwrap();

            assert_eq!(
                status_pre_rollback.update_manager_status.update_state.unwrap(),
                ApiUpdateState::ACTIVATE_POST_REBOOT_COMPLETE
            );

            UpdateManagerService::Rollback(
                &service_impl_post_reboot,
                make_service_fqin(),
                &RollbackRequest::default(),
            )
            .await
            .expect("Failure when calling UpdateManagerService::Rollback");

            let status_post_rollback = UpdateManagerService::GetStatus(
                &service_impl_post_reboot,
                make_service_fqin(),
                &GetStatusRequest::default(),
            )
            .await
            .unwrap();

            assert_eq!(
                status_post_rollback.update_manager_status.update_state.unwrap(),
                ApiUpdateState::ACTIVATE_POST_REBOOT_ROLLBACK_COMPLETE
            );

            let fake_update_engine_test_details =
                ctx.fake_update_engine_test_details.lock().unwrap();
            assert!(fake_update_engine_test_details.reset_status_called);

            // At this point, the state is ACTIVATE_POST_REBOOT_ROLLBACK_COMPLETE.
            // Persisting this state here, and allowing 'service_impl_post_reboot' to go out of scope,
            // simulates the update manager service shutting down after completing its rollback actions.
            // The next scope will simulate its restart after the VM Reboot from the diagram.
        } // End of Scope 2

        // Scope 3: Post-Rollback Reboot/Restart to test recovery logic
        let ota_package_path = temp_paths.ota_package.clone();
        {
            let mut mock_services_restart = MockServices::new();

            mock_services_restart
                .boot_control_service
                .expect_get_current_slot()
                .times(1)
                .returning(|| {
                    Err(SdvStatus::with_message(
                        SdvStatusCode::Internal,
                        "IBootControl::getCurrentSlot() failed".to_string(),
                    ))
                });

            let (ctx, init) = TestContext::new(temp_paths.clone(), mock_services_restart);

            let service_impl_final = UpdateManagerServiceImpl::new_with_paths(
                init,
                &ota_package_path,
                &ctx.save_state_path,
            )
            .await
            .unwrap();

            let status_final_result = UpdateManagerService::GetStatus(
                &service_impl_final,
                make_service_fqin(),
                &GetStatusRequest::default(),
            )
            .await
            .unwrap();

            assert_eq!(
                status_final_result.update_manager_status.update_state.unwrap(),
                ApiUpdateState::ACTIVATE_POST_REBOOT_ROLLBACK_COMPLETE
            );
        }
    }

    #[tokio::test]
    async fn test_activate_post_reboot_rollback_complete_system_update_different_slot_recovery() {
        let temp_dirs = TempDirs::new();
        let temp_paths = temp_dirs.as_temp_paths();
        let ota_package_path = temp_paths.ota_package.clone();

        // Scope 1: Pre-Reboot Activation
        {
            let mut mock_services = MockServices::new();
            mock_services
                .boot_control_service
                .expect_get_current_slot()
                .times(1)
                .returning(|| Ok(1));

            mock_services
                .power_state_client
                .expect_power_state_report_processing_complete()
                .times(1)
                .returning(|| Ok(()));

            let (mut ctx, init) = TestContext::new(temp_paths.clone(), mock_services);
            let service_impl = UpdateManagerServiceImpl::new_with_paths(
                init,
                &ota_package_path,
                &ctx.save_state_path,
            )
            .await
            .unwrap();

            let requested_system_update_payload = make_system_update_payload();

            subscribe_and_wait_for_state(&mut ctx, &service_impl, ApiUpdateState::READY).await;

            UpdateManagerService::Prepare(
                &service_impl,
                make_service_fqin(),
                &PrepareRequest {
                    payload: Some(PrepareRequestPayload::SystemUpdatePayload(
                        requested_system_update_payload.clone(),
                    )),
                    ..Default::default()
                },
            )
            .await
            .expect("Failure when calling UpdateManagerService::Prepare");

            // PREPARE
            wait_for_successful_callback(&mut ctx.callback_receiver, STATE_CHANGE_TIMEOUT).await;
            // PREPARE_COMPLETE
            wait_for_successful_callback(&mut ctx.callback_receiver, STATE_CHANGE_TIMEOUT).await;
            // ACTIVATE_PRE_REBOOT
            wait_for_successful_callback(&mut ctx.callback_receiver, STATE_CHANGE_TIMEOUT).await;

            UpdateManagerService::Activate(
                &service_impl,
                make_service_fqin(),
                &ActivateRequest::default(),
            )
            .await
            .expect("Failure when calling UpdateManagerService::Activate");

            wait_for_successful_callback(&mut ctx.callback_receiver, STATE_CHANGE_TIMEOUT).await;
            wait_for_successful_callback(&mut ctx.callback_receiver, STATE_CHANGE_TIMEOUT).await;
            wait_for_successful_callback(&mut ctx.callback_receiver, STATE_CHANGE_TIMEOUT).await;
            let status_result = UpdateManagerService::GetStatus(
                &service_impl,
                make_service_fqin(),
                &GetStatusRequest::default(),
            )
            .await
            .unwrap();

            assert_eq!(
                status_result.update_manager_status.update_state.unwrap(),
                ApiUpdateState::ACTIVATE_PRE_REBOOT_COMPLETE
            );
            // State is ACTIVATE_PRE_REBOOT_COMPLETE and saved.
            // Service_impl goes out of scope, simulating first reboot.
        }

        // Scope 2: Post-Reboot Activation and Trigger Rollback
        let ota_package_path = temp_paths.ota_package.clone();
        {
            let mut mock_services_post_reboot_activate = MockServices::new();

            // new_slot = 0 (system successfully booted to new slot)
            mock_services_post_reboot_activate
                .boot_control_service
                .expect_get_current_slot()
                .times(1)
                .returning(|| Ok(0));

            mock_services_post_reboot_activate
                .boot_control_service
                .expect_is_slot_marked_successful()
                .withf(|&slot| slot == 0)
                .times(1)
                .returning(|_| Ok(false));

            // Rollback to initial_slot = 1
            mock_services_post_reboot_activate
                .boot_control_service
                .expect_revert_to_slot()
                .withf(|&slot| slot == 1)
                .times(1)
                .returning(|_| Ok(()));

            let (ctx, init) =
                TestContext::new(temp_paths.clone(), mock_services_post_reboot_activate);
            let service_impl_post_reboot = UpdateManagerServiceImpl::new_with_paths(
                init,
                &ota_package_path,
                &ctx.save_state_path,
            )
            .await
            .unwrap();

            // At this point, service_impl_post_reboot should be in ACTIVATE_POST_REBOOT_COMPLETE
            // due to loading the saved state and successful check_system_update_status_and_recover.
            let status_pre_rollback = UpdateManagerService::GetStatus(
                &service_impl_post_reboot,
                make_service_fqin(),
                &GetStatusRequest::default(),
            )
            .await
            .unwrap();

            assert_eq!(
                status_pre_rollback.update_manager_status.update_state.unwrap(),
                ApiUpdateState::ACTIVATE_POST_REBOOT_COMPLETE
            );

            UpdateManagerService::Rollback(
                &service_impl_post_reboot,
                make_service_fqin(),
                &RollbackRequest::default(),
            )
            .await
            .expect("Failure when calling UpdateManagerService::Rollback");

            let status_post_rollback = UpdateManagerService::GetStatus(
                &service_impl_post_reboot,
                make_service_fqin(),
                &GetStatusRequest::default(),
            )
            .await
            .unwrap();

            assert_eq!(
                status_post_rollback.update_manager_status.update_state.unwrap(),
                ApiUpdateState::ACTIVATE_POST_REBOOT_ROLLBACK_COMPLETE
            );

            let fake_update_engine_test_details =
                ctx.fake_update_engine_test_details.lock().unwrap();
            assert!(fake_update_engine_test_details.reset_status_called);
            // At this point, the state is ACTIVATE_POST_REBOOT_ROLLBACK_COMPLETE.
            // Persisting this state here, and allowing 'service_impl_post_reboot' to go out of scope,
            // simulates the update manager service shutting down after completing its rollback actions.
            // The next scope will simulate its restart after the VM Reboot from the diagram.
        } // End of Scope 2

        // Scope 3: Post-Rollback Reboot/Restart to test recovery logic
        let ota_package_path = temp_paths.ota_package.clone();
        {
            let mut mock_services_restart = MockServices::new();

            mock_services_restart
                .boot_control_service
                .expect_get_current_slot()
                .times(1)
                .returning(|| Ok(0));

            let (ctx, init) = TestContext::new(temp_paths.clone(), mock_services_restart);

            let service_impl_final = UpdateManagerServiceImpl::new_with_paths(
                init,
                &ota_package_path,
                &ctx.save_state_path,
            )
            .await
            .unwrap();

            let status_final_result = UpdateManagerService::GetStatus(
                &service_impl_final,
                make_service_fqin(),
                &GetStatusRequest::default(),
            )
            .await
            .unwrap();

            assert_eq!(
                status_final_result.update_manager_status.update_state.unwrap(),
                ApiUpdateState::ACTIVATE_POST_REBOOT_ROLLBACK_COMPLETE
            );
        }
    }

    #[tokio::test]
    async fn test_activate_post_reboot_rollback_complete_service_bundle_success_recovery() {
        use apex_aidl_interface::aidl::android::apex::ApexSessionInfo::ApexSessionInfo;

        let temp_dirs = TempDirs::new();
        let temp_paths = temp_dirs.as_temp_paths();
        let ota_package_path = temp_paths.ota_package.clone();
        const EXPECTED_SESSION_ID: i32 = 1;
        let expected_child_session_ids = vec![2];

        // Scope 1: Pre-Rollback Activation (Prepare -> Activate -> ACTIVATE_PRE_REBOOT_COMPLETE)
        {
            let mut mock_services = MockServices::new();

            mock_services
                .apex_service
                .expect_submit_staged_session()
                .times(1)
                .withf(move |session_id, child_session_ids| {
                    *session_id == EXPECTED_SESSION_ID
                        && child_session_ids == expected_child_session_ids
                })
                .returning(|_, _| Ok(make_fake_apex_info_list()));

            mock_services
                .apex_service
                .expect_mark_staged_session_ready()
                .times(1)
                .withf(move |session_id| *session_id == EXPECTED_SESSION_ID)
                .returning(|_| Ok(()));

            mock_services
                .vold_service
                .expect_start_checkpoint()
                .times(1)
                .withf(move |retry| *retry == SERVICE_BUNDLE_UPDATE_RETRIES)
                .returning(|_| Ok(()));

            let (mut ctx, init) = TestContext::new(temp_paths.clone(), mock_services);

            let service_impl = UpdateManagerServiceImpl::new_with_paths(
                init,
                &ota_package_path,
                &ctx.save_state_path,
            )
            .await
            .unwrap();

            let service_bundle_dir = tempdir().unwrap();
            let requested_service_bundle_update_payload = make_service_bundle_update_payload(
                service_bundle_dir.path(),
                SignedApexType::ValidSignedApex,
                SERVICE_BUNDLE_UPDATE_RETRIES,
            );

            subscribe_and_wait_for_state(&mut ctx, &service_impl, ApiUpdateState::READY).await;

            UpdateManagerService::Prepare(
                &service_impl,
                make_service_fqin(),
                &PrepareRequest {
                    payload: Some(PrepareRequestPayload::ServiceBundleUpdatePayload(
                        requested_service_bundle_update_payload.clone().into(),
                    )),
                    ..Default::default()
                },
            )
            .await
            .expect("Failure when calling UpdateManagerService::Prepare");

            wait_for_successful_callback(&mut ctx.callback_receiver, STATE_CHANGE_TIMEOUT).await;
            wait_for_successful_callback(&mut ctx.callback_receiver, STATE_CHANGE_TIMEOUT).await;

            UpdateManagerService::Activate(
                &service_impl,
                make_service_fqin(),
                &ActivateRequest::default(),
            )
            .await
            .expect("Failure when calling UpdateManagerService::Activate");

            // ACTIVATE_PRE_REBOOT
            wait_for_successful_callback(&mut ctx.callback_receiver, STATE_CHANGE_TIMEOUT).await;
            // ACTIVATE_PRE_REBOOT_COMPLETE
            wait_for_successful_callback(&mut ctx.callback_receiver, STATE_CHANGE_TIMEOUT).await;

            let status_result = UpdateManagerService::GetStatus(
                &service_impl,
                make_service_fqin(),
                &GetStatusRequest::default(),
            )
            .await
            .unwrap();

            assert_eq!(
                status_result.update_manager_status.update_state.unwrap(),
                ApiUpdateState::ACTIVATE_PRE_REBOOT_COMPLETE
            );
        }

        // Scope 2: Post-Reboot Activation & Trigger Rollback
        let ota_package_path = temp_paths.ota_package.clone();
        {
            let mut mock_services_post_reboot_activate = MockServices::new();

            mock_services_post_reboot_activate
                .vold_service
                .expect_start_checkpoint()
                .withf(|&retry| retry == 0)
                .times(1)
                .returning(|_| Ok(()));

            mock_services_post_reboot_activate
                .apex_service
                .expect_get_staged_session_info()
                .times(1)
                .withf(|&session_id| session_id == EXPECTED_SESSION_ID)
                .returning(|_| {
                    Ok(ApexSessionInfo {
                        sessionId: EXPECTED_SESSION_ID,
                        isActivated: true,
                        ..Default::default()
                    })
                });

            // Mocks for Rollback process:
            mock_services_post_reboot_activate
                .apex_service
                .expect_revert_active_sessions()
                .times(1)
                .returning(|| Ok(()));

            let (ctx, init) =
                TestContext::new(temp_paths.clone(), mock_services_post_reboot_activate);

            let service_impl_post_reboot = UpdateManagerServiceImpl::new_with_paths(
                init,
                &ota_package_path,
                &ctx.save_state_path,
            )
            .await
            .unwrap();

            // Confirm state is ACTIVATE_POST_REBOOT_COMPLETE
            let status_pre_rollback = UpdateManagerService::GetStatus(
                &service_impl_post_reboot,
                make_service_fqin(),
                &GetStatusRequest::default(),
            )
            .await
            .unwrap();

            assert_eq!(
                status_pre_rollback.update_manager_status.update_state.unwrap(),
                ApiUpdateState::ACTIVATE_POST_REBOOT_COMPLETE
            );

            // Trigger Rollback for Service Bundle
            UpdateManagerService::Rollback(
                &service_impl_post_reboot,
                make_service_fqin(),
                &RollbackRequest::default(),
            )
            .await
            .expect("Failure when calling UpdateManagerService::Rollback");

            // Confirm state is ACTIVATE_POST_REBOOT_ROLLBACK_COMPLETE
            let status_post_rollback = UpdateManagerService::GetStatus(
                &service_impl_post_reboot,
                make_service_fqin(),
                &GetStatusRequest::default(),
            )
            .await
            .unwrap();

            assert_eq!(
                status_post_rollback.update_manager_status.update_state.unwrap(),
                ApiUpdateState::ACTIVATE_POST_REBOOT_ROLLBACK_COMPLETE
            );

            // At this point, the state is ACTIVATE_POST_REBOOT_ROLLBACK_COMPLETE.
            // Persisting this state here, and allowing 'service_impl_post_reboot' to go out of scope,
            // simulates the update manager service shutting down after completing its rollback actions.
            // The next scope will simulate its restart after the VM Reboot.
        } // End of Scope 2

        // Scope 3: Post-Rollback Reboot/Restart to test recovery logic
        let ota_package_path = temp_paths.ota_package.clone();
        {
            let mut mock_services_restart = MockServices::new();

            mock_services_restart
                .apex_service
                .expect_get_active_packages()
                .times(1)
                .returning(|| Ok(make_fake_apex_service_apex_info()));

            let (ctx, init) = TestContext::new(temp_paths.clone(), mock_services_restart);

            let service_impl_final = UpdateManagerServiceImpl::new_with_paths(
                init,
                &ota_package_path,
                &ctx.save_state_path,
            )
            .await
            .unwrap();

            let status_final_result = UpdateManagerService::GetStatus(
                &service_impl_final,
                make_service_fqin(),
                &GetStatusRequest::default(),
            )
            .await
            .unwrap();

            assert_eq!(
                status_final_result.update_manager_status.update_state.unwrap(),
                ApiUpdateState::ACTIVATE_POST_REBOOT_ROLLBACK_COMPLETE
            );
        } // End of Scope 3
    }

    #[tokio::test]
    async fn test_activate_post_reboot_rollback_complete_service_bundle_ready_recovery() {
        use apex_aidl_interface::aidl::android::apex::ApexSessionInfo::ApexSessionInfo;

        let temp_dirs = TempDirs::new();
        let temp_paths = temp_dirs.as_temp_paths();
        let ota_package_path = temp_paths.ota_package.clone();
        const EXPECTED_SESSION_ID: i32 = 1;
        let expected_child_session_ids = vec![2];

        // Scope 1: Pre-Rollback Activation (Prepare -> Activate -> ACTIVATE_PRE_REBOOT_COMPLETE)
        {
            let mut mock_services = MockServices::new();

            mock_services
                .apex_service
                .expect_submit_staged_session()
                .times(1)
                .withf(move |session_id, child_session_ids| {
                    *session_id == EXPECTED_SESSION_ID
                        && child_session_ids == expected_child_session_ids
                })
                .returning(|_, _| Ok(make_fake_apex_info_list()));

            mock_services
                .apex_service
                .expect_mark_staged_session_ready()
                .times(1)
                .withf(move |session_id| *session_id == EXPECTED_SESSION_ID)
                .returning(|_| Ok(()));

            mock_services
                .vold_service
                .expect_start_checkpoint()
                .times(1)
                .withf(move |retry| *retry == SERVICE_BUNDLE_UPDATE_RETRIES)
                .returning(|_| Ok(()));

            let (mut ctx, init) = TestContext::new(temp_paths.clone(), mock_services);

            let service_impl = UpdateManagerServiceImpl::new_with_paths(
                init,
                &ota_package_path,
                &ctx.save_state_path,
            )
            .await
            .unwrap();

            let service_bundle_dir = tempdir().unwrap();
            let requested_service_bundle_update_payload = make_service_bundle_update_payload(
                service_bundle_dir.path(),
                SignedApexType::ValidSignedApex,
                SERVICE_BUNDLE_UPDATE_RETRIES,
            );

            subscribe_and_wait_for_state(&mut ctx, &service_impl, ApiUpdateState::READY).await;

            UpdateManagerService::Prepare(
                &service_impl,
                make_service_fqin(),
                &PrepareRequest {
                    payload: Some(PrepareRequestPayload::ServiceBundleUpdatePayload(
                        requested_service_bundle_update_payload.clone().into(),
                    )),
                    ..Default::default()
                },
            )
            .await
            .expect("Failure when calling UpdateManagerService::Prepare");

            // PREPARE
            wait_for_successful_callback(&mut ctx.callback_receiver, STATE_CHANGE_TIMEOUT).await;
            // PREPARE_COMPLETE
            wait_for_successful_callback(&mut ctx.callback_receiver, STATE_CHANGE_TIMEOUT).await;

            UpdateManagerService::Activate(
                &service_impl,
                make_service_fqin(),
                &ActivateRequest::default(),
            )
            .await
            .expect("Failure when calling UpdateManagerService::Activate");

            // ACTIVATE_PRE_REBOOT
            wait_for_successful_callback(&mut ctx.callback_receiver, STATE_CHANGE_TIMEOUT).await;
            // ACTIVATE_PRE_REBOOT_COMPLETE
            wait_for_successful_callback(&mut ctx.callback_receiver, STATE_CHANGE_TIMEOUT).await;

            let status_result = UpdateManagerService::GetStatus(
                &service_impl,
                make_service_fqin(),
                &GetStatusRequest::default(),
            )
            .await
            .unwrap();

            assert_eq!(
                status_result.update_manager_status.update_state.unwrap(),
                ApiUpdateState::ACTIVATE_PRE_REBOOT_COMPLETE
            );
        }

        // Scope 2: Post-Reboot Activation & Trigger Rollback
        let ota_package_path = temp_paths.ota_package.clone();
        {
            let mut mock_services_post_reboot_activate = MockServices::new();

            mock_services_post_reboot_activate
                .vold_service
                .expect_start_checkpoint()
                .withf(|&retry| retry == 0)
                .times(1)
                .returning(|_| Ok(()));

            mock_services_post_reboot_activate
                .apex_service
                .expect_get_staged_session_info()
                .times(1)
                .withf(|&session_id| session_id == EXPECTED_SESSION_ID)
                .returning(|_| {
                    Ok(ApexSessionInfo {
                        sessionId: EXPECTED_SESSION_ID,
                        isActivated: true,
                        ..Default::default()
                    })
                });

            // Mocks for Rollback process:
            mock_services_post_reboot_activate
                .apex_service
                .expect_revert_active_sessions()
                .times(1)
                .returning(|| Ok(()));

            let (ctx, init) =
                TestContext::new(temp_paths.clone(), mock_services_post_reboot_activate);

            let service_impl_post_reboot = UpdateManagerServiceImpl::new_with_paths(
                init,
                &ota_package_path,
                &ctx.save_state_path,
            )
            .await
            .unwrap();

            // Confirm state is ACTIVATE_POST_REBOOT_COMPLETE
            let status_pre_rollback = UpdateManagerService::GetStatus(
                &service_impl_post_reboot,
                make_service_fqin(),
                &GetStatusRequest::default(),
            )
            .await
            .unwrap();

            assert_eq!(
                status_pre_rollback.update_manager_status.update_state.unwrap(),
                ApiUpdateState::ACTIVATE_POST_REBOOT_COMPLETE
            );

            // Trigger Rollback for Service Bundle
            UpdateManagerService::Rollback(
                &service_impl_post_reboot,
                make_service_fqin(),
                &RollbackRequest::default(),
            )
            .await
            .expect("Failure when calling UpdateManagerService::Rollback");

            // Confirm state is ACTIVATE_POST_REBOOT_ROLLBACK_COMPLETE
            let status_post_rollback = UpdateManagerService::GetStatus(
                &service_impl_post_reboot,
                make_service_fqin(),
                &GetStatusRequest::default(),
            )
            .await
            .unwrap();

            assert_eq!(
                status_post_rollback.update_manager_status.update_state.unwrap(),
                ApiUpdateState::ACTIVATE_POST_REBOOT_ROLLBACK_COMPLETE
            );

            // At this point, the state is ACTIVATE_POST_REBOOT_ROLLBACK_COMPLETE.
            // Persisting this state here, and allowing 'service_impl_post_reboot' to go out of scope,
            // simulates the update manager service shutting down after completing its rollback actions.
            // The next scope will simulate its restart after the VM Reboot.
        } // End of Scope 2

        // Scope 3: Post-Rollback Reboot/Restart to test recovery logic
        let ota_package_path = temp_paths.ota_package.clone();
        {
            let mut mock_services_restart = MockServices::new();

            mock_services_restart.apex_service.expect_get_active_packages().times(1).returning(
                || {
                    Ok(vec![]) // Return an EMPTY vector. This ensures check_submitted_apexes_active returns false.
                },
            );

            let (ctx, init) = TestContext::new(temp_paths.clone(), mock_services_restart);

            let service_impl_final = UpdateManagerServiceImpl::new_with_paths(
                init,
                &ota_package_path,
                &ctx.save_state_path,
            )
            .await
            .unwrap();

            let status_final_result = UpdateManagerService::GetStatus(
                &service_impl_final,
                make_service_fqin(),
                &GetStatusRequest::default(),
            )
            .await
            .unwrap();

            assert_eq!(
                status_final_result.update_manager_status.update_state.unwrap(),
                ApiUpdateState::READY
            );
        } // End of Scope 3
    }

    #[tokio::test]
    async fn test_activate_pre_reboot_complete_check_system_update_get_current_slot_error() {
        let temp_dirs = TempDirs::new();
        let temp_paths = temp_dirs.as_temp_paths();
        let ota_package_path = temp_paths.ota_package.clone();
        {
            let mut mock_services = MockServices::new();
            mock_services
                .boot_control_service
                .expect_get_current_slot()
                .times(1)
                .returning(|| Ok(/* initial_boot_slot=*/ 1));
            mock_services
                .power_state_client
                .expect_power_state_report_processing_complete()
                .times(1)
                .returning(|| Ok(()));

            let (mut ctx, init) = TestContext::new(temp_paths.clone(), mock_services);

            let service_impl = UpdateManagerServiceImpl::new_with_paths(
                init,
                &ota_package_path,
                &ctx.save_state_path,
            )
            .await
            .unwrap();

            let requested_system_update_payload = make_system_update_payload();

            subscribe_and_wait_for_state(&mut ctx, &service_impl, ApiUpdateState::READY).await;

            UpdateManagerService::Prepare(
                &service_impl,
                make_service_fqin(),
                &PrepareRequest {
                    payload: Some(PrepareRequestPayload::SystemUpdatePayload(
                        requested_system_update_payload.clone(),
                    )),
                    ..Default::default()
                },
            )
            .await
            .expect("Failure when calling UpdateManagerService::Prepare");

            wait_for_successful_callback(&mut ctx.callback_receiver, STATE_CHANGE_TIMEOUT).await;
            wait_for_successful_callback(&mut ctx.callback_receiver, STATE_CHANGE_TIMEOUT).await;
            wait_for_successful_callback(&mut ctx.callback_receiver, STATE_CHANGE_TIMEOUT).await;

            let result = UpdateManagerService::Activate(
                &service_impl,
                make_service_fqin(),
                &ActivateRequest::default(),
            )
            .await;

            // Wait for the activate pre-reboot step to finish
            let update_state_start =
                wait_for_successful_callback(&mut ctx.callback_receiver, STATE_CHANGE_TIMEOUT)
                    .await;
            let update_progress =
                wait_for_successful_callback(&mut ctx.callback_receiver, STATE_CHANGE_TIMEOUT)
                    .await;
            let update_state_end =
                wait_for_successful_callback(&mut ctx.callback_receiver, STATE_CHANGE_TIMEOUT)
                    .await;
            assert!(result.is_ok());

            if let StatusUpdate::UpdateManagerStatus(update_manager_status) = update_state_start {
                assert_eq!(
                    update_manager_status.update_state.enum_value().unwrap(),
                    ApiUpdateState::ACTIVATE_PRE_REBOOT
                );
            } else {
                panic!("Unexpected StatusUpdate: {update_state_start:?}");
            }

            if let StatusUpdate::UpdateProgress(update_progress) = update_progress {
                assert_eq!(update_progress.status_code, IDLE);
                assert_eq!(update_progress.percentage, STATUS_UPDATE_PERCENTAGE_ACTIVE);
            } else {
                panic!("Unexpected StatusUpdate: {update_progress:?}");
            }

            if let StatusUpdate::UpdateManagerStatus(update_manager_status) = update_state_end {
                assert_eq!(
                    update_manager_status.update_state.enum_value().unwrap(),
                    ApiUpdateState::ACTIVATE_PRE_REBOOT_COMPLETE
                );
            } else {
                panic!("Unexpected StatusUpdate: {update_state_end:?}");
            }
        }

        let ota_package_path = temp_paths.ota_package.clone();
        {
            let mut new_mock_services = MockServices::new();
            new_mock_services.boot_control_service.expect_get_current_slot().times(1).returning(
                || {
                    Err(SdvStatus::with_message(
                        SdvStatusCode::Internal,
                        "IBootControl::getCurrentSlot() failed".to_string(),
                    ))
                },
            );

            let (ctx, init) = TestContext::new(temp_paths.clone(), new_mock_services);

            UpdateManagerServiceImpl::new_with_paths(init, &ota_package_path, &ctx.save_state_path)
                .await
                .unwrap();
        }

        let ota_package_path = temp_paths.ota_package.clone();
        {
            let new_mock_services = MockServices::new();

            let (ctx, init) = TestContext::new(temp_paths.clone(), new_mock_services);

            let service_impl = UpdateManagerServiceImpl::new_with_paths(
                init,
                &ota_package_path,
                &ctx.save_state_path,
            )
            .await
            .unwrap();

            let status_result = UpdateManagerService::GetStatus(
                &service_impl,
                make_service_fqin(),
                &GetStatusRequest::default(),
            )
            .await
            .unwrap();

            assert_eq!(
                status_result.update_manager_status.update_state.unwrap(),
                ApiUpdateState::ACTIVATE_PRE_REBOOT_FAILURE,
            );
        }
    }

    #[tokio::test]
    async fn test_activate_post_reboot_complete_check_system_update_get_curr_slot_err() {
        let temp_dirs = TempDirs::new();
        let temp_paths = temp_dirs.as_temp_paths();
        let ota_package_path = temp_paths.ota_package.clone();
        {
            let mut mock_services = MockServices::new();
            mock_services
                .boot_control_service
                .expect_get_current_slot()
                .times(1)
                .returning(|| Ok(/* initial_boot_slot=*/ 1));
            mock_services
                .power_state_client
                .expect_power_state_report_processing_complete()
                .times(1)
                .returning(|| Ok(()));

            let (mut ctx, init) = TestContext::new(temp_paths.clone(), mock_services);

            let service_impl = UpdateManagerServiceImpl::new_with_paths(
                init,
                &ota_package_path,
                &ctx.save_state_path,
            )
            .await
            .unwrap();

            let requested_system_update_payload = make_system_update_payload();

            subscribe_and_wait_for_state(&mut ctx, &service_impl, ApiUpdateState::READY).await;

            UpdateManagerService::Prepare(
                &service_impl,
                make_service_fqin(),
                &PrepareRequest {
                    payload: Some(PrepareRequestPayload::SystemUpdatePayload(
                        requested_system_update_payload.clone(),
                    )),
                    ..Default::default()
                },
            )
            .await
            .expect("Failure when calling UpdateManagerService::Prepare");

            wait_for_successful_callback(&mut ctx.callback_receiver, STATE_CHANGE_TIMEOUT).await;
            wait_for_successful_callback(&mut ctx.callback_receiver, STATE_CHANGE_TIMEOUT).await;
            wait_for_successful_callback(&mut ctx.callback_receiver, STATE_CHANGE_TIMEOUT).await;

            let result = UpdateManagerService::Activate(
                &service_impl,
                make_service_fqin(),
                &ActivateRequest::default(),
            )
            .await;

            // Wait for the activate pre-reboot step to finish
            let update_state_start =
                wait_for_successful_callback(&mut ctx.callback_receiver, STATE_CHANGE_TIMEOUT)
                    .await;
            let update_progress =
                wait_for_successful_callback(&mut ctx.callback_receiver, STATE_CHANGE_TIMEOUT)
                    .await;
            let update_state_end =
                wait_for_successful_callback(&mut ctx.callback_receiver, STATE_CHANGE_TIMEOUT)
                    .await;
            assert!(result.is_ok());

            if let StatusUpdate::UpdateManagerStatus(update_manager_status) = update_state_start {
                assert_eq!(
                    update_manager_status.update_state.enum_value().unwrap(),
                    ApiUpdateState::ACTIVATE_PRE_REBOOT
                );
            } else {
                panic!("Unexpected StatusUpdate: {update_state_start:?}");
            }

            if let StatusUpdate::UpdateProgress(update_progress) = update_progress {
                assert_eq!(update_progress.status_code, IDLE);
                assert_eq!(update_progress.percentage, STATUS_UPDATE_PERCENTAGE_ACTIVE);
            } else {
                panic!("Unexpected StatusUpdate: {update_progress:?}");
            }

            if let StatusUpdate::UpdateManagerStatus(update_manager_status) = update_state_end {
                assert_eq!(
                    update_manager_status.update_state.enum_value().unwrap(),
                    ApiUpdateState::ACTIVATE_PRE_REBOOT_COMPLETE
                );
            } else {
                panic!("Unexpected StatusUpdate: {update_state_end:?}");
            }
        }

        let ota_package_path = temp_paths.ota_package.clone();
        {
            let mut new_mock_services = MockServices::new();
            new_mock_services
                .boot_control_service
                .expect_get_current_slot()
                .times(1)
                .returning(|| Ok(0));

            new_mock_services
                .boot_control_service
                .expect_is_slot_marked_successful()
                .times(1)
                .returning(|_| Ok(false));

            let (ctx, init) = TestContext::new(temp_paths.clone(), new_mock_services);

            UpdateManagerServiceImpl::new_with_paths(init, &ota_package_path, &ctx.save_state_path)
                .await
                .unwrap();
        }

        let ota_package_path = temp_paths.ota_package.clone();
        {
            let mut new_mock_services = MockServices::new();
            new_mock_services.boot_control_service.expect_get_current_slot().times(1).returning(
                || {
                    Err(SdvStatus::with_message(
                        SdvStatusCode::Internal,
                        "IBootControl::getCurrentSlot() failed".to_string(),
                    ))
                },
            );

            let (ctx, init) = TestContext::new(temp_paths.clone(), new_mock_services);

            let service_impl = UpdateManagerServiceImpl::new_with_paths(
                init,
                &ota_package_path,
                &ctx.save_state_path,
            )
            .await
            .unwrap();

            let status_result = UpdateManagerService::GetStatus(
                &service_impl,
                make_service_fqin(),
                &GetStatusRequest::default(),
            )
            .await
            .unwrap();

            assert_eq!(
                status_result.update_manager_status.update_state.unwrap(),
                ApiUpdateState::ACTIVATE_POST_REBOOT_COMPLETE
            );
        }
    }

    #[tokio::test]
    async fn test_activate_post_reboot_complete_check_system_update_fails_same_slot() {
        let temp_dirs = TempDirs::new();
        let temp_paths = temp_dirs.as_temp_paths();
        let ota_package_path = temp_paths.ota_package.clone();
        {
            let mut mock_services = MockServices::new();
            mock_services
                .boot_control_service
                .expect_get_current_slot()
                .times(1)
                .returning(|| Ok(/* initial_boot_slot=*/ 1));
            mock_services
                .power_state_client
                .expect_power_state_report_processing_complete()
                .times(1)
                .returning(|| Ok(()));

            let (mut ctx, init) = TestContext::new(temp_paths.clone(), mock_services);

            let service_impl = UpdateManagerServiceImpl::new_with_paths(
                init,
                &ota_package_path,
                &ctx.save_state_path,
            )
            .await
            .unwrap();

            let requested_system_update_payload = make_system_update_payload();

            subscribe_and_wait_for_state(&mut ctx, &service_impl, ApiUpdateState::READY).await;

            UpdateManagerService::Prepare(
                &service_impl,
                make_service_fqin(),
                &PrepareRequest {
                    payload: Some(PrepareRequestPayload::SystemUpdatePayload(
                        requested_system_update_payload.clone(),
                    )),
                    ..Default::default()
                },
            )
            .await
            .unwrap();

            wait_for_successful_callback(&mut ctx.callback_receiver, STATE_CHANGE_TIMEOUT).await;
            wait_for_successful_callback(&mut ctx.callback_receiver, STATE_CHANGE_TIMEOUT).await;
            wait_for_successful_callback(&mut ctx.callback_receiver, STATE_CHANGE_TIMEOUT).await;

            let result = UpdateManagerService::Activate(
                &service_impl,
                make_service_fqin(),
                &ActivateRequest::default(),
            )
            .await;

            // Wait for the activate pre-reboot step to finish
            let update_state_start =
                wait_for_successful_callback(&mut ctx.callback_receiver, STATE_CHANGE_TIMEOUT)
                    .await;
            let update_progress =
                wait_for_successful_callback(&mut ctx.callback_receiver, STATE_CHANGE_TIMEOUT)
                    .await;
            let update_state_end =
                wait_for_successful_callback(&mut ctx.callback_receiver, STATE_CHANGE_TIMEOUT)
                    .await;
            assert!(result.is_ok());

            if let StatusUpdate::UpdateManagerStatus(update_manager_status) = update_state_start {
                assert_eq!(
                    update_manager_status.update_state.enum_value().unwrap(),
                    ApiUpdateState::ACTIVATE_PRE_REBOOT
                );
            } else {
                panic!("Unexpected StatusUpdate: {update_state_start:?}");
            }

            if let StatusUpdate::UpdateProgress(update_progress) = update_progress {
                assert_eq!(update_progress.status_code, IDLE);
                assert_eq!(update_progress.percentage, STATUS_UPDATE_PERCENTAGE_ACTIVE);
            } else {
                panic!("Unexpected StatusUpdate: {update_progress:?}");
            }

            if let StatusUpdate::UpdateManagerStatus(update_manager_status) = update_state_end {
                assert_eq!(
                    update_manager_status.update_state.enum_value().unwrap(),
                    ApiUpdateState::ACTIVATE_PRE_REBOOT_COMPLETE
                );
            } else {
                panic!("Unexpected StatusUpdate: {update_state_end:?}");
            }
        }

        let ota_package_path = temp_paths.ota_package.clone();
        {
            let mut new_mock_services = MockServices::new();
            new_mock_services
                .boot_control_service
                .expect_get_current_slot()
                .times(1)
                .returning(|| Ok(0));

            new_mock_services
                .boot_control_service
                .expect_is_slot_marked_successful()
                .times(1)
                .returning(|_| Ok(false));

            let (ctx, init) = TestContext::new(temp_paths.clone(), new_mock_services);

            UpdateManagerServiceImpl::new_with_paths(init, &ota_package_path, &ctx.save_state_path)
                .await
                .unwrap();
        }

        let ota_package_path = temp_paths.ota_package.clone();
        {
            let mut new_mock_services = MockServices::new();
            new_mock_services
                .boot_control_service
                .expect_get_current_slot()
                .times(1)
                .returning(|| Ok(1));

            let (ctx, init) = TestContext::new(temp_paths.clone(), new_mock_services);

            let service_impl = UpdateManagerServiceImpl::new_with_paths(
                init,
                &ota_package_path,
                &ctx.save_state_path,
            )
            .await
            .unwrap();

            let status_result = UpdateManagerService::GetStatus(
                &service_impl,
                make_service_fqin(),
                &GetStatusRequest::default(),
            )
            .await
            .unwrap();

            assert_eq!(
                status_result.update_manager_status.update_state.unwrap(),
                ApiUpdateState::ACTIVATE_PRE_REBOOT_FAILURE
            );
        }
    }
}

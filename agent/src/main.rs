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

//! Update Manager agent

mod apex_service;
mod atomic_file;
mod boot_control_service;
mod conversion;
mod metadata;
mod ota_package;
mod payload;
mod state_machine;
mod subscriber_manager;
mod update_engine;
mod update_manager_service_impl;
mod update_manager_state;
mod vold;
mod vpm;
mod vsidl_update_manager_listener_service_client_provider;
use apex_service::BinderApexService;
use boot_control_service::BinderBootControlService;
use log::{debug, error};
use sdv_comms::id::ServiceFqin;
use sdv_comms::ContextRef;
use sdv_mw_rs_com_android_sdv_update_manager_update_manager_agent::UpdateManagerAgent;
use sdv_mw_rs_sdv_comms::SdvComms;
use sdv_status_rs::SdvResult;
use std::path::Path;
use std::sync::Arc;
use tokio::sync::mpsc::unbounded_channel;
use tokio::task;
use tracing::Level;
use update_engine::BinderUpdateEngine;
use update_manager_service_impl::{UpdateManagerInit, UpdateManagerServiceImpl};
use vold::VdcVoldService;
use vpm::BinderPowerStateClient;
use vsidl_update_manager_listener_service_client_provider::VSIDLUpdateManagerListenerServiceClientProvider;

const APP_STAGING_DIR: &str = "/data/app-staging";

async fn run() -> SdvResult<()> {
    sdv_log::init_logger("sdv_update_manager_agent").expect("Failed to initialize logger");
    binder::ProcessState::start_thread_pool();
    sdv_comms_ctx::wait_for_agents()?;

    let fqin = ServiceFqin::builder()
        .sdv_vm_name("vm")
        .sdv_package_name("com.android.sdv.update_manager")
        .service_bundle_name("UpdateManagerAgent")
        .service_instance_name("default")
        .build()?;

    let context_ref = ContextRef::create(fqin);
    let comms = Arc::new(SdvComms { context: context_ref });

    let apex_service = BinderApexService::new().await?;
    let boot_control_service = BinderBootControlService::new().await?;

    let vsidl_update_manager_listener_service_client_provider =
        Box::new(VSIDLUpdateManagerListenerServiceClientProvider::new(comms.clone()));

    let update_engine = BinderUpdateEngine::new().await?;
    let vold_service = VdcVoldService::new();

    let (power_state_report_sender, power_state_report_receiver) = unbounded_channel();
    let power_state_client = BinderPowerStateClient::new(power_state_report_sender)?;

    let init = UpdateManagerInit {
        apex_service: Arc::new(apex_service),
        app_staging_dir: Path::new(APP_STAGING_DIR).to_path_buf(),
        boot_control_service: Arc::new(boot_control_service),
        power_state_client: Arc::new(power_state_client),
        power_state_report_receiver,
        update_engine: Box::new(update_engine),
        update_manager_listener_service_client_provider:
            vsidl_update_manager_listener_service_client_provider,
        vold_service: Arc::new(vold_service),
    };

    let update_manager_service_impl = Arc::new(UpdateManagerServiceImpl::new(init).await?);

    let _update_manager_agent =
        UpdateManagerAgent::new(comms.clone(), (update_manager_service_impl.clone(),))
            .await
            .map(Arc::new);

    let start_message = "sdv_update_manager_agent has started successfully";
    debug!("{start_message}");
    tracing::event!(Level::DEBUG, "{start_message}");

    task::block_in_place(move || {
        binder::ProcessState::join_thread_pool();
    });

    Ok(())
}

#[tokio::main]
async fn main() -> SdvResult<()> {
    run().await.inspect_err(|e| {
        error!("Exiting UpdateManagerAgent with error: {e:?}");
    })
}

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

use binder::{BinderFeatures, Interface, Result as BinderResult, Status, StatusCode, Strong};
use google_sdv_vpm::aidl::google::sdv::vpm::IPowerStateClientApi::{
    BpPowerStateClientApi, IPowerStateClientApi,
};
use google_sdv_vpm::aidl::google::sdv::vpm::IPowerStateReportListener::{
    BnPowerStateReportListener, IPowerStateReportListener,
};
use google_sdv_vpm::aidl::google::sdv::vpm::PowerStateReport::PowerStateReport;
use log::{debug, error};
use mockall::automock;
use persistent_binder::{BinderConnectionManager, PersistentBinderConnection};
use sdv_status_rs::{SdvResult, SdvStatus, SdvStatusCode};
use std::sync::{Arc, Mutex};
use tokio::sync::mpsc::UnboundedSender;
use tokio::sync::oneshot;

struct BinderPowerStateReportListener {
    power_state_report_processing_complete_sender: Arc<Mutex<Option<oneshot::Sender<()>>>>,
    power_state_report_sender: UnboundedSender<PowerStateReport>,
}

impl Interface for BinderPowerStateReportListener {}

#[allow(non_snake_case)]
impl IPowerStateReportListener for BinderPowerStateReportListener {
    fn onPowerStateReportChanged(&self, power_state_report: PowerStateReport) -> BinderResult<()> {
        let (response_sender, response_receiver) = oneshot::channel();

        {
            let mut power_state_report_processing_complete_sender = self
                .power_state_report_processing_complete_sender
                .lock()
                .expect("Could not lock on power_state_report_processing_complete_sender");
            *power_state_report_processing_complete_sender = Some(response_sender);
        }

        if self.power_state_report_sender.send(power_state_report).is_err() {
            error!("Failed to send {power_state_report:?}: Receiver dropped");
        }

        response_receiver.blocking_recv().map_err(|_| {
            const ERROR_MESSAGE: &str =
                "Sender of onPowerStateReportChanged response oneshot dropped";
            error!("{ERROR_MESSAGE}");

            Status::new_service_specific_error_str(
                StatusCode::BAD_VALUE as i32,
                Some(ERROR_MESSAGE),
            )
        })
    }
}

#[automock]
pub trait PowerStateClient: Send + Sync {
    fn power_state_report_processing_complete(&self) -> SdvResult<()>;
}

fn get_vpm_binder_service() -> Strong<dyn IPowerStateClientApi> {
    let descriptor =
        <BpPowerStateClientApi as IPowerStateClientApi>::get_descriptor().to_owned() + "/default";

    loop {
        match binder::wait_for_interface(&descriptor) {
            Ok(power_state_report_listener) => return power_state_report_listener,
            Err(e) => error!("Failed to get IPowerStateClientApi interface, retrying: {e:?}"),
        }
    }
}

fn subscribe_to_power_state_report(
    power_state_client_service: &Strong<dyn IPowerStateClientApi>,
    power_state_report_listener: &Strong<dyn IPowerStateReportListener>,
    power_state_report_sender: &UnboundedSender<PowerStateReport>,
) {
    let result = power_state_client_service
        .subscribeToPowerStateReport(power_state_report_listener)
        .map_err(|e| format!("subscribeToPowerStateReport failed: {e:?}"))
        .and_then(|power_state_report| {
            power_state_report_sender
                .send(power_state_report)
                .map_err(|_| "Failed to send power state report, receiver dropped".to_string())
        });

    if let Err(e) = result {
        error!("{e}");
    }
}

pub struct BinderPowerStateClient {
    _power_state_client_service: Arc<dyn BinderConnectionManager<dyn IPowerStateClientApi>>,
    power_state_report_processing_complete_sender: Arc<Mutex<Option<oneshot::Sender<()>>>>,
}

impl BinderPowerStateClient {
    pub fn new(power_state_report_sender: UnboundedSender<PowerStateReport>) -> SdvResult<Self> {
        let power_state_report_processing_complete_sender = Arc::new(Mutex::new(None));

        let _power_state_client_service: Arc<
            dyn BinderConnectionManager<dyn IPowerStateClientApi>,
        > = PersistentBinderConnection::new(get_vpm_binder_service);

        let power_state_report_listener = BnPowerStateReportListener::new_binder(
            BinderPowerStateReportListener {
                power_state_report_processing_complete_sender:
                    power_state_report_processing_complete_sender.clone(),
                power_state_report_sender: power_state_report_sender.clone(),
            },
            BinderFeatures::default(),
        );

        {
            let power_state_client_service = _power_state_client_service
                .get_binder()
                .lock()
                .expect("Could not lock on _power_state_client_service");

            // NOTE: A failure to subscribe to power state reports is only logged. It does not
            // block the service from starting. If a bad update has been applied and VPM isn't
            // functioning correctly, the SDV Update Manager should still be available to
            // handle a Rollback() request.
            subscribe_to_power_state_report(
                &power_state_client_service,
                &power_state_report_listener,
                &power_state_report_sender,
            );
        };

        _power_state_client_service.set_reconnect_listener(Box::new(
            move |power_state_client_service| {
                debug!("Re-subscribing to PowerStateReports");

                subscribe_to_power_state_report(
                    power_state_client_service,
                    &power_state_report_listener,
                    &power_state_report_sender,
                );
            },
        ));

        Ok(BinderPowerStateClient {
            _power_state_client_service,
            power_state_report_processing_complete_sender,
        })
    }
}

impl PowerStateClient for BinderPowerStateClient {
    fn power_state_report_processing_complete(&self) -> SdvResult<()> {
        let sender_option = {
            let mut power_state_report_processing_complete_sender = self
                .power_state_report_processing_complete_sender
                .lock()
                .expect("Could not lock on power_state_report_processing_complete_sender");
            power_state_report_processing_complete_sender.take()
        };

        match sender_option {
            Some(sender) => {
                debug!("PowerStateReport in progress, signaling processing complete");
                sender.send(()).or(Err(SdvStatus::with_message(
                    SdvStatusCode::Internal,
                    "PowerStateReport processing complete receiver dropped",
                )))
            }
            None => Ok(()),
        }
    }
}

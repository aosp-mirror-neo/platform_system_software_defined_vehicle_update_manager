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
use com_sdv_google_update_manager_client_update_manager_listener_service_rpc::Client as UpdateManagerListenerServiceClient;
use log::debug;
use mockall::automock;
use sdv_mw_rs_comms::{
    Availability, ClientAttributes, ClientOptions, Communicate, RpcClient, UnitName, UnitQuery,
};
use sdv_mw_rs_sdv_comms::SdvComms;
use sdv_status_rs::SdvResult;
use std::sync::Arc;
use update_manager_api::update_manager_listener_service::{
    OnStatusUpdateRequest, OnStatusUpdateResponse,
};

#[automock]
#[allow(non_snake_case, missing_docs)]
#[async_trait]
pub trait UpdateManagerListenerService: Send + Sync {
    async fn OnStatusUpdate(
        &self,
        request: &OnStatusUpdateRequest,
    ) -> sdv_status_rs::SdvResult<OnStatusUpdateResponse>;
}

#[async_trait]
impl UpdateManagerListenerService for UpdateManagerListenerServiceClient {
    async fn OnStatusUpdate(
        &self,
        request: &OnStatusUpdateRequest,
    ) -> sdv_status_rs::SdvResult<OnStatusUpdateResponse> {
        self.OnStatusUpdate(request).await
    }
}

pub struct UpdateManagerListenerServiceInfo {
    pub client: Box<dyn UpdateManagerListenerService>,
    pub registration_event_stream:
        std::pin::Pin<Box<dyn futures::Stream<Item = Availability> + Send>>,
}

// This trait is implemented by structs that provide clients to the
// UpdateManagerListenerService. These clients are used to send callbacks to clients of the
// UpdateManagerService.
#[automock]
#[async_trait]
pub trait UpdateManagerListenerServiceClientProvider: Send + Sync {
    async fn get_sdv_update_manager_update_manager_listener_service_info(
        &self,
        package: &str,
        bundle: &str,
    ) -> SdvResult<UpdateManagerListenerServiceInfo>;
}

pub struct VSIDLUpdateManagerListenerServiceClientProvider {
    comms: Arc<SdvComms>,
}

impl VSIDLUpdateManagerListenerServiceClientProvider {
    pub fn new(comms: Arc<SdvComms>) -> Self {
        VSIDLUpdateManagerListenerServiceClientProvider { comms }
    }
}

#[async_trait]
impl UpdateManagerListenerServiceClientProvider
    for VSIDLUpdateManagerListenerServiceClientProvider
{
    async fn get_sdv_update_manager_update_manager_listener_service_info(
        &self,
        package: &str,
        bundle: &str,
    ) -> SdvResult<UpdateManagerListenerServiceInfo> {
        debug!("Creating a client for (package: {package}, bundle: {bundle})");
        // TODO b/373844234 handle clients disconnecting
        let attributes = ClientAttributes::builder()
            .server_name(
                UnitName::builder()
                    .vm_name(self.comms.context.get_self_fqin().get_sdv_vm_name())
                    .package_name(package)
                    .bundle_name(bundle)
                    .service_unit_name(UpdateManagerListenerServiceClient::DEFAULT_UNIT_NAME)
                    .build()?,
            )
            .client_options(ClientOptions::default())
            .build()?;

        let update_manager_listener_service_client = UpdateManagerListenerServiceClient::new(
            self.comms.create_client_transport(&attributes).await?,
        );

        let registration_event_stream = self
            .comms
            .create_registration_event_stream(&UnitQuery::Name(
                attributes.get_server_name().clone(),
            ))
            .await?;

        Ok(UpdateManagerListenerServiceInfo {
            client: Box::new(update_manager_listener_service_client)
                as Box<dyn UpdateManagerListenerService>,
            registration_event_stream,
        })
    }
}

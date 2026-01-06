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

use crate::vsidl_update_manager_listener_service_client_provider::{
    UpdateManagerListenerService, UpdateManagerListenerServiceClientProvider,
};
use futures::StreamExt;
use log::{debug, error};
use sdv_mw_rs_comms::Availability;
use sdv_status_rs::{derive, SdvResult, SdvStatusCode};
use std::borrow::Borrow;
use std::collections::hash_map::Entry::{Occupied, Vacant};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::Mutex;
use tokio::task::JoinHandle;
use update_manager_api::update_manager_listener_service::on_status_update_request::Status_update as StatusUpdate;
use update_manager_api::update_manager_listener_service::OnStatusUpdateRequest;

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
struct SubscriberIdentifier {
    package: String,
    bundle: String,
}

impl SubscriberIdentifier {
    fn new(package: &str, bundle: &str) -> Self {
        SubscriberIdentifier { package: package.to_string(), bundle: bundle.to_string() }
    }
}

struct SubscriberInfo {
    client: Box<dyn UpdateManagerListenerService>,
    join_handle: JoinHandle<()>,
}

async fn send_callback(
    client: &dyn UpdateManagerListenerService,
    subscriber_identifier: &SubscriberIdentifier,
    on_status_update_request: &OnStatusUpdateRequest,
) -> SdvResult<()> {
    debug!(
        "Sending callback to {:?} for StatusUpdate: {:?}",
        subscriber_identifier, on_status_update_request
    );

    client.OnStatusUpdate(on_status_update_request).await?;

    Ok(())
}

pub struct SubscriberManager {
    subscribers: Arc<Mutex<HashMap<SubscriberIdentifier, SubscriberInfo>>>,
    update_manager_listener_service_client_provider:
        Box<dyn UpdateManagerListenerServiceClientProvider>,
}

impl SubscriberManager {
    pub fn new(
        update_manager_listener_service_client_provider: Box<
            dyn UpdateManagerListenerServiceClientProvider,
        >,
    ) -> Self {
        Self {
            subscribers: Arc::new(Mutex::new(HashMap::new())),
            update_manager_listener_service_client_provider,
        }
    }

    pub async fn notify(&self, status_update: StatusUpdate) {
        let on_status_update_request = OnStatusUpdateRequest {
            status_update: Some(status_update.clone()),
            ..Default::default()
        };

        let unlocked_subscribers = self.subscribers.lock().await;

        for (subscriber_identifier, subscriber_info) in unlocked_subscribers.iter() {
            if let Err(e) = send_callback(
                subscriber_info.client.borrow(),
                subscriber_identifier,
                &on_status_update_request,
            )
            .await
            {
                error!(
                    "Failed to send callback to subscriber {:?}: {:?}",
                    subscriber_identifier, e
                );
            }
        }
    }

    pub async fn subscribe(
        &self,
        package: &str,
        bundle: &str,
        status_update: StatusUpdate,
    ) -> SdvResult<()> {
        let subscriber_identifier = SubscriberIdentifier::new(package, bundle);
        let cloned_subscriber_identifier = subscriber_identifier.clone();
        let on_status_update_request =
            OnStatusUpdateRequest { status_update: Some(status_update), ..Default::default() };

        let mut subscribers = self.subscribers.lock().await;
        match subscribers.entry(subscriber_identifier.clone()) {
            Vacant(entry) => {
                let update_manager_listener_service_info = self
                    .update_manager_listener_service_client_provider
                    .get_sdv_update_manager_update_manager_listener_service_info(package, bundle)
                    .await?;

                send_callback(
                    update_manager_listener_service_info.client.borrow(),
                    &subscriber_identifier,
                    &on_status_update_request,
                )
                .await
                .map_err(derive!(
                    SdvStatusCode::Unavailable,
                    "Failed to send status update to subscriber, dropping subscription."
                ))?;

                let cloned_subscribers = self.subscribers.clone();
                let join_handle = tokio::spawn(async move {
                    Self::unsubscribe_task(
                        update_manager_listener_service_info.registration_event_stream,
                        cloned_subscribers,
                        cloned_subscriber_identifier,
                    )
                    .await;
                });

                entry.insert_entry(SubscriberInfo {
                    client: update_manager_listener_service_info.client,
                    join_handle,
                });
            }
            Occupied(entry) => {
                let subscriber_info = entry.get();
                send_callback(
                    subscriber_info.client.borrow(),
                    &subscriber_identifier,
                    &on_status_update_request,
                )
                .await
                .map_err(derive!(
                    SdvStatusCode::Unavailable,
                    "Failed to send status update to subscriber"
                ))?;
            }
        };

        Ok(())
    }

    pub async fn unsubscribe(&self, package: &str, bundle: &str) {
        let mut locked_subscribers = self.subscribers.lock().await;

        if let Some(subscriber_info) =
            locked_subscribers.remove(&SubscriberIdentifier::new(package, bundle))
        {
            subscriber_info.join_handle.abort();
            if let Err(e) = subscriber_info.join_handle.await {
                if !e.is_cancelled() {
                    error!("An error occurred when aborting the unsubscribe task for {package}.{bundle}: {e:?}");
                }
            }
        }
    }

    async fn unsubscribe_task(
        mut registration_event_stream: std::pin::Pin<
            Box<dyn futures::Stream<Item = Availability> + Send>,
        >,
        subscribers: Arc<Mutex<HashMap<SubscriberIdentifier, SubscriberInfo>>>,
        subscriber_identifier: SubscriberIdentifier,
    ) {
        while let Some(availability) = registration_event_stream.next().await {
            if let Availability::Unavailable = availability {
                let mut locked_subscribers = subscribers.lock().await;
                locked_subscribers.remove(&subscriber_identifier);
                break;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::vsidl_update_manager_listener_service_client_provider::{
        MockUpdateManagerListenerService, MockUpdateManagerListenerServiceClientProvider,
        UpdateManagerListenerServiceClientProvider, UpdateManagerListenerServiceInfo,
    };
    use sdv_status_rs::SdvStatus;
    use std::sync::{Arc, Mutex};
    use tokio::sync::mpsc::unbounded_channel;
    use tokio_stream::wrappers::UnboundedReceiverStream;
    use update_manager_api::update_manager_listener_service::{
        OnStatusUpdateResponse, UpdateProgress,
    };
    use update_manager_api::update_manager_status::UpdateManagerStatus;
    use update_manager_api::update_state::UpdateState;

    fn get_status_update_with_update_manager_status() -> StatusUpdate {
        StatusUpdate::UpdateManagerStatus(UpdateManagerStatus {
            update_state: UpdateState::READY.into(),
            ..Default::default()
        })
    }

    fn get_mock_update_manager_listener_service(
    ) -> (MockUpdateManagerListenerService, mockall::Sequence) {
        let mut sequence = mockall::Sequence::new();
        let mut mock_service = MockUpdateManagerListenerService::new();
        mock_service
            .expect_OnStatusUpdate()
            .withf(|request| {
                if let Some(StatusUpdate::UpdateManagerStatus(ref update_manager_status)) =
                    request.status_update
                {
                    matches!(update_manager_status.update_state.enum_value(), Ok(update_state) if update_state == UpdateState::READY)
                } else {
                    false
                }
            })
            .times(1)
            .in_sequence(&mut sequence)
            .returning(|_| Ok(OnStatusUpdateResponse::default()));

        (mock_service, sequence)
    }

    #[tokio::test]
    async fn test_unavailable_subscriber_is_dropped() {
        let mut mock_provider = MockUpdateManagerListenerServiceClientProvider::new();
        mock_provider
            .expect_get_sdv_update_manager_update_manager_listener_service_info()
            .withf(|package: &str, bundle: &str| package == "package" && bundle == "bundle")
            .times(1)
            .returning(move |_, _| {
                let mut mock_service = MockUpdateManagerListenerService::new();
                mock_service
                    .expect_OnStatusUpdate()
                    .withf(|request| {
                        if let Some(StatusUpdate::UpdateManagerStatus(ref update_manager_status)) =
                            request.status_update
                        {
                            matches!(update_manager_status.update_state.enum_value(), Ok(update_state) if update_state == UpdateState::READY)
                        } else {
                            false
                        }
                    })
                    .times(1)
                    .returning(|_| {
                        Err(SdvStatus::with_message(SdvStatusCode::Unavailable, "Not available"))
                    });

                let boxed_mock_service =
                    Box::new(mock_service) as Box<dyn UpdateManagerListenerService>;

                let registration_event_stream = Box::new(futures::stream::iter(vec![]))
                    as Box<dyn futures::Stream<Item = Availability> + Send>;

                Ok(UpdateManagerListenerServiceInfo {
                    client: boxed_mock_service,
                    registration_event_stream: Box::into_pin(registration_event_stream),
                })
            });

        let subscriber_manager = SubscriberManager::new(
            Box::new(mock_provider) as Box<dyn UpdateManagerListenerServiceClientProvider>
        );

        let status_update = get_status_update_with_update_manager_status();
        let result = subscriber_manager.subscribe("package", "bundle", status_update).await;

        assert!(result.is_err());

        let subscribers = subscriber_manager.subscribers.lock().await;
        assert!(subscribers.is_empty())
    }

    #[tokio::test]
    async fn test_notify_sends_callback() {
        let mut mock_provider = MockUpdateManagerListenerServiceClientProvider::new();
        mock_provider
            .expect_get_sdv_update_manager_update_manager_listener_service_info()
            .withf(|package: &str, bundle: &str| package == "package" && bundle == "bundle")
            .times(1)
            .returning(move |_, _| {
                let (mut mock_service, mut sequence) = get_mock_update_manager_listener_service();
                mock_service
                    .expect_OnStatusUpdate()
                    .withf(|request| {
                        if let Some(StatusUpdate::UpdateProgress(ref update_progress)) =
                            request.status_update
                        {
                            update_progress == &UpdateProgress::default()
                        } else {
                            false
                        }
                    })
                    .times(1)
                    .in_sequence(&mut sequence)
                    .returning(|_| Ok(OnStatusUpdateResponse::default()));

                let boxed_mock_service =
                    Box::new(mock_service) as Box<dyn UpdateManagerListenerService>;

                let registration_event_stream = Box::new(futures::stream::iter(vec![]))
                    as Box<dyn futures::Stream<Item = Availability> + Send>;

                Ok(UpdateManagerListenerServiceInfo {
                    client: boxed_mock_service,
                    registration_event_stream: Box::into_pin(registration_event_stream),
                })
            });

        let subscriber_manager = SubscriberManager::new(
            Box::new(mock_provider) as Box<dyn UpdateManagerListenerServiceClientProvider>
        );

        let status_update = get_status_update_with_update_manager_status();
        subscriber_manager
            .subscribe("package", "bundle", status_update)
            .await
            .expect("Failed to subscribe");
        subscriber_manager.notify(StatusUpdate::UpdateProgress(UpdateProgress::default())).await;
    }

    #[tokio::test]
    async fn test_unsubscribed_client_does_not_get_notifications() {
        let mut mock_provider = MockUpdateManagerListenerServiceClientProvider::new();
        mock_provider
            .expect_get_sdv_update_manager_update_manager_listener_service_info()
            .withf(|package: &str, bundle: &str| package == "package" && bundle == "bundle")
            .times(1)
            .returning(move |_, _| {
                let (mock_service, _) = get_mock_update_manager_listener_service();

                let boxed_mock_service =
                    Box::new(mock_service) as Box<dyn UpdateManagerListenerService>;

                let registration_event_stream = Box::new(futures::stream::iter(vec![]))
                    as Box<dyn futures::Stream<Item = Availability> + Send>;

                Ok(UpdateManagerListenerServiceInfo {
                    client: boxed_mock_service,
                    registration_event_stream: Box::into_pin(registration_event_stream),
                })
            });

        let subscriber_manager = SubscriberManager::new(
            Box::new(mock_provider) as Box<dyn UpdateManagerListenerServiceClientProvider>
        );

        let status_update = get_status_update_with_update_manager_status();
        subscriber_manager
            .subscribe("package", "bundle", status_update)
            .await
            .expect("Failed to subscribe");
        subscriber_manager.unsubscribe("package", "bundle").await;
        subscriber_manager.notify(StatusUpdate::UpdateProgress(UpdateProgress::default())).await;
    }

    #[tokio::test]
    async fn test_unavailable_client_does_not_get_notifications() {
        let mut mock_provider = MockUpdateManagerListenerServiceClientProvider::new();
        let (sender, receiver) = unbounded_channel::<Availability>();

        let registration_event_stream = Box::new(UnboundedReceiverStream::new(receiver))
            as Box<dyn futures::Stream<Item = Availability> + Send>;
        let registration_event_stream = Box::into_pin(registration_event_stream);
        let shareable_registration_event_stream =
            Arc::new(Mutex::new(Some(registration_event_stream)));
        let cloned_registration_event_stream = shareable_registration_event_stream.clone();

        mock_provider
            .expect_get_sdv_update_manager_update_manager_listener_service_info()
            .withf(|package: &str, bundle: &str| package == "package" && bundle == "bundle")
            .times(1)
            .returning(move |_, _| {
                let (mock_service, _) = get_mock_update_manager_listener_service();

                let boxed_mock_service =
                    Box::new(mock_service) as Box<dyn UpdateManagerListenerService>;

                let registration_event_stream = cloned_registration_event_stream
                    .lock()
                    .expect("Failed to lock mutex")
                    .take()
                    .expect("Should be possible since only called once");

                Ok(UpdateManagerListenerServiceInfo {
                    client: boxed_mock_service,
                    registration_event_stream,
                })
            });

        let subscriber_manager = SubscriberManager::new(
            Box::new(mock_provider) as Box<dyn UpdateManagerListenerServiceClientProvider>
        );

        let status_update = get_status_update_with_update_manager_status();
        subscriber_manager
            .subscribe("package", "bundle", status_update)
            .await
            .expect("Failed to subscribe");

        sender
            .send(Availability::Unavailable)
            .expect("Failed to send Unavailable registration event");

        // The Unavailable messages are processed on another task. We need to yield in this test to
        // allow that task to run and process the message.
        tokio::task::yield_now().await;

        // This test assumes that yielding once allows the `unsubscribe_task` the opportunity to
        // remove the subscriber. This assert ensures this did happen. If this assert fails,
        // then the test needs to be rewritten to properly wait for the subscriber to be removed.
        let no_subscribers = {
            let subscribers = subscriber_manager.subscribers.lock().await;
            subscribers.is_empty()
        };
        assert!(no_subscribers, "Expected all subscribers to be removed");

        // At this point, there should be no subscribers
        subscriber_manager.notify(StatusUpdate::UpdateProgress(UpdateProgress::default())).await;
    }
}

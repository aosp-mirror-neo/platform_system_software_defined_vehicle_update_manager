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

use crate::update_manager_service_impl::UpdateManagerEnvironment;
use crate::update_manager_state::{UpdateManagerEvent, UpdateManagerState, UpdateState};
use log::{debug, error, warn};
use sdv_status_rs::{derive, SdvResult, SdvStatusCode};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use tokio::sync::{mpsc, oneshot, Mutex};
use update_manager_api::update_manager_listener_service::on_status_update_request::Status_update as StatusUpdate;

pub const SAVE_STATE_FILE: &str = "/metadata/sdv_update_manager_agent/state.bp";

struct EventQueueItem {
    event: UpdateManagerEvent,
    sender: Option<oneshot::Sender<SdvResult<()>>>,
}

impl EventQueueItem {
    fn new(event: UpdateManagerEvent, sender: Option<oneshot::Sender<SdvResult<()>>>) -> Self {
        Self { event, sender }
    }
}

fn send_event_queue_item(
    event_queue_sender: &mpsc::UnboundedSender<EventQueueItem>,
    event_queue_item: EventQueueItem,
) {
    if let Err(e) = event_queue_sender.send(event_queue_item) {
        error!("Failed to send event to event queue: {e:?}");
    }
}

struct StateMachineTask {
    env: Arc<UpdateManagerEnvironment>,
    event_queue_receiver: mpsc::UnboundedReceiver<EventQueueItem>,
    event_queue_sender: mpsc::UnboundedSender<EventQueueItem>,
    save_state_path: PathBuf,
    state: Arc<Mutex<UpdateState>>,
}

impl StateMachineTask {
    async fn init(&self, recovery_required: bool) -> SdvResult<()> {
        let initial_event = {
            let state = self.state.lock().await;

            // TODO this will panic right now. It should return an Err instead.
            self.save_state_and_notify(&state).await;

            match recovery_required {
                true => state.on_recovery(self.env.clone()).await,
                false => state.on_enter(self.env.clone()).await,
            }
        };

        if let Some(event) = initial_event {
            let event_queue_item = EventQueueItem::new(event, None);
            send_event_queue_item(&self.event_queue_sender, event_queue_item);
        }

        Ok(())
    }

    async fn handle_event(&self, event: &UpdateManagerEvent) -> SdvResult<()> {
        let mut state = self.state.lock().await;
        let old_state = state.value();
        let new_state = state.get_next_state(event.clone())?;

        *state = new_state;

        let new_state_value = state.value();
        debug!("Transitioned from {old_state:?} to {new_state_value:?} due to event {event:?}");

        // TODO b/380047542 Notifications should be offloaded to a separate thread once the
        // transition loop is removed.
        self.save_state_and_notify(&state).await;
        let optional_returned_event = state.on_enter(self.env.clone()).await;
        if let Some(returned_event) = optional_returned_event {
            send_event_queue_item(
                &self.event_queue_sender,
                EventQueueItem::new(returned_event, None),
            );
        }

        Ok(())
    }

    // WARNING: This function will panic if a failure occurs while saving the state.
    async fn save_state_and_notify(&self, state: &UpdateState) {
        debug!("Saving state to {:?}", self.save_state_path);
        if let Err(e) = state.save(&self.save_state_path) {
            panic!(
                "Failed to save state {:?} to {:?} due to error: {:?}",
                state.value(),
                self.save_state_path,
                e
            );
        }

        self.env.subscriber_manager.notify(StatusUpdate::UpdateManagerStatus((state).into())).await;
    }

    pub async fn run(mut self) {
        while let Some(EventQueueItem { event, sender }) = self.event_queue_receiver.recv().await {
            let result = match &event {
                UpdateManagerEvent::Init(recovery_required) => self.init(*recovery_required).await,
                not_init_event => self.handle_event(not_init_event).await,
            };

            if let Some(sender) = sender {
                if let Err(e) = sender.send(result) {
                    error!("Failed to send event result: {e:?}");
                }
            } else if let Err(e) = result {
                error!("Encountered error while handling event {event:?}: {e:?}");
            }
        }
    }
}

pub struct StateMachine {
    event_queue_sender: mpsc::UnboundedSender<EventQueueItem>,
    state: Arc<Mutex<UpdateState>>,
}

impl StateMachine {
    pub async fn new(
        env: Arc<UpdateManagerEnvironment>,
        save_state_path: &Path,
    ) -> SdvResult<Self> {
        let (state, recovery_required) = match UpdateState::load(save_state_path) {
            Ok(state) => {
                debug!("Initial state: {:?}", state.value());
                (state, true)
            }
            Err(e) => {
                warn!("Failed to load state from disk: {e:?}");
                let state = UpdateState::new();
                debug!("Falling back to state: {:?}", state.value());
                // Using the default state due to a lack of readable saved state does not trigger
                // the "recovery" condition.
                (state, false)
            }
        };

        let state = Arc::new(Mutex::new(state));

        let (event_queue_sender, event_queue_receiver) =
            mpsc::unbounded_channel::<EventQueueItem>();

        let state_machine =
            StateMachine { event_queue_sender: event_queue_sender.clone(), state: state.clone() };

        let state_machine_task = StateMachineTask {
            env: env.clone(),
            event_queue_receiver,
            event_queue_sender,
            save_state_path: save_state_path.to_path_buf(),
            state,
        };

        tokio::spawn(async move { state_machine_task.run().await });

        state_machine
            .handle_event_and_wait_for_response(UpdateManagerEvent::Init(recovery_required))
            .await?;

        Ok(state_machine)
    }

    pub async fn get_state(&self) -> UpdateState {
        self.state.lock().await.clone()
    }

    /// Add an event to the event queue. If any errors occur while handling the event, they are returned.
    /// This function blocks until the event is handled.
    ///
    /// If the event leads to a transition but the state fails to save to disk, the service panics.
    pub async fn handle_event_and_wait_for_response(
        &self,
        event: UpdateManagerEvent,
    ) -> SdvResult<()> {
        let (sender, receiver) = oneshot::channel::<SdvResult<()>>();
        self.event_queue_sender.send(EventQueueItem::new(event, Some(sender))).map_err(derive!(
            SdvStatusCode::Internal,
            "Failed to add event to event queue: {e:?}"
        ))?;

        receiver.await.map_err(derive!(
            SdvStatusCode::Internal,
            "Failed to receive response from handling event (sender dropped)"
        ))?
    }

    /// Add an event to the event queue. If an error occurs while adding it to the event queue, it is
    /// immediately returned. If any errors occur while handling the event, they are logged.
    /// This function immediately returns (it does not wait for the event to be handled).
    ///
    /// If the event leads to a transition but the state fails to save to disk, the service panics.
    pub fn handle_event(&self, event: UpdateManagerEvent) -> SdvResult<()> {
        self.event_queue_sender
            .send(EventQueueItem::new(event, None))
            .map_err(derive!(SdvStatusCode::Internal, "Failed to add event to event queue: {e:?}"))
    }
}

// TODO b/379521848 Add unit tests

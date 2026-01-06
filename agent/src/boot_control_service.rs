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

use android_hardware_boot::aidl::android::hardware::boot::IBootControl::IBootControlAsync;
use async_trait::async_trait;
use binder::Strong;
use binder_tokio::Tokio;
use mockall::automock;
use sdv_status_rs::{derive, SdvResult, SdvStatus, SdvStatusCode};

/// Given a boot slot, get its opposite slot.
///
/// This function returns 1 if given 0 and 0 if given 1.
/// If the given slot is neither 0 nor 1 then an error is returned.
pub fn get_opposite_slot(slot: i32) -> SdvResult<i32> {
    match slot {
        0 => Ok(1),
        1 => Ok(0),
        _ => Err(SdvStatus::with_message(
            SdvStatusCode::OutOfRange,
            "Update Manager only supports systems with two update slots",
        )),
    }
}

// automock must come before async_trait
#[automock]
#[async_trait]
pub trait BootControlService: Send + Sync {
    /// Get the currently booted slot
    async fn get_current_slot(&self) -> SdvResult<i32>;

    /// Set the active boot slot (takes effect on next reboot)
    async fn set_active_boot_slot(&self, slot_id: i32) -> SdvResult<()>;

    /// Mark the current slot as successful
    async fn mark_boot_successful(&self) -> SdvResult<()>;

    /// Returns true if the given slot is marked as successful
    async fn is_slot_marked_successful(&self, slot: i32) -> SdvResult<bool>;

    /// Returns true if the given slot is bootable
    async fn is_slot_bootable(&self, slot: i32) -> SdvResult<bool>;

    /// Set the given slot as unbootable
    async fn set_slot_as_unbootable(&self, slot: i32) -> SdvResult<()>;

    /// First, set the active slot to the given slot. If this succeeds, set the opposite slot as unbootable.
    /// If setting the opposite slot as unbootable fails, the active slot remains set to the given
    /// slot.
    ///
    /// The active slot is what controls which slot is booted. Setting the slot as unbootable is an extra guard
    /// so the bootloader does not attempt to boot a known bad slot. If setting the active slot succeeds but
    /// setting the opposite slot as unbootable fails, the device will still boot into a known good slot.
    /// However, if there are other users of the boot control service, care should be taken to not
    /// set the active boot slot to the known bad slot.
    async fn revert_to_slot(&self, slot: i32) -> SdvResult<()>;
}

pub struct BinderBootControlService {
    boot_control_service: Strong<dyn IBootControlAsync<Tokio>>,
}

impl BinderBootControlService {
    pub async fn new() -> SdvResult<Self> {
        const DESCRIPTOR: &str = "android.hardware.boot.IBootControl/default";

        let boot_control_service: Strong<dyn IBootControlAsync<Tokio>> =
            binder_tokio::wait_for_interface(DESCRIPTOR).await.map_err(|e| {
                SdvStatus::with_message(
                    SdvStatusCode::Unavailable,
                    format!("Failed to find IBootControl interface: {e}"),
                )
            })?;

        Ok(BinderBootControlService { boot_control_service })
    }
}

#[async_trait]
impl BootControlService for BinderBootControlService {
    async fn get_current_slot(&self) -> SdvResult<i32> {
        let current_slot = self
            .boot_control_service
            .getCurrentSlot()
            .await
            .map_err(derive!(SdvStatusCode::Internal, "IBootControl::getCurrentSlot() failed"))?;

        Ok(current_slot)
    }
    async fn set_active_boot_slot(&self, slot_id: i32) -> SdvResult<()> {
        self.boot_control_service.setActiveBootSlot(slot_id).await.map_err(derive!(
            SdvStatusCode::Internal,
            format!("IBootControl::setActiveBootSlot({}) failed", slot_id)
        ))?;
        Ok(())
    }
    async fn mark_boot_successful(&self) -> SdvResult<()> {
        self.boot_control_service.markBootSuccessful().await.map_err(derive!(
            SdvStatusCode::Internal,
            "IBootControl::markBootSuccessful() failed"
        ))?;
        Ok(())
    }

    async fn is_slot_marked_successful(&self, slot: i32) -> SdvResult<bool> {
        let is_slot_marked_successful =
            self.boot_control_service.isSlotMarkedSuccessful(slot).await.map_err(derive!(
                SdvStatusCode::Internal,
                format!("IBootControl::isSlotMarkedSuccessful({}) failed", slot)
            ))?;
        Ok(is_slot_marked_successful)
    }

    async fn is_slot_bootable(&self, slot: i32) -> SdvResult<bool> {
        let is_slot_bootable =
            self.boot_control_service.isSlotBootable(slot).await.map_err(derive!(
                SdvStatusCode::Internal,
                format!("IBootControl::isSlotBootable({}) failed", slot)
            ))?;
        Ok(is_slot_bootable)
    }

    async fn set_slot_as_unbootable(&self, slot: i32) -> SdvResult<()> {
        self.boot_control_service.setSlotAsUnbootable(slot).await.map_err(derive!(
            SdvStatusCode::Internal,
            format!("IBootControl::setSlotAsUnbootable({}) failed", slot)
        ))?;
        Ok(())
    }

    async fn revert_to_slot(&self, slot: i32) -> SdvResult<()> {
        let opposite_slot = self
            .set_active_boot_slot(slot)
            .await
            .map_err(derive!(SdvStatusCode::Internal, "Failed when reverting the active boot slot"))
            .and_then(|_| {
                get_opposite_slot(slot).map_err(derive!(
                    SdvStatusCode::Internal,
                    "Failed when getting the opposite slot"
                ))
            })?;

        self.set_slot_as_unbootable(opposite_slot).await.map_err(derive!(
            SdvStatusCode::Internal,
            "Failed when setting the opposite slot as unbootable"
        ))
    }
}

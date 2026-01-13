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

use crate::update_engine::UpdateEngineClient;
use sdv_status_rs::{SdvResult, SdvStatus, SdvStatusCode};
use std::collections::HashMap;
use std::path::Path;

// Check whether the given argument is valid. If not return an Err(SdvStatus).
// Example usages:
// validate_argument!(x >= 0, "x should be greater than or equal to zero");
// validate_argument!(y < 0, "y should be less than zero");
macro_rules! validate_argument {
    ($argument_condition:expr, $error_message:expr) => {
        if !($argument_condition) {
            return Err(SdvStatus::with_message(SdvStatusCode::InvalidArgument, $error_message));
        }
    };
}

fn header_map_as_string_pairs(map: &HashMap<String, String>) -> Vec<String> {
    map.iter().map(|(k, v)| format!("{k}={v}")).collect::<Vec<String>>()
}

#[derive(Clone, Debug, PartialEq)]
pub struct SystemUpdatePayload {
    pub payload: String,
    pub payload_offset: i64,
    pub payload_size: i64,
    pub header_key_value_pairs: HashMap<String, String>,
}

impl SystemUpdatePayload {
    pub fn new(
        payload: &str,
        payload_offset: i64,
        payload_size: i64,
        header_key_value_pairs: &HashMap<String, String>,
    ) -> SdvResult<Self> {
        validate_argument!(
            Path::new(payload).is_absolute(),
            "The payload path must be an absolute path"
        );
        validate_argument!(payload_offset >= 0, "The payload_offset must be non-negative");
        validate_argument!(payload_size >= 0, "The payload_size must be non-negative");
        Ok(SystemUpdatePayload {
            payload: payload.to_string(),
            payload_offset,
            payload_size,
            header_key_value_pairs: header_key_value_pairs.clone(),
        })
    }

    pub async fn apply(
        &self,
        update_engine_client: &UpdateEngineClient,
        switch_slot_on_boot: bool,
    ) -> SdvResult<()> {
        const HEADER_KEY_RUN_POST_INSTALL: &str = "RUN_POST_INSTALL";
        const HEADER_KEY_SWITCH_SLOT_ON_REBOOT: &str = "SWITCH_SLOT_ON_REBOOT";
        const HEADER_VALUE_ENABLED: &str = "1";
        const HEADER_VALUE_DISABLED: &str = "0";

        // Ensure that the post install step is ran and that the slot is not switched
        let mut headers_map = self.header_key_value_pairs.clone();
        headers_map
            .insert(HEADER_KEY_RUN_POST_INSTALL.to_string(), HEADER_VALUE_ENABLED.to_string());

        let switch_slot_on_boot_str =
            if switch_slot_on_boot { HEADER_VALUE_ENABLED } else { HEADER_VALUE_DISABLED };
        headers_map.insert(
            HEADER_KEY_SWITCH_SLOT_ON_REBOOT.to_string(),
            switch_slot_on_boot_str.to_string(),
        );

        let header_key_value_pairs = header_map_as_string_pairs(&headers_map);

        update_engine_client
            .apply_payload_fd(
                &self.payload,
                self.payload_offset,
                self.payload_size,
                &header_key_value_pairs,
            )
            .await
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct ServiceBundleUpdatePayload {
    pub apex_paths: Vec<String>,
    pub boot_attempts: i32,
}

impl ServiceBundleUpdatePayload {
    pub fn new(apex_paths: Vec<String>, boot_attempts: i32) -> SdvResult<Self> {
        for apex_path in &apex_paths {
            validate_argument!(
                Path::new(apex_path).is_absolute(),
                "The APEX path must be an absolute path"
            );
        }

        validate_argument!(boot_attempts > 0, "The number of boot attempts must be greater than 0");

        Ok(ServiceBundleUpdatePayload { apex_paths, boot_attempts })
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum UpdatePayload {
    System(SystemUpdatePayload),
    ServiceBundle(ServiceBundleUpdatePayload),
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_header_map_as_string_pairs_succeeds() {
        let input = HashMap::from([
            (String::from("a"), String::from("b")),
            (String::from("key"), String::from("value")),
        ]);
        let expected = vec![String::from("a=b"), String::from("key=value")];
        let string_pairs = header_map_as_string_pairs(&input);
        assert_eq!(string_pairs.len(), expected.len());

        for expected_item in expected {
            assert!(
                string_pairs.contains(&expected_item),
                "Did not find expected string pair {expected_item} in the output"
            );
        }
    }
}

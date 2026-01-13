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

use log::debug;
use sdv_status_rs::{derive, SdvResult, SdvStatus, SdvStatusCode};
use std::collections::HashMap;
use std::convert::TryFrom;
use std::fs::File;
use std::io::{Read, Seek};
use std::path::Path;
use zip::ZipArchive;

pub struct OtaPackage {
    pub zip_file_path: String,
    pub payload_info: PayloadInfo,
}
#[derive(Debug)]
pub struct PayloadInfo {
    pub offset: i64,
    pub size: i64,
    pub header_key_value_pairs: HashMap<String, String>,
}

impl OtaPackage {
    const PAYLOAD_BIN: &str = "payload.bin";
    const PAYLOAD_PROPERTIES_TXT: &str = "payload_properties.txt";
    const CARE_MAP_ENTRY_NAME: &str = "care_map.pb";
    const PAYLOAD_MAGIC_HEADER: &[u8; 4] = b"CrAU";
    //  The .zip local file header.  See https://en.wikipedia.org/wiki/ZIP_(file_format)#Local_file_header
    const LOCAL_FILE_HEADER_SIZE: i64 = 30;
    fn calculate_payload_offset_and_size<R: Read + Seek>(
        zip: &mut ZipArchive<R>,
    ) -> SdvResult<(i64, i64)> {
        // Find payload.bin entry.
        let mut payload_entry = match zip.by_name(Self::PAYLOAD_BIN) {
            Ok(entry) => entry,
            Err(e) => {
                return Err(SdvStatus::with_message(
                    SdvStatusCode::Internal,
                    format!("Failed to find payload.bin: {e}"),
                ))
            }
        };

        // Calculate the offset.
        let offset = i64::try_from(payload_entry.header_start())
            .map_err(derive!(SdvStatusCode::Internal, "Payload offset too large"))?
            + Self::LOCAL_FILE_HEADER_SIZE
            + i64::try_from(payload_entry.name_raw().len())
                .map_err(derive!(SdvStatusCode::Internal, "Filename length too large"))?
            + i64::try_from(payload_entry.extra_data().len())
                .map_err(derive!(SdvStatusCode::Internal, "Extra field length too large"))?;

        let size = i64::try_from(payload_entry.size())
            .map_err(derive!(SdvStatusCode::Internal, "Payload size too large"))?;

        debug!("Calculated offset: {offset}");
        debug!("Calculated size: {size}");
        let mut payload_header = [0u8; 4];
        payload_entry
            .read_exact(&mut payload_header)
            .map_err(derive!(SdvStatusCode::Internal, "Failed to read payload header"))?;

        if payload_header != *Self::PAYLOAD_MAGIC_HEADER {
            return Err(SdvStatus::with_message(
                SdvStatusCode::InvalidArgument,
                format!(
                    "Invalid header, expected {:?}, got {:?}. Either the offset is not correct, or payload is corrupted",
                    Self::PAYLOAD_MAGIC_HEADER, payload_header
                ),
            ));
        }
        Ok((offset, size))
    }
    /// Creates a new OtaPackage instance by opening and parsing the given ZIP file.
    pub fn with_zip_file(zip_file_path: &str) -> SdvResult<Self> {
        if !Path::new(zip_file_path).is_absolute() {
            return Err(SdvStatus::with_message(
                SdvStatusCode::InvalidArgument,
                "The payload path must be an absolute path",
            ));
        }
        let file = File::open(zip_file_path)
            .map_err(derive!(SdvStatusCode::Internal, "Failed to open zip file"))?;
        let mut zip = ZipArchive::new(file)
            .map_err(derive!(SdvStatusCode::Internal, "Failed to open ZIP archive"))?;

        let (offset, size) = Self::calculate_payload_offset_and_size(&mut zip)?;
        let mut header_key_value_pairs = HashMap::new();
        match zip.by_name(Self::PAYLOAD_PROPERTIES_TXT) {
            Ok(mut file) => {
                let mut contents = String::new();
                file.read_to_string(&mut contents)
                    .map_err(derive!(SdvStatusCode::Internal, "Failed to read properties file"))?;
                for line in contents.lines() {
                    if let Some((key, value)) = line.split_once('=') {
                        let key = key.trim().to_string();
                        let value = value.trim().to_string();
                        header_key_value_pairs.insert(key, value);
                    }
                }
            }
            Err(e) => {
                return Err(SdvStatus::with_message(
                    SdvStatusCode::Internal,
                    format!("Failed to open properties file: {e:?}"),
                ))
            }
        };

        Ok(OtaPackage {
            zip_file_path: zip_file_path.to_string(),
            payload_info: PayloadInfo { offset, size, header_key_value_pairs },
        })
    }
    pub fn extract_care_map(&self) -> SdvResult<Vec<u8>> {
        let zip_file = File::open(&self.zip_file_path).map_err(derive!(
            SdvStatusCode::Internal,
            "Failed to open ZIP file for care_map extraction"
        ))?;
        let mut zip = ZipArchive::new(zip_file).map_err(derive!(
            SdvStatusCode::Internal,
            "Failed to parse ZIP archive for care_map"
        ))?;

        let mut care_map_file = zip
            .by_name(Self::CARE_MAP_ENTRY_NAME)
            .map_err(derive!(SdvStatusCode::NotFound, "care_map.pb not found in ZIP"))?;

        let mut buffer = Vec::new();
        care_map_file
            .read_to_end(&mut buffer)
            .map_err(derive!(SdvStatusCode::Internal, "Failed to read care_map.pb"))?;
        Ok(buffer)
    }
}

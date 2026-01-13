#!/bin/bash
# Copyright (C) 2026 The Android Open Source Project
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# Usage: make_test_ota_payload.sh <path to ota_payload.zip>
#
# This script takes a valid OTA payload .zip file and creates a small .zip file
# by limiting the payload.bin file to just the first 100 bytes. This is enough
# to perform the verification of the header, which is performed by the SDV
# Update Manager during the prepare request. At the time of writing this
# reduces the OTA payload .zip file from 223MB to 8.3KB, making it possible to
# commit it to the repository's test data.

set -e

# Unzip the first 100 bytes of the payload.bin file
unzip -p $1 payload.bin | head -c 100 > payload.bin

# Replace the large payload.bin in the zip file with the small one.
# "-m" removes the payload.bin file that we created above.
zip -m $1 payload.bin


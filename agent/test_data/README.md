<!--
  Copyright (C) 2026 The Android Open Source Project

  Licensed under the Apache License, Version 2.0 (the "License");
  you may not use this file except in compliance with the License.
  You may obtain a copy of the License at

      http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  See the License for the specific language governing permissions and
  limitations under the License.
-->

# SDV Update Manager Test Data

-   `test.apex` is a copy of the APEX found at
    `packages/modules/Virtualization/libs/apkverify/tests/data/test.apex`
-   `bad-signed-test.apex` is a copy of the APK found at
    `packages/modules/Virtualization/libs/apkverify/tests/data/v3-only-with-dsa-sha256-2048-sig-does-not-verify.apk`.
    Although this is an APK file, it serves the purposes of a bad signed APEX to
    test the resilience of the Update Manager.
-   `ota_payload.zip` is a built OTA package that has been reduced in size using
    the `../scripts/make_test_ota_payload.sh` script. Note that this is not a
    valid OTA package. Its signature is invalid and the payload is incomplete.
    It should only be used for testing the OTA payload parsing logic.

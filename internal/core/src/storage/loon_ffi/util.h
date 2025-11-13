// Copyright (C) 2019-2020 Zilliz. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software distributed under the License
// is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
// or implied. See the License for the specific language governing permissions and limitations under the License

#pragma once

#include <memory>
#include "common/common_type_c.h"
#include "common/type_c.h"
#include "milvus-storage/ffi_c.h"

/**
 * @brief Creates a shared pointer to Properties from CStorageConfig
 *
 * This utility function converts a CStorageConfig structure into a Properties
 * object by calling the FFI properties_create function. All configuration fields
 * from CStorageConfig are mapped to corresponding key-value pairs in Properties.
 *
 * The following fields are converted:
 * - String fields: address, bucket_name, access_key_id, access_key_value,
 *   root_path, storage_type, cloud_provider, iam_endpoint, log_level,
 *   region, ssl_ca_cert, gcp_credential_json
 * - Boolean fields: use_ssl, use_iam, use_virtual_host, use_custom_part_upload
 * - Integer fields: request_timeout_ms, max_connections
 *
 * @param c_storage_config The storage configuration to convert
 * @return std::shared_ptr<Properties> Shared pointer to the created Properties
 * @throws std::runtime_error If properties_create fails with error message from FFI
 */
std::shared_ptr<Properties>
MakePropertiesFromStorageConfig(CStorageConfig c_storage_config);
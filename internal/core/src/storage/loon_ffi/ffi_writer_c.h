// Copyright 2023 Zilliz
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#pragma once

#ifdef __cplusplus
extern "C" {
#endif

#include "common/common_type_c.h"
#include "common/type_c.h"

/**
 * @brief Retrieves encryption parameters from the cipher plugin for CMEK (Customer Managed Encryption Keys).
 *
 * This function loads the cipher plugin, updates it with the provided plugin context,
 * and retrieves the encryption key and metadata required for encrypting data in storage.
 *
 * @param[in] c_plugin_context Pointer to the plugin context containing:
 *                             - ez_id: Encryption zone ID
 *                             - collection_id: The collection ID
 *                             - key: The encryption key string
 * @param[out] out_key Pointer to receive the encryption key (caller must free with free())
 * @param[out] out_meta Pointer to receive the encoded key metadata (caller must free with free())
 *
 * @return CStatus Success status or error with message if failed
 *
 * @note The caller is responsible for freeing the allocated out_key and out_meta strings.
 */
CStatus
GetEncParams(CPluginContext* c_plugin_context, char** out_key, char** out_meta);

#ifdef __cplusplus
}
#endif
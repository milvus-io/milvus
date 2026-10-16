// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#pragma once

#include "index/Utils.h"

namespace milvus::index::vector_load_params {

// Loader and resource metadata historically used GetValueFromConfig<bool>,
// including its permissive string handling. Their key orders differ, so share
// only the single-key primitive and let each boundary preserve its order.
inline bool
ReadLenientIdMapMmapFlag(const Config& config, const char* key) {
    return GetValueFromConfig<bool>(config, key).value_or(false);
}

}  // namespace milvus::index::vector_load_params

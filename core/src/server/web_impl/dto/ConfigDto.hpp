// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#pragma once

#include "server/web_impl/dto/Dto.h"

namespace milvus {
namespace server {
namespace web {

#include OATPP_CODEGEN_BEGIN(DTO)

class AdvancedConfigDto : public OObject {
    DTO_INIT(AdvancedConfigDto, Object);

    DTO_FIELD(Int64, cpu_cache_capacity);
    DTO_FIELD(Boolean, cache_insert_data);
    DTO_FIELD(Int64, use_blas_threshold);
    DTO_FIELD(Int64, gpu_search_threshold);
};

class GPUConfigDto : public OObject {
    DTO_INIT(GPUConfigDto, Object);

    DTO_FIELD(Boolean, enable) = false;
    DTO_FIELD(Int64, cache_capacity);
    DTO_FIELD(List<String>::ObjectWrapper, search_resources);
    DTO_FIELD(List<String>::ObjectWrapper, build_index_resources);
};

#include OATPP_CODEGEN_END(DTO)

} // namespace web
} // namespace server
} // namespace milvus

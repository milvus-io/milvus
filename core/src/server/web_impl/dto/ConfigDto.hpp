// Copyright (C) 2019-2020 Zilliz. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software distributed under the License
// is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
// or implied. See the License for the specific language governing permissions and limitations under the License.

#pragma once

#include "server/web_impl/Constants.h"
#include "server/web_impl/dto/Dto.h"

namespace milvus::server::web {

#include OATPP_CODEGEN_BEGIN(DTO)

class AdvancedConfigDto : public ODTO {
    DTO_INIT(AdvancedConfigDto, DTO);

    DTO_FIELD(Int64, cpu_cache_capacity) = VALUE_CONFIG_CPU_CACHE_CAPACITY_DEFAULT;
    DTO_FIELD(Boolean, cache_insert_data) = VALUE_CONFIG_CACHE_INSERT_DATA_DEFAULT;
    DTO_FIELD(Int64, use_blas_threshold) = 1100;

#ifdef MILVUS_GPU_VERSION
    DTO_FIELD(Int64, gpu_search_threshold) = 1000;

#endif
};

using AdvancedConfigDtoT = oatpp::Object<AdvancedConfigDto>;

class GPUConfigDto : public ODTO {
    DTO_INIT(GPUConfigDto, DTO);

    DTO_FIELD(Boolean, enable) = true;
    DTO_FIELD(Int64, cache_capacity) = 1;
    DTO_FIELD(List<String>::ObjectWrapper, search_resources);
    DTO_FIELD(List<String>::ObjectWrapper, build_index_resources);
};

#include OATPP_CODEGEN_END(DTO)

using GPUConfigDtoT = oatpp::Object<GPUConfigDto>;

} // namespace milvus::server::web

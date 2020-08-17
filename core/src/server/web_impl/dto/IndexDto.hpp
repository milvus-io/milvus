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

#include "server/web_impl/dto/Dto.h"
#include "server/web_impl/dto/StatusDto.hpp"
#include "server/web_impl/Constants.h"

namespace milvus::server::web {

#include OATPP_CODEGEN_BEGIN(DTO)

class IndexRequestDto : public ODTO {
 DTO_INIT(IndexRequestDto, DTO)

    DTO_FIELD(String, index_type) = VALUE_INDEX_INDEX_TYPE_DEFAULT;

    DTO_FIELD(String, params);
};

using IndexDto = IndexRequestDto;

#include OATPP_CODEGEN_END(DTO)

} // namespace milvus::server::web

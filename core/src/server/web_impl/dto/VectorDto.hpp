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
#include "server/web_impl/Constants.h"

namespace milvus {
namespace server {
namespace web {

#include OATPP_CODEGEN_BEGIN(DTO)

class VectorIdsDto : public oatpp::data::mapping::type::Object {
    DTO_INIT(VectorIdsDto, Object)

    DTO_FIELD(List<String>::ObjectWrapper, ids);
};

class ResultDto : public oatpp::data::mapping::type::Object {
    DTO_INIT(ResultDto, Object)

    DTO_FIELD(String, id);
    DTO_FIELD(String, dit, "distance");
};

class TopkResultsDto : public OObject {
    DTO_INIT(TopkResultsDto, Object);

    DTO_FIELD(Int64, num);
    DTO_FIELD(List<List<ResultDto::ObjectWrapper>::ObjectWrapper>::ObjectWrapper, results);
};

#include OATPP_CODEGEN_END(DTO)

} // namespace web
} // namespace server
} // namespace milvus

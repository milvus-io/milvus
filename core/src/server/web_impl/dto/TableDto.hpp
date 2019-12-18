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
#include "server/web_impl/dto/StatusDto.hpp"

namespace milvus {
namespace server {
namespace web {

#include OATPP_CODEGEN_BEGIN(DTO)


class TableRequestDto : public oatpp::data::mapping::type::Object {
 DTO_INIT(TableRequestDto, Object)

    DTO_FIELD(String, table_name, "table-name");
    DTO_FIELD(Int64, dimension, "dimension");
    DTO_FIELD(Int64, index_file_size, "index-file-size");
    DTO_FIELD(Int64, metric_type, "metric-type");
};

class TableFieldsDto : public oatpp::data::mapping::type::Object {
 DTO_INIT(TableFieldsDto, Object)

    DTO_FIELD(Fields<String>::ObjectWrapper, schema, "schema");
};

class TableListDto : public oatpp::data::mapping::type::Object {
 DTO_INIT(TableListDto, Object)

    DTO_FIELD(List<String>::ObjectWrapper, tables);
};

class RangeDto : public oatpp::data::mapping::type::Object {
 DTO_INIT(RangeDto, Object)
};

#include OATPP_CODEGEN_END(DTO)

} // namespace web
} // namespace server
} // namespace milvus
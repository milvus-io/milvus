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
#include "server/web_impl/Constants.h"

namespace milvus {
namespace server {
namespace web {

#include OATPP_CODEGEN_BEGIN(DTO)

class RowRecordDto : public oatpp::data::mapping::type::Object {
    DTO_INIT(RowRecordDto, Object)

    DTO_FIELD(List<Float32>::ObjectWrapper, record);
};

class RecordsDto : public oatpp::data::mapping::type::Object {
    DTO_INIT(RecordsDto, Object)

    DTO_FIELD(List<RowRecordDto::ObjectWrapper>::ObjectWrapper, records);
};

class SearchRequestDto : public OObject {
    DTO_INIT(SearchRequestDto, Object)

    DTO_FIELD(Int64, topk);
    DTO_FIELD(Int64, nprobe);
    DTO_FIELD(List<String>::ObjectWrapper, tags);
    DTO_FIELD(List<String>::ObjectWrapper, file_ids);
    DTO_FIELD(List<List<Float32>::ObjectWrapper>::ObjectWrapper, records);
    DTO_FIELD(List<List<Int64>::ObjectWrapper>::ObjectWrapper, records_bin);
};

class InsertRequestDto : public oatpp::data::mapping::type::Object {
    DTO_INIT(InsertRequestDto, Object)

    DTO_FIELD(String, tag) = VALUE_PARTITION_TAG_DEFAULT;
    DTO_FIELD(List<List<Float32>::ObjectWrapper>::ObjectWrapper, records);
    DTO_FIELD(List<List<Int64>::ObjectWrapper>::ObjectWrapper, records_bin);
    DTO_FIELD(List<Int64>::ObjectWrapper, ids);
};

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

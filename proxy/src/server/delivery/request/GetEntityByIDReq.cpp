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

#include "server/delivery/request/GetEntityByIDReq.h"

#include "db/Types.h"
#include "server/ValidationUtil.h"
#include "utils/TimeRecorder.h"

#include <memory>
#include <vector>

namespace milvus {
namespace server {

constexpr uint64_t MAX_COUNT_RETURNED = 1000;

GetEntityByIDReq::GetEntityByIDReq(const ContextPtr& context, const std::string& collection_name,
                                   const engine::IDNumbers& id_array, std::vector<std::string>& field_names,
                                   std::vector<bool>& valid_row,
                                   engine::DataChunkPtr& data_chunk)
    : BaseReq(context, ReqType::kGetEntityByID),
      collection_name_(collection_name),
      id_array_(id_array),
      field_names_(field_names),
      valid_row_(valid_row),
      data_chunk_(data_chunk) {
}

BaseReqPtr
GetEntityByIDReq::Create(const ContextPtr& context, const std::string& collection_name,
                         const engine::IDNumbers& id_array, std::vector<std::string>& field_names_,
                         std::vector<bool>& valid_row,
                         engine::DataChunkPtr& data_chunk) {
    return std::shared_ptr<BaseReq>(
        new GetEntityByIDReq(context, collection_name, id_array, field_names_, valid_row, data_chunk));
}

Status
GetEntityByIDReq::OnExecute() {

    return Status::OK();
}

}  // namespace server
}  // namespace milvus

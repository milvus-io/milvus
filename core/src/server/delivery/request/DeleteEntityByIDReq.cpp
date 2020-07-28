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

#include "server/delivery/request/DeleteEntityByIDReq.h"

#include <memory>
#include <string>
#include <unordered_map>
#include <vector>

#include "server/DBWrapper.h"
#include "server/ValidationUtil.h"
#include "utils/Log.h"
#include "utils/TimeRecorder.h"

namespace milvus {
namespace server {

DeleteEntityByIDReq::DeleteEntityByIDReq(const std::shared_ptr<milvus::server::Context>& context,
                                         const std::string& collection_name, const engine::IDNumbers& entity_ids)
    : BaseReq(context, BaseReq::kDeleteEntityByID), collection_name_(collection_name), entity_ids_(entity_ids) {
}

BaseReqPtr
DeleteEntityByIDReq::Create(const std::shared_ptr<milvus::server::Context>& context, const std::string& collection_name,
                            const engine::IDNumbers& entity_ids) {
    return std::shared_ptr<BaseReq>(new DeleteEntityByIDReq(context, collection_name, entity_ids));
}

Status
DeleteEntityByIDReq::OnExecute() {
    try {
        TimeRecorderAuto rc("DeleteEntityByIDReq");

        bool exist = false;
        auto status = DBWrapper::DB()->HasCollection(collection_name_, exist);
        if (!exist) {
            return Status(SERVER_COLLECTION_NOT_EXIST, CollectionNotExistMsg(collection_name_));
        }

#ifdef MILVUS_SUPPORT_SPTAG
        /* Check collection's index type supports delete */
        if (collection_schema.engine_type_ == (int32_t)engine::EngineType::SPTAG_BKT ||
            collection_schema.engine_type_ == (int32_t)engine::EngineType::SPTAG_KDT) {
            std::string err_msg =
                "Index type " + std::to_string(collection_schema.engine_type_) + " does not support delete operation";
            LOG_SERVER_ERROR_ << err_msg;
            return Status(SERVER_UNSUPPORTED_ERROR, err_msg);
        }
#endif

        STATUS_CHECK(DBWrapper::DB()->DeleteEntityByID(collection_name_, entity_ids_));
    } catch (std::exception& ex) {
        return Status(SERVER_UNEXPECTED_ERROR, ex.what());
    }

    return Status::OK();
}

}  // namespace server
}  // namespace milvus

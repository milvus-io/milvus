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

#include "server/delivery/request/CountCollectionRequest.h"
#include "BaseRequest.h"
#include "server/DBWrapper.h"
#include "server/ValidationUtil.h"
#include "utils/Log.h"
#include "utils/TimeRecorder.h"

#include <fiu-local.h>
#include <memory>
#include <unordered_map>
#include <vector>

namespace milvus {
namespace server {

CountCollectionRequest::CountCollectionRequest(const std::shared_ptr<milvus::server::Context>& context,
                                               const std::string& collection_name, int64_t& row_count)
    : BaseRequest(context, BaseRequest::kCountCollection), collection_name_(collection_name), row_count_(row_count) {
}

BaseRequestPtr
CountCollectionRequest::Create(const std::shared_ptr<milvus::server::Context>& context,
                               const std::string& collection_name, int64_t& row_count) {
    return std::shared_ptr<BaseRequest>(new CountCollectionRequest(context, collection_name, row_count));
}

Status
CountCollectionRequest::OnExecute() {
    try {
        std::string hdr = "CountCollectionRequest(collection=" + collection_name_ + ")";
        TimeRecorderAuto rc(hdr);

        // step 1: check arguments
        auto status = ValidateCollectionName(collection_name_);
        if (!status.ok()) {
            return status;
        }

        // only process root collection, ignore partition collection
        engine::snapshot::CollectionPtr collection;
        std::unordered_map<engine::snapshot::FieldPtr, std::vector<engine::snapshot::FieldElementPtr>> fields_schema;
        status = DBWrapper::SSDB()->DescribeCollection(collection_name_, collection, fields_schema);
        if (!status.ok()) {
            if (status.code() == DB_NOT_FOUND) {
                return Status(SERVER_COLLECTION_NOT_EXIST, CollectionNotExistMsg(collection_name_));
            } else {
                return status;
            }
        }

        rc.RecordSection("check validation");

        // step 2: get row count
        uint64_t row_count = 0;
        status = DBWrapper::SSDB()->GetCollectionRowCount(collection_name_, row_count);
        fiu_do_on("CountCollectionRequest.OnExecute.db_not_found", status = Status(DB_NOT_FOUND, ""));
        fiu_do_on("CountCollectionRequest.OnExecute.status_error", status = Status(SERVER_UNEXPECTED_ERROR, ""));
        fiu_do_on("CountCollectionRequest.OnExecute.throw_std_exception", throw std::exception());
        if (!status.ok()) {
            if (status.code() == DB_NOT_FOUND) {
                return Status(SERVER_COLLECTION_NOT_EXIST, CollectionNotExistMsg(collection_name_));
            } else {
                return status;
            }
        }

        row_count_ = static_cast<int64_t>(row_count);
    } catch (std::exception& ex) {
        return Status(SERVER_UNEXPECTED_ERROR, ex.what());
    }

    return Status::OK();
}

}  // namespace server
}  // namespace milvus

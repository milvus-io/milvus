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

#include "server/delivery/request/HasTableRequest.h"
#include "server/DBWrapper.h"
#include "utils/Log.h"
#include "utils/TimeRecorder.h"
#include "utils/ValidationUtil.h"

#include <fiu-local.h>
#include <memory>

namespace milvus {
namespace server {

HasTableRequest::HasTableRequest(const std::shared_ptr<milvus::server::Context>& context,
                                 const std::string& collection_name, bool& has_table)
    : BaseRequest(context, BaseRequest::kHasTable), collection_name_(collection_name), has_table_(has_table) {
}

BaseRequestPtr
HasTableRequest::Create(const std::shared_ptr<milvus::server::Context>& context, const std::string& collection_name,
                        bool& has_table) {
    return std::shared_ptr<BaseRequest>(new HasTableRequest(context, collection_name, has_table));
}

Status
HasTableRequest::OnExecute() {
    try {
        std::string hdr = "HasTableRequest(collection=" + collection_name_ + ")";
        TimeRecorderAuto rc(hdr);

        // step 1: check arguments
        auto status = ValidationUtil::ValidateCollectionName(collection_name_);
        if (!status.ok()) {
            return status;
        }

        // step 2: check table existence
        status = DBWrapper::DB()->HasNativeTable(collection_name_, has_table_);
        fiu_do_on("HasTableRequest.OnExecute.throw_std_exception", throw std::exception());

        // only process root collection, ignore partition collection
        if (has_table_) {
            engine::meta::CollectionSchema table_schema;
            table_schema.collection_id_ = collection_name_;
            status = DBWrapper::DB()->DescribeTable(table_schema);
            if (!table_schema.owner_table_.empty()) {
                has_table_ = false;
            }
        }
    } catch (std::exception& ex) {
        return Status(SERVER_UNEXPECTED_ERROR, ex.what());
    }

    return Status::OK();
}

}  // namespace server
}  // namespace milvus

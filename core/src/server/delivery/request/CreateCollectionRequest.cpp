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

#include "server/delivery/request/CreateCollectionRequest.h"
#include "db/Utils.h"
#include "server/DBWrapper.h"
#include "server/delivery/request/BaseRequest.h"
#include "utils/Log.h"
#include "utils/TimeRecorder.h"
#include "utils/ValidationUtil.h"

#include <fiu-local.h>
#include <memory>
#include <string>

namespace milvus {
namespace server {

CreateCollectionRequest::CreateCollectionRequest(const std::shared_ptr<milvus::server::Context>& context,
                                                 const std::string& collection_name, int64_t dimension,
                                                 int64_t index_file_size, int64_t metric_type)
    : BaseRequest(context, BaseRequest::kCreateCollection),
      collection_name_(collection_name),
      dimension_(dimension),
      index_file_size_(index_file_size),
      metric_type_(metric_type) {
}

BaseRequestPtr
CreateCollectionRequest::Create(const std::shared_ptr<milvus::server::Context>& context,
                                const std::string& collection_name, int64_t dimension, int64_t index_file_size,
                                int64_t metric_type) {
    return std::shared_ptr<BaseRequest>(
        new CreateCollectionRequest(context, collection_name, dimension, index_file_size, metric_type));
}

Status
CreateCollectionRequest::OnExecute() {
    std::string hdr =
        "CreateCollectionRequest(collection=" + collection_name_ + ", dimension=" + std::to_string(dimension_) + ")";
    TimeRecorderAuto rc(hdr);

    try {
        // step 1: check arguments
        auto status = ValidationUtil::ValidateCollectionName(collection_name_);
        if (!status.ok()) {
            return status;
        }

        status = ValidationUtil::ValidateTableDimension(dimension_, metric_type_);
        if (!status.ok()) {
            return status;
        }

        status = ValidationUtil::ValidateCollectionIndexFileSize(index_file_size_);
        fiu_do_on("CreateCollectionRequest.OnExecute.invalid_index_file_size",
                  status = Status(milvus::SERVER_UNEXPECTED_ERROR, ""));
        if (!status.ok()) {
            return status;
        }

        status = ValidationUtil::ValidateCollectionIndexMetricType(metric_type_);
        if (!status.ok()) {
            return status;
        }

        rc.RecordSection("check validation");

        // step 2: construct collection schema
        engine::meta::CollectionSchema collection_info;
        collection_info.collection_id_ = collection_name_;
        collection_info.dimension_ = static_cast<uint16_t>(dimension_);
        collection_info.index_file_size_ = index_file_size_;
        collection_info.metric_type_ = metric_type_;

        // some metric type only support binary vector, adapt the index type
        if (engine::utils::IsBinaryMetricType(metric_type_)) {
            if (collection_info.engine_type_ == static_cast<int32_t>(engine::EngineType::FAISS_IDMAP)) {
                collection_info.engine_type_ = static_cast<int32_t>(engine::EngineType::FAISS_BIN_IDMAP);
            } else if (collection_info.engine_type_ == static_cast<int32_t>(engine::EngineType::FAISS_IVFFLAT)) {
                collection_info.engine_type_ = static_cast<int32_t>(engine::EngineType::FAISS_BIN_IVFFLAT);
            }
        }

        // step 3: create collection
        status = DBWrapper::DB()->CreateCollection(collection_info);
        fiu_do_on("CreateCollectionRequest.OnExecute.db_already_exist", status = Status(milvus::DB_ALREADY_EXIST, ""));
        fiu_do_on("CreateCollectionRequest.OnExecute.create_collection_fail",
                  status = Status(milvus::SERVER_UNEXPECTED_ERROR, ""));
        fiu_do_on("CreateCollectionRequest.OnExecute.throw_std_exception", throw std::exception());
        if (!status.ok()) {
            // collection could exist
            if (status.code() == DB_ALREADY_EXIST) {
                return Status(SERVER_INVALID_COLLECTION_NAME, status.message());
            }
            return status;
        }
    } catch (std::exception& ex) {
        return Status(SERVER_UNEXPECTED_ERROR, ex.what());
    }

    return Status::OK();
}

}  // namespace server
}  // namespace milvus

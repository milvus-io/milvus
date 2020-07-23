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

#include "server/delivery/request/CreateIndexRequest.h"
#include "db/Utils.h"
#include "server/DBWrapper.h"
#include "server/ValidationUtil.h"
#include "utils/Log.h"
#include "utils/TimeRecorder.h"

#include <fiu-local.h>
#include <memory>
#include <string>

namespace milvus {
namespace server {

CreateIndexRequest::CreateIndexRequest(const std::shared_ptr<milvus::server::Context>& context,
                                       const std::string& collection_name, const std::string& field_name,
                                       const std::string& index_name, const milvus::json& json_params)
    : BaseRequest(context, BaseRequest::kCreateIndex),
      collection_name_(collection_name),
      field_name_(field_name),
      index_name_(index_name),
      json_params_(json_params) {
}

BaseRequestPtr
CreateIndexRequest::Create(const std::shared_ptr<milvus::server::Context>& context, const std::string& collection_name,
                           const std::string& field_name, const std::string& index_name,
                           const milvus::json& json_params) {
    return std::shared_ptr<BaseRequest>(
        new CreateIndexRequest(context, collection_name, field_name, index_name, json_params));
}

Status
CreateIndexRequest::OnExecute() {
    try {
        std::string hdr = "CreateIndexRequest(collection=" + collection_name_ + ")";
        TimeRecorderAuto rc(hdr);

        // step 1: check arguments
        auto status = ValidateCollectionName(collection_name_);
        if (!status.ok()) {
            return status;
        }

        status = ValidateFieldName(field_name_);
        if (!status.ok()) {
            return status;
        }

        status = ValidateIndexName(index_name_);
        if (!status.ok()) {
            return status;
        }

        // only process root collection, ignore partition collection
        engine::meta::CollectionSchema collection_schema;
        engine::meta::hybrid::FieldsSchema fields_schema;
        collection_schema.collection_id_ = collection_name_;
        status = DBWrapper::DB()->DescribeHybridCollection(collection_schema, fields_schema);
        fiu_do_on("CreateIndexRequest.OnExecute.not_has_collection",
                  status = Status(milvus::SERVER_UNEXPECTED_ERROR, ""));
        fiu_do_on("CreateIndexRequest.OnExecute.throw_std.exception", throw std::exception());
        if (!status.ok()) {
            if (status.code() == DB_NOT_FOUND) {
                return Status(SERVER_COLLECTION_NOT_EXIST, CollectionNotExistMsg(collection_name_));
            } else {
                return status;
            }
        } else {
            if (!collection_schema.owner_collection_.empty()) {
                return Status(SERVER_INVALID_COLLECTION_NAME, CollectionNotExistMsg(collection_name_));
            }
        }

        int32_t index_type = 0;
        if (json_params_.contains("index_type")) {
            auto index_type_str = json_params_["index_type"].get<std::string>();
            if (engine::s_map_engine_type.find(index_type_str) == engine::s_map_engine_type.end()) {
                return Status(SERVER_INVALID_ARGUMENT, "Set wrong index type");
            }
            index_type = (int32_t)engine::s_map_engine_type.at(index_type_str);
        }

        status = ValidateCollectionIndexType(index_type);
        if (!status.ok()) {
            return status;
        }

        status = ValidateIndexParams(json_params_, collection_schema, index_type);
        if (!status.ok()) {
            return status;
        }

        // step 2: binary and float vector support different index/metric type, need to adapt here
        engine::meta::CollectionSchema collection_info;
        collection_info.collection_id_ = collection_name_;
        status = DBWrapper::DB()->DescribeCollection(collection_info);

        int32_t adapter_index_type = index_type;
        if (engine::utils::IsBinaryMetricType(collection_info.metric_type_)) {  // binary vector not allow
            if (adapter_index_type == static_cast<int32_t>(engine::EngineType::FAISS_IDMAP)) {
                adapter_index_type = static_cast<int32_t>(engine::EngineType::FAISS_BIN_IDMAP);
            } else if (adapter_index_type == static_cast<int32_t>(engine::EngineType::FAISS_IVFFLAT)) {
                adapter_index_type = static_cast<int32_t>(engine::EngineType::FAISS_BIN_IVFFLAT);
            }
            //            else {
            //                return Status(SERVER_INVALID_INDEX_TYPE, "Invalid index type for collection metric type");
            //            }
        }

        rc.RecordSection("check validation");

        // step 3: create index
        engine::CollectionIndex index;
        if (json_params_.contains("metric_type")) {
            index.metric_type_ = (int32_t)engine::s_map_metric_type.at(json_params_["metric_type"]);
        }

        index.engine_type_ = adapter_index_type;
        index.extra_params_ = json_params_;
        status = DBWrapper::DB()->CreateIndex(context_, collection_name_, index);
        fiu_do_on("CreateIndexRequest.OnExecute.create_index_fail",
                  status = Status(milvus::SERVER_UNEXPECTED_ERROR, ""));
        if (!status.ok()) {
            return status;
        }
    } catch (std::exception& ex) {
        return Status(SERVER_UNEXPECTED_ERROR, ex.what());
    }

    return Status::OK();
}

}  // namespace server
}  // namespace milvus

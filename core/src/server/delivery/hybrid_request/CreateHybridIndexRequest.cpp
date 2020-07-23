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

#include "server/delivery/hybrid_request/CreateHybridIndexRequest.h"
#include "db/Utils.h"
#include "server/DBWrapper.h"
#include "server/ValidationUtil.h"
#include "utils/Log.h"
#include "utils/TimeRecorder.h"

#include <fiu-local.h>
#include <memory>
#include <string>
#include <unordered_map>

namespace milvus {
namespace server {

CreateHybridIndexRequest::CreateHybridIndexRequest(const std::shared_ptr<milvus::server::Context>& context,
                                                   const std::string& collection_name,
                                                   const std::vector<std::string>& field_names,
                                                   const milvus::json& extra_params)
    : BaseRequest(context, BaseRequest::kCreateHybridIndex),
      collection_name_(collection_name),
      field_names_(field_names),
      extra_params_(extra_params) {
}

BaseRequestPtr
CreateHybridIndexRequest::Create(const std::shared_ptr<milvus::server::Context>& context,
                                 const std::string& collection_name, const std::vector<std::string>& field_names,
                                 const milvus::json& extra_params) {
    return std::shared_ptr<BaseRequest>(
        new CreateHybridIndexRequest(context, collection_name, field_names, extra_params));
}

Status
CreateHybridIndexRequest::OnExecute() {
    try {
        std::string hdr = "CreateIndexRequest(collection=" + collection_name_ + ")";
        TimeRecorderAuto rc(hdr);

        // step 1: check arguments
        auto status = ValidateCollectionName(collection_name_);
        if (!status.ok()) {
            return status;
        }

        // only process root collection, ignore partition collection
        engine::meta::CollectionSchema collection_schema;
        engine::meta::hybrid::FieldsSchema fields_schema;
        collection_schema.collection_id_ = collection_name_;
        status = DBWrapper::DB()->DescribeHybridCollection(collection_schema, fields_schema);

        std::unordered_map<std::string, engine::meta::hybrid::DataType> attr_types;
        for (const auto& schema : fields_schema.fields_schema_) {
            attr_types.insert(std::make_pair(schema.field_name_, (engine::meta::hybrid::DataType)schema.field_type_));
        }

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
        if (extra_params_.contains("index_type")) {
            std::string index_type_str = extra_params_["index_type"];
            index_type = (int32_t)engine::s_map_engine_type.at(index_type_str);
        }

        status = ValidateCollectionIndexType(index_type);
        if (!status.ok()) {
            return status;
        }

        status = ValidateIndexParams(extra_params_, collection_schema, index_type);
        if (!status.ok()) {
            return status;
        }

        // step 2: binary and float vector support different index/metric type, need to adapt here
        int32_t adapter_index_type = index_type;
        if (engine::utils::IsBinaryMetricType(collection_schema.metric_type_)) {  // binary vector not allow
            if (adapter_index_type == static_cast<int32_t>(engine::EngineType::FAISS_IDMAP)) {
                adapter_index_type = static_cast<int32_t>(engine::EngineType::FAISS_BIN_IDMAP);
            } else if (adapter_index_type == static_cast<int32_t>(engine::EngineType::FAISS_IVFFLAT)) {
                adapter_index_type = static_cast<int32_t>(engine::EngineType::FAISS_BIN_IVFFLAT);
            } else {
                return Status(SERVER_INVALID_INDEX_TYPE, "Invalid index type for collection metric type");
            }
        }

        rc.RecordSection("check validation");

        //        // step 3: create index
        //        status = DBWrapper::DB()->CreateStructuredIndex(context_, collection_name_, field_names_);
        //        fiu_do_on("CreateIndexRequest.OnExecute.create_index_fail",
        //                  status = Status(milvus::SERVER_UNEXPECTED_ERROR, ""));
        //        if (!status.ok()) {
        //            return status;
        //        }
        //
        //        engine::CollectionIndex index;
        //        index.engine_type_ = adapter_index_type;
        //        index.extra_params_ = extra_params_;
        //        status = DBWrapper::DB()->CreateIndex(context_, collection_name_, index);
        //        if (!status.ok()) {
        //            return status;
        //        }
    } catch (std::exception& ex) {
        return Status(SERVER_UNEXPECTED_ERROR, ex.what());
    }

    return Status::OK();
}

}  // namespace server
}  // namespace milvus

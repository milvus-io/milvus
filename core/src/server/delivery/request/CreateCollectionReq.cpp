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

#include "server/delivery/request/CreateCollectionReq.h"
#include "db/Utils.h"
#include "server/DBWrapper.h"
#include "server/ValidationUtil.h"
#include "server/delivery/request/BaseReq.h"
#include "server/web_impl/Constants.h"
#include "utils/Log.h"
#include "utils/TimeRecorder.h"

#include <fiu-local.h>
#include <src/db/snapshot/Context.h>
#include <memory>
#include <string>
#include <unordered_map>
#include <vector>

namespace milvus {
namespace server {

CreateCollectionReq::CreateCollectionReq(const std::shared_ptr<milvus::server::Context>& context,
                                         const std::string& collection_name,
                                         std::unordered_map<std::string, engine::meta::hybrid::DataType>& field_types,
                                         std::unordered_map<std::string, milvus::json>& field_index_params,
                                         std::unordered_map<std::string, std::string>& field_params,
                                         milvus::json& extra_params)
    : BaseReq(context, BaseReq::kCreateCollection),
      collection_name_(collection_name),
      field_types_(field_types),
      field_index_params_(field_index_params),
      field_params_(field_params),
      extra_params_(extra_params) {
}

BaseReqPtr
CreateCollectionReq::Create(const std::shared_ptr<milvus::server::Context>& context, const std::string& collection_name,
                            std::unordered_map<std::string, engine::meta::hybrid::DataType>& field_types,
                            std::unordered_map<std::string, milvus::json>& field_index_params,
                            std::unordered_map<std::string, std::string>& field_params, milvus::json& extra_params) {
    return std::shared_ptr<BaseReq>(
        new CreateCollectionReq(context, collection_name, field_types, field_index_params, field_params, extra_params));
}

Status
CreateCollectionReq::OnExecute() {
    std::string hdr = "CreateCollectionReq(collection=" + collection_name_ + ")";
    TimeRecorderAuto rc(hdr);

    try {
        // step 1: check arguments
        auto status = ValidateCollectionName(collection_name_);
        fiu_do_on("CreateCollectionReq.OnExecute.invalid_collection_name",
                  status = Status(milvus::SERVER_UNEXPECTED_ERROR, ""));
        if (!status.ok()) {
            return status;
        }

        rc.RecordSection("check validation");

        // step 2: construct collection schema and vector schema
        engine::meta::CollectionSchema collection_schema;
        engine::meta::hybrid::FieldsSchema fields_schema;

        uint16_t dimension = 0;
        milvus::json vector_param;
        for (auto& field_type : field_types_) {
            engine::meta::hybrid::FieldSchema schema;
            auto field_name = field_type.first;
            STATUS_CHECK(ValidateFieldName(field_name));

            auto index_params = field_index_params_.at(field_name);
            schema.collection_id_ = collection_name_;
            schema.field_name_ = field_name;
            schema.field_type_ = (int32_t)field_type.second;
            if (index_params.contains("name")) {
                schema.index_name_ = index_params["name"];
            }
            schema.index_param_ = index_params.dump();

            if (!field_params_.at(field_name).empty()) {
                auto field_param = field_params_.at(field_name);
                schema.field_params_ = field_param;
                if (field_type.second == engine::meta::hybrid::DataType::VECTOR_FLOAT ||
                    field_type.second == engine::meta::hybrid::DataType::VECTOR_BINARY) {
                    vector_param = milvus::json::parse(field_param);
                    if (vector_param.contains(engine::PARAM_COLLECTION_DIMENSION)) {
                        dimension = vector_param[engine::PARAM_COLLECTION_DIMENSION].get<int64_t>();
                    } else {
                        return Status{milvus::SERVER_INVALID_VECTOR_DIMENSION,
                                      "Dimension should be defined in vector field extra_params"};
                    }
                }
            }
            fields_schema.fields_schema_.emplace_back(schema);
        }

        collection_schema.collection_id_ = collection_name_;
        collection_schema.dimension_ = dimension;
        if (extra_params_.contains(engine::PARAM_SEGMENT_SIZE)) {
            auto segment_size = extra_params_[engine::PARAM_SEGMENT_SIZE].get<int64_t>();
            collection_schema.index_file_size_ = segment_size;
            STATUS_CHECK(ValidateCollectionIndexFileSize(segment_size));
        }

        if (vector_param.contains(engine::PARAM_INDEX_METRIC_TYPE)) {
            auto metric_type = engine::s_map_metric_type.at(vector_param[engine::PARAM_INDEX_METRIC_TYPE]);
            collection_schema.metric_type_ = (int32_t)metric_type;
        }

        // step 3: create snapshot collection
        engine::snapshot::CreateCollectionContext create_collection_context;
        auto ss_collection_schema = std::make_shared<engine::snapshot::Collection>(collection_name_, extra_params_);
        create_collection_context.collection = ss_collection_schema;
        for (const auto& schema : fields_schema.fields_schema_) {
            auto field = std::make_shared<engine::snapshot::Field>(
                schema.field_name_, 0, (engine::FieldType)schema.field_type_, json::parse(schema.field_params_));

            auto field_element = std::make_shared<engine::snapshot::FieldElement>(
                0, 0, schema.index_name_, engine::FieldElementType::FET_INDEX, json::parse(schema.index_param_));
            create_collection_context.fields_schema[field] = {field_element};
        }

        status = DBWrapper::DB()->CreateCollection(create_collection_context);
        fiu_do_on("CreateCollectionReq.OnExecute.invalid_db_execute",
                  status = Status(milvus::SERVER_UNEXPECTED_ERROR, ""));
        if (!status.ok()) {
            // collection could exist
            if (status.code() == DB_ALREADY_EXIST) {
                return Status(SERVER_INVALID_COLLECTION_NAME, status.message());
            }
            return status;
        }

        rc.ElapseFromBegin("done");
    } catch (std::exception& ex) {
        return Status(SERVER_UNEXPECTED_ERROR, ex.what());
    }

    return Status::OK();
}

}  // namespace server
}  // namespace milvus

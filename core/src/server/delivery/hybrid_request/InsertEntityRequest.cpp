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

#include "server/delivery/hybrid_request/InsertEntityRequest.h"
#include "db/Utils.h"
#include "server/DBWrapper.h"
#include "server/ValidationUtil.h"
#include "utils/CommonUtil.h"
#include "utils/Log.h"
#include "utils/TimeRecorder.h"

#include <fiu-local.h>
#include <memory>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>
#ifdef ENABLE_CPU_PROFILING
#include <gperftools/profiler.h>
#endif

namespace milvus {
namespace server {

InsertEntityRequest::InsertEntityRequest(const std::shared_ptr<milvus::server::Context>& context,
                                         const std::string& collection_name, const std::string& partition_tag,
                                         uint64_t& row_num, std::vector<std::string>& field_names,
                                         std::vector<uint8_t>& attr_values,
                                         std::unordered_map<std::string, engine::VectorsData>& vector_datas)
    : BaseRequest(context, BaseRequest::kInsertEntity),
      collection_name_(collection_name),
      partition_tag_(partition_tag),
      row_num_(row_num),
      field_names_(field_names),
      attr_values_(attr_values),
      vector_datas_(vector_datas) {
}

BaseRequestPtr
InsertEntityRequest::Create(const std::shared_ptr<milvus::server::Context>& context, const std::string& collection_name,
                            const std::string& partition_tag, uint64_t& row_num, std::vector<std::string>& field_names,
                            std::vector<uint8_t>& attr_values,
                            std::unordered_map<std::string, engine::VectorsData>& vector_datas) {
    return std::shared_ptr<BaseRequest>(new InsertEntityRequest(context, collection_name, partition_tag, row_num,
                                                                field_names, attr_values, vector_datas));
}

Status
InsertEntityRequest::OnExecute() {
    LOG_SERVER_INFO_ << LogOut("[%s][%ld] ", "insert", 0) << "Execute insert request.";
    try {
        fiu_do_on("InsertEntityRequest.OnExecute.throw_std_exception", throw std::exception());
        std::string hdr = "InsertEntityRequest(table=" + collection_name_ + ", partition_tag=" + partition_tag_ + ")";
        TimeRecorder rc(hdr);

        // step 1: check arguments
        auto status = ValidateCollectionName(collection_name_);
        if (!status.ok()) {
            return status;
        }

        if (vector_datas_.empty()) {
            return Status{SERVER_INVALID_ARGUMENT,
                          "The vector field is empty, Make sure you have entered vector records"};
        }

        auto vector_datas_it = vector_datas_.begin();
        if (vector_datas_it->second.float_data_.empty() && vector_datas_it->second.binary_data_.empty()) {
            return Status(SERVER_INVALID_ROWRECORD_ARRAY,
                          "The entity array is empty. Make sure you have entered vector records.");
        }

        int64_t entity_count = vector_datas_it->second.vector_count_;
        fiu_do_on("InsertEntityRequest.OnExecute.id_array_error",
                  vector_datas_it->second.id_array_.resize(entity_count + 1));
        if (!vector_datas_it->second.id_array_.empty()) {
            if (vector_datas_it->second.id_array_.size() != (size_t)entity_count) {
                std::string msg = "The size of entity ID array must be equal to the size of the entity.";
                LOG_SERVER_ERROR_ << LogOut("[%s][%ld] Invalid id array: %s", "insert", 0, msg.c_str());
                return Status(SERVER_ILLEGAL_VECTOR_ID, msg);
            }
        }

        // step 2: check table existence
        // only process root table, ignore partition table
        engine::meta::CollectionSchema collection_schema;
        engine::meta::hybrid::FieldsSchema fields_schema;
        collection_schema.collection_id_ = collection_name_;
        status = DBWrapper::DB()->DescribeHybridCollection(collection_schema, fields_schema);
        fiu_do_on("InsertEntityRequest.OnExecute.db_not_found", status = Status(milvus::DB_NOT_FOUND, ""));
        fiu_do_on("InsertEntityRequest.OnExecute.describe_collection_fail",
                  status = Status(milvus::SERVER_UNEXPECTED_ERROR, ""));
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

        for (const auto& schema : fields_schema.fields_schema_) {
            if (schema.field_type_ == (int32_t)engine::meta::hybrid::DataType::VECTOR_FLOAT &&
                vector_datas_it->second.float_data_.empty()) {
                return Status{
                    SERVER_INVALID_ROWRECORD_ARRAY,
                    "The vector field is defined as float vector. Make sure you have entered float vector records"};
            }
            if (schema.field_type_ == (int32_t)engine::meta::hybrid::DataType::VECTOR_BINARY &&
                vector_datas_it->second.binary_data_.empty()) {
                return Status{
                    SERVER_INVALID_ROWRECORD_ARRAY,
                    "The vector field is defined as binary vector. Make sure you have entered binary vector records"};
            }
        }

        std::unordered_map<std::string, engine::meta::hybrid::DataType> field_types;
        auto size = fields_schema.fields_schema_.size();
        if (size - 1 != field_names_.size()) {
            return Status{SERVER_INVALID_FIELD_NAME, "Field numbers is wrong"};
        }
        for (const auto& field_name : field_names_) {
            bool find_field_name = false;
            for (uint64_t i = 0; i < size; ++i) {
                if (fields_schema.fields_schema_[i].field_name_ == field_name) {
                    field_types.insert(std::make_pair(
                        field_name, (engine::meta::hybrid::DataType)fields_schema.fields_schema_[i].field_type_));
                    find_field_name = true;
                    break;
                }
            }
            if (not find_field_name) {
                return Status{SERVER_INVALID_FIELD_NAME, "Field " + field_name + " not exist"};
            }
        }

        // step 3: check table flag
        // all user provide id, or all internal id
        bool user_provide_ids = !vector_datas_it->second.id_array_.empty();
        fiu_do_on("InsertEntityRequest.OnExecute.illegal_entity_id", user_provide_ids = false;
                  collection_schema.flag_ = engine::meta::FLAG_MASK_HAS_USERID);
        // user already provided id before, all insert action require user id
        if ((collection_schema.flag_ & engine::meta::FLAG_MASK_HAS_USERID) != 0 && !user_provide_ids) {
            return Status(SERVER_ILLEGAL_VECTOR_ID,
                          "Collection entity IDs are user-defined. Please provide IDs for all entities of this table.");
        }

        fiu_do_on("InsertEntityRequest.OnExecute.illegal_entity_id2", user_provide_ids = true;
                  collection_schema.flag_ = engine::meta::FLAG_MASK_NO_USERID);
        // user didn't provided id before, no need to provide user id
        if ((collection_schema.flag_ & engine::meta::FLAG_MASK_NO_USERID) != 0 && user_provide_ids) {
            return Status(
                SERVER_ILLEGAL_VECTOR_ID,
                "Table entity IDs are auto-generated. All entities of this table must use auto-generated IDs.");
        }

        rc.RecordSection("check validation");

#ifdef ENABLE_CPU_PROFILING
        std::string fname = "/tmp/insert_" + CommonUtil::GetCurrentTimeStr() + ".profiling";
        ProfilerStart(fname.c_str());
#endif
        // step 4: some metric type doesn't support float vectors

        status = ValidateVectorData(vector_datas_it->second, collection_schema);
        if (!status.ok()) {
            LOG_SERVER_ERROR_ << LogOut("[%s][%d] Invalid vector data: %s", "insert", 0, status.message().c_str());
            return status;
        }

        // TODO(yukun): check dimension and metric_type

        // step 5: insert entities
        // auto vec_count = static_cast<uint64_t>(vector_datas_it->second.vector_count_);

        engine::Entity entity;
        entity.entity_count_ = row_num_;

        entity.attr_value_ = attr_values_;
        entity.vector_data_.insert(std::make_pair(vector_datas_it->first, vector_datas_it->second));
        entity.id_array_ = std::move(vector_datas_it->second.id_array_);

        rc.RecordSection("prepare vectors data");
        status = DBWrapper::DB()->InsertEntities(collection_name_, partition_tag_, field_names_, entity, field_types);
        fiu_do_on("InsertEntityRequest.OnExecute.insert_fail", status = Status(milvus::SERVER_UNEXPECTED_ERROR, ""));
        if (!status.ok()) {
            return status;
        }
        vector_datas_it->second.id_array_ = entity.id_array_;

        auto ids_size = entity.id_array_.size();
        fiu_do_on("InsertEntityRequest.OnExecute.invalid_ids_size", ids_size = entity_count - 1);
        if (ids_size != entity_count) {
            std::string msg =
                "Add " + std::to_string(entity_count) + " entities but only return " + std::to_string(ids_size) + " id";
            return Status(SERVER_ILLEGAL_VECTOR_ID, msg);
        }

        // step 6: update table flag
        user_provide_ids ? collection_schema.flag_ |= engine::meta::FLAG_MASK_HAS_USERID
                         : collection_schema.flag_ |= engine::meta::FLAG_MASK_NO_USERID;
        status = DBWrapper::DB()->UpdateCollectionFlag(collection_name_, collection_schema.flag_);

#ifdef ENABLE_CPU_PROFILING
        ProfilerStop();
#endif

        rc.RecordSection("add vectors to engine");
        rc.ElapseFromBegin("total cost");
    } catch (std::exception& ex) {
        return Status(SERVER_UNEXPECTED_ERROR, ex.what());
    }

    return Status::OK();
}

}  // namespace server
}  // namespace milvus

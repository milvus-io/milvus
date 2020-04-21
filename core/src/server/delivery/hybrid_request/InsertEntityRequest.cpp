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
#include "utils/CommonUtil.h"
#include "utils/Log.h"
#include "utils/TimeRecorder.h"
#include "utils/ValidationUtil.h"

#include <fiu-local.h>
#include <memory>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>
#ifdef MILVUS_ENABLE_PROFILING
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
    try {
        fiu_do_on("InsertEntityRequest.OnExecute.throw_std_exception", throw std::exception());
        std::string hdr = "InsertEntityRequest(table=" + collection_name_ + ", partition_tag=" + partition_tag_ + ")";
        TimeRecorder rc(hdr);

        // step 1: check arguments
        auto status = ValidationUtil::ValidateCollectionName(collection_name_);
        if (!status.ok()) {
            return status;
        }

        auto vector_datas_it = vector_datas_.begin();
        if (vector_datas_it->second.float_data_.empty() && vector_datas_it->second.binary_data_.empty()) {
            return Status(SERVER_INVALID_ROWRECORD_ARRAY,
                          "The vector array is emp ty. Make sure you have entered vector records.");
        }

        // step 2: check table existence
        // only process root table, ignore partition table
        engine::meta::CollectionSchema collection_schema;
        engine::meta::hybrid::FieldsSchema fields_schema;
        collection_schema.collection_id_ = collection_name_;
        status = DBWrapper::DB()->DescribeHybridCollection(collection_schema, fields_schema);
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

        std::unordered_map<std::string, engine::meta::hybrid::DataType> field_types;
        auto size = fields_schema.fields_schema_.size();
        for (auto field_name : field_names_) {
            for (uint64_t i = 0; i < size; ++i) {
                if (fields_schema.fields_schema_[i].field_name_ == field_name) {
                    field_types.insert(std::make_pair(
                        field_name, (engine::meta::hybrid::DataType)fields_schema.fields_schema_[i].field_type_));
                }
            }
        }

        // step 3: check table flag
        // all user provide id, or all internal id
        bool user_provide_ids = !vector_datas_it->second.id_array_.empty();
        fiu_do_on("InsertEntityRequest.OnExecute.illegal_vector_id", user_provide_ids = false;
                  collection_schema.flag_ = engine::meta::FLAG_MASK_HAS_USERID);
        // user already provided id before, all insert action require user id
        if ((collection_schema.flag_ & engine::meta::FLAG_MASK_HAS_USERID) != 0 && !user_provide_ids) {
            return Status(SERVER_ILLEGAL_VECTOR_ID,
                          "Collection vector IDs are user-defined. Please provide IDs for all vectors of this table.");
        }

        fiu_do_on("InsertRequest.OnExecute.illegal_vector_id2", user_provide_ids = true;
                  collection_schema.flag_ = engine::meta::FLAG_MASK_NO_USERID);
        // user didn't provided id before, no need to provide user id
        if ((collection_schema.flag_ & engine::meta::FLAG_MASK_NO_USERID) != 0 && user_provide_ids) {
            return Status(
                SERVER_ILLEGAL_VECTOR_ID,
                "Table vector IDs are auto-generated. All vectors of this table must use auto-generated IDs.");
        }

        rc.RecordSection("check validation");

#ifdef MILVUS_ENABLE_PROFILING
        std::string fname = "/tmp/insert_" + CommonUtil::GetCurrentTimeStr() + ".profiling";
        ProfilerStart(fname.c_str());
#endif
        // step 4: some metric type doesn't support float vectors

        // TODO(yukun): check dimension and metric_type

        // step 5: insert entities
        auto vec_count = static_cast<uint64_t>(vector_datas_it->second.vector_count_);

        engine::Entity entity;
        entity.entity_count_ = row_num_;

        entity.attr_value_ = attr_values_;
        entity.vector_data_.insert(std::make_pair(vector_datas_it->first, vector_datas_it->second));

        rc.RecordSection("prepare vectors data");
        status = DBWrapper::DB()->InsertEntities(collection_name_, partition_tag_, field_names_, entity, field_types);
        fiu_do_on("InsertRequest.OnExecute.insert_fail", status = Status(milvus::SERVER_UNEXPECTED_ERROR, ""));
        if (!status.ok()) {
            return status;
        }
        vector_datas_it->second.id_array_ = entity.id_array_;

        //        auto ids_size = vectors_data_.id_array_.size();
        //        fiu_do_on("InsertRequest.OnExecute.invalid_ids_size", ids_size = vec_count - 1);
        //        if (ids_size != vec_count) {
        //            std::string msg =
        //                "Add " + std::to_string(vec_count) + " vectors but only return " + std::to_string(ids_size) +
        //                " id";
        //            return Status(SERVER_ILLEGAL_VECTOR_ID, msg);
        //        }

        // step 6: update table flag
        user_provide_ids ? collection_schema.flag_ |= engine::meta::FLAG_MASK_HAS_USERID
                         : collection_schema.flag_ |= engine::meta::FLAG_MASK_NO_USERID;
        status = DBWrapper::DB()->UpdateCollectionFlag(collection_name_, collection_schema.flag_);

#ifdef MILVUS_ENABLE_PROFILING
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

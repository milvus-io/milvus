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

#include "server/delivery/request/DescribeIndexRequest.h"
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

DescribeIndexRequest::DescribeIndexRequest(const std::shared_ptr<milvus::server::Context>& context,
                                           const std::string& collection_name, IndexParam& index_param)
    : BaseRequest(context, BaseRequest::kDescribeIndex), collection_name_(collection_name), index_param_(index_param) {
}

BaseRequestPtr
DescribeIndexRequest::Create(const std::shared_ptr<milvus::server::Context>& context,
                             const std::string& collection_name, IndexParam& index_param) {
    return std::shared_ptr<BaseRequest>(new DescribeIndexRequest(context, collection_name, index_param));
}

Status
DescribeIndexRequest::OnExecute() {
    try {
        fiu_do_on("DescribeIndexRequest.OnExecute.throw_std_exception", throw std::exception());
        std::string hdr = "DescribeIndexRequest(collection=" + collection_name_ + ")";
        TimeRecorderAuto rc(hdr);

        // step 1: check arguments
        auto status = ValidateCollectionName(collection_name_);
        if (!status.ok()) {
            return status;
        }

        // only process root collection, ignore partition collection
        engine::snapshot::CollectionPtr collection;
        engine::snapshot::CollectionMappings fields_schema;
        status = DBWrapper::DB()->DescribeCollection(collection_name_, collection, fields_schema);
        fiu_do_on("DropIndexRequest.OnExecute.collection_not_exist",
                  status = Status(milvus::SERVER_UNEXPECTED_ERROR, ""));
        if (!status.ok()) {
            if (status.code() == DB_NOT_FOUND) {
                return Status(SERVER_COLLECTION_NOT_EXIST, CollectionNotExistMsg(collection_name_));
            } else {
                return status;
            }
        }

        //        // pick up field
        //        engine::snapshot::FieldPtr field;
        //        for (auto field_it = fields_schema.begin(); field_it != fields_schema.end(); field_it++) {
        //            if (field_it->first->GetName() == field_name_) {
        //                field = field_it->first;
        //                break;
        //            }
        //        }
        //        if (field == nullptr) {
        //            return Status(SERVER_INVALID_FIELD_NAME, "Invalid field name");
        //        }
        //
        //        // TODO(yukun): Currently field name is vector field name
        //        std::string field_name;
        //        for (auto field_it = fields_schema.begin(); field_it != fields_schema.end(); field_it++) {
        //            auto type = field_it->first->GetFtype();
        //            if (type == (int64_t)engine::meta::hybrid::DataType::VECTOR_FLOAT ||
        //                type == (int64_t)engine::meta::hybrid::DataType::VECTOR_BINARY) {
        //                field_name = field_it->first->GetName();
        //                break;
        //            }
        //        }
        //
        //        // step 2: check collection existence
        //        engine::CollectionIndex index;
        //        status = DBWrapper::DB()->DescribeIndex(collection_name_, field_name, index);
        //        if (!status.ok()) {
        //            return status;
        //        }
        //
        //        // for binary vector, IDMAP and IVFLAT will be treated as BIN_IDMAP and BIN_IVFLAT internally
        //        // return IDMAP and IVFLAT for outside caller
        //        if (index.engine_type_ == (int32_t)engine::EngineType::FAISS_BIN_IDMAP) {
        //            index.engine_type_ = (int32_t)engine::EngineType::FAISS_IDMAP;
        //        } else if (index.engine_type_ == (int32_t)engine::EngineType::FAISS_BIN_IVFFLAT) {
        //            index.engine_type_ = (int32_t)engine::EngineType::FAISS_IVFFLAT;
        //        }
        //
        //        index_param_.collection_name_ = collection_name_;
        //        index_param_.index_type_ = index.engine_type_;
        //        index_param_.extra_params_ = index.extra_params_.dump();
    } catch (std::exception& ex) {
        return Status(SERVER_UNEXPECTED_ERROR, ex.what());
    }

    return Status::OK();
}

}  // namespace server
}  // namespace milvus

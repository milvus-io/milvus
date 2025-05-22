// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#pragma once

#include <memory>
#include <unordered_map>

#include "pb/common.pb.h"
#include "pb/segcore.pb.h"
#include "knowhere/utils.h"
#include "Types.h"

namespace milvus {
class FieldIndexMeta {
 public:
    //For unittest init
    FieldIndexMeta(FieldId fieldId,
                   std::map<std::string, std::string>&& index_params,
                   std::map<std::string, std::string>&& type_params);

    FieldIndexMeta(
        const milvus::proto::segcore::FieldIndexMeta& fieldIndexMeta);

    knowhere::MetricType
    GeMetricType() const {
        return index_params_.at(knowhere::meta::METRIC_TYPE);
    }

    knowhere::IndexType
    GetIndexType() const {
        return index_params_.at(knowhere::meta::INDEX_TYPE);
    }

    bool
    IsFlatIndex() const {
        return knowhere::IsFlatIndex(GetIndexType());
    }

    const std::map<std::string, std::string>&
    GetIndexParams() const {
        return index_params_;
    }

    const std::map<std::string, std::string>&
    GetTypeParams() const {
        return type_params_;
    }

 private:
    FieldId fieldId_;
    std::map<std::string, std::string> index_params_;
    std::map<std::string, std::string> type_params_;
    std::map<std::string, std::string> user_index_params_;
};

class CollectionIndexMeta {
 public:
    //just for unittest
    CollectionIndexMeta(int64_t max_index_row_cnt,
                        std::map<FieldId, FieldIndexMeta>&& fieldMetas);

    CollectionIndexMeta(
        const milvus::proto::segcore::CollectionIndexMeta& collectionIndexMeta);

    int64_t
    GetIndexMaxRowCount() const;

    bool
    HasFiled(FieldId fieldId) const;

    const FieldIndexMeta&
    GetFieldIndexMeta(FieldId fieldId) const;

    std::string
    ToString();

 private:
    int64_t max_index_row_cnt_;
    std::map<FieldId, FieldIndexMeta> fieldMetas_;
};

using IndexMetaPtr = std::shared_ptr<CollectionIndexMeta>;

const static IndexMetaPtr empty_index_meta =
    std::make_shared<CollectionIndexMeta>(1024,
                                          std::map<FieldId, FieldIndexMeta>());

}  //namespace milvus
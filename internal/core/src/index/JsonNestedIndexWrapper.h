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
#include <string>
#include <utility>
#include <vector>

#include "common/FieldData.h"
#include "common/FieldDataInterface.h"
#include "common/JsonCastType.h"
#include "common/Types.h"
#include "index/JsonIndexBuilder.h"
#include "pb/schema.pb.h"
#include "storage/MemFileManagerImpl.h"
#include "storage/Types.h"
#include "storage/Util.h"

namespace milvus::index {

// Wraps a nested (element-level) ARRAY scalar index (BaseIndex, one of the
// InvertedIndexTantivy<T> / BitmapIndex<T> / ScalarIndexSort<T> / StringIndexSort
// nested builders) so it can be built over a JSON array PATH instead of a real
// ARRAY field.
//
// Approach A (materialize ARRAY FieldData): the only override is Build(config).
// It fetches the raw JSON binlog through its own MemFileManager (built from the
// original JSON-typed context, since the BaseIndex's file_manager_ was built
// from an ARRAY-typed context that cannot read JSON binlogs), materializes an
// ARRAY-shaped FieldData<Array> from it via ConvertJsonToArrayFieldData<T>, and
// feeds it to BaseIndex::BuildWithFieldData. Because BaseIndex was constructed
// with is_nested_index=true and an ARRAY-typed schema (data_type=Array,
// element_type=cast element type), BuildWithFieldData routes straight to the
// existing element-level nested builders, unchanged.
//
// Everything else (Serialize/Upload/Load, is_nested marker persistence, the
// element-level query path) is inherited from BaseIndex unchanged.
//
// T is the array element scalar type: bool (ARRAY_BOOL), double (ARRAY_DOUBLE),
// or std::string (ARRAY_VARCHAR).
template <typename T, typename BaseIndex>
class JsonNestedIndexWrapper : public BaseIndex {
 public:
    // args... are forwarded verbatim to BaseIndex — the factory supplies the
    // ARRAY-typed context and nested-index flags in the exact order each base
    // index constructor expects (see MakeJsonNestedWrapped in IndexFactory).
    template <typename... Args>
    JsonNestedIndexWrapper(const JsonCastType& cast_type,
                           const std::string& nested_path,
                           const proto::schema::FieldSchema& json_schema,
                           const storage::FileManagerContext& original_ctx,
                           Args&&... args)
        : BaseIndex(std::forward<Args>(args)...),
          cast_type_(cast_type),
          nested_path_(nested_path),
          json_schema_(json_schema) {
        json_file_manager_ =
            std::make_shared<storage::MemFileManagerImpl>(original_ctx);
    }

    void
    Build(const Config& config) override {
        auto json_field_datas =
            storage::CacheRawDataAndFillMissing(json_file_manager_, config);
        BuildWithJsonFieldData(json_field_datas);
    }

    void
    BuildWithFieldData(const std::vector<FieldDataPtr>& field_datas) override {
        BuildWithJsonFieldData(field_datas);
    }

    JsonCastType
    GetCastType() const override {
        return cast_type_;
    }

 private:
    void
    BuildWithJsonFieldData(const std::vector<FieldDataPtr>& json_field_datas) {
        auto array_field_data = ConvertJsonToArrayFieldData<T>(
            json_field_datas, json_schema_, nested_path_, cast_type_);
        BaseIndex::BuildWithFieldData({array_field_data});
    }

    JsonCastType cast_type_;
    std::string nested_path_;
    proto::schema::FieldSchema json_schema_;
    storage::MemFileManagerImplPtr json_file_manager_;
};

}  // namespace milvus::index

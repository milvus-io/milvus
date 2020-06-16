// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#pragma once

#include <memory>
#include <string>
#include <utility>
#include "db/meta/MetaTypes.h"
#include "knowhere/index/Index.h"

namespace milvus {
namespace segment {

class AttrIndex {
 public:
    explicit AttrIndex(knowhere::IndexPtr index_ptr, engine::meta::hybrid::DataType& data_type,
                       const std::string& field_name)
        : index_ptr_(std::move(index_ptr)), data_type_(data_type), field_name_(std::move(field_name)) {
    }

    AttrIndex() = default;

    knowhere::IndexPtr
    GetAttrIndex() const {
        return index_ptr_;
    }

    engine::meta::hybrid::DataType
    GetDataType() const {
        return data_type_;
    }

    const std::string&
    GetFieldName() const {
        return field_name_;
    }

    void
    SetAttrIndex(const knowhere::IndexPtr& index_ptr) {
        index_ptr_ = index_ptr;
    }

    void
    SetDataType(const engine::meta::hybrid::DataType data_type) {
        data_type_ = data_type;
    }

    void
    SetFieldName(const std::string& field_name) {
        field_name_ = field_name;
    }

    // No copy and move
    AttrIndex(const AttrIndex&) = delete;
    AttrIndex(AttrIndex&&) = delete;

    AttrIndex&
    operator=(const AttrIndex&) = delete;
    AttrIndex&
    operator=(AttrIndex&&) = delete;

 private:
    knowhere::IndexPtr index_ptr_ = nullptr;
    engine::meta::hybrid::DataType data_type_;
    std::string field_name_;
};

using AttrIndexPtr = std::shared_ptr<AttrIndex>;

}  // namespace segment
}  // namespace milvus

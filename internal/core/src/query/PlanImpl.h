// Copyright (C) 2019-2020 Zilliz. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software distributed under the License
// is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
// or implied. See the License for the specific language governing permissions and limitations under the License

#pragma once

#include "Plan.h"
#include "utils/Json.h"
#include "PlanNode.h"
#include "exceptions/EasyAssert.h"
#include "pb/milvus.pb.h"
#include <memory>
#include <map>
#include <string>
#include <vector>
#include <optional>
#include <boost/dynamic_bitset.hpp>

namespace milvus::query {
using Json = nlohmann::json;

// class definitions

struct ExtractedPlanInfo {
 public:
    explicit ExtractedPlanInfo(int64_t size) : involved_fields_(size) {
    }

    void
    add_involved_field(FieldOffset field_offset) {
        involved_fields_.set(field_offset.get());
    }

 public:
    boost::dynamic_bitset<> involved_fields_;
};

struct Plan {
 public:
    explicit Plan(const Schema& schema) : schema_(schema) {
    }

 public:
    const Schema& schema_;
    std::unique_ptr<VectorPlanNode> plan_node_;
    std::map<std::string, FieldOffset> tag2field_;  // PlaceholderName -> FieldOffset
    std::vector<FieldOffset> target_entries_;
    void
    check_identical(Plan& other);

 public:
    std::optional<ExtractedPlanInfo> extra_info_opt_;
    // TODO: add move extra info
};

struct Placeholder {
    // milvus::proto::service::PlaceholderGroup group_;
    std::string tag_;
    int64_t num_of_queries_;
    int64_t line_sizeof_;
    aligned_vector<char> blob_;

    template <typename T>
    const T*
    get_blob() const {
        return reinterpret_cast<const T*>(blob_.data());
    }

    template <typename T>
    T*
    get_blob() {
        return reinterpret_cast<T*>(blob_.data());
    }
};

struct RetrievePlan {
 public:
    explicit RetrievePlan(const Schema& schema) : schema_(schema) {
    }

 public:
    const Schema& schema_;
    std::unique_ptr<RetrievePlanNode> plan_node_;
    std::vector<FieldOffset> field_offsets_;
};

using PlanPtr = std::unique_ptr<Plan>;

struct PlaceholderGroup : std::vector<Placeholder> {
    using std::vector<Placeholder>::vector;
};
}  // namespace milvus::query

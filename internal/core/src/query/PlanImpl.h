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
#include "utils/EasyAssert.h"
#include "pb/service_msg.pb.h"
#include <memory>
#include <map>
#include <string>
#include <vector>
#include <boost/align/aligned_allocator.hpp>

namespace milvus::query {
using Json = nlohmann::json;

// class definitions
struct Plan {
 public:
    explicit Plan(const Schema& schema) : schema_(schema) {
    }

 public:
    const Schema& schema_;
    std::unique_ptr<VectorPlanNode> plan_node_;
    std::map<std::string, FieldId> tag2field_;  // PlaceholderName -> FieldId
    // TODO: add move extra info
};

template <typename T>
using aligned_vector = std::vector<T, boost::alignment::aligned_allocator<T, 512>>;

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

struct PlaceholderGroup : std::vector<Placeholder> {
    using std::vector<Placeholder>::vector;
};
}  // namespace milvus::query

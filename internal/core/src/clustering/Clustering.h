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
#include <vector>
#include <unordered_map>
#include <string>
#include "common/Types.h"

namespace milvus::clustering {

// after clustering result uploaded, return result meta for golang usage
struct ClusteringResultMeta {
    std::string centroid_path;   // centroid result path
    int64_t centroid_file_size;  // centroid result size
    std::unordered_map<std::string, int64_t>
        id_mappings;  // id mapping result path/size for each segment
};

class Clustering {
 public:
    virtual ~Clustering() = default;

    virtual void
    Run(const Config& config) = 0;

    virtual ClusteringResultMeta
    GetClusteringResultMeta() = 0;
};

using ClusteringBasePtr = std::unique_ptr<Clustering>;
}  // namespace milvus::clustering

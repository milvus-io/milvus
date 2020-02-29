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

#include "index/knowhere/knowhere/index/Index.h"

namespace milvus {
namespace segment {

class VectorIndex {
 public:
    explicit VectorIndex(knowhere::IndexPtr index_ptr);

    void
    Get(knowhere::IndexPtr& index_ptr);

    // No copy and move
    VectorIndex(const VectorIndex&) = delete;
    VectorIndex(VectorIndex&&) = delete;

    VectorIndex&
    operator=(const VectorIndex&) = delete;
    VectorIndex&
    operator=(VectorIndex&&) = delete;

 private:
    knowhere::IndexPtr index_ptr_;
};

using VectorIndexPtr = std::shared_ptr<VectorIndex>;

}  // namespace segment
}  // namespace milvus

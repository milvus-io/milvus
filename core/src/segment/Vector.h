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

namespace milvus {
namespace segment {

class Vector {
 public:
    Vector(void* data, size_t nbytes, int64_t* uid);

    // No copy and move
    Vector(const Vector&) = delete;
    Vector(Vector&&) = delete;

    Vector&
    operator=(const Vector&) = delete;
    Vector&
    operator=(Vector&&) = delete;

 private:
    void* data_;
    size_t nbytes_;
    int64_t* uid_;
};

using VectorPtr = std::shared_ptr<Vector>;

}  // namespace segment
}  // namespace milvus

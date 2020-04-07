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

#include <mutex>
#include <string>

#include "codecs/VectorIndexFormat.h"

namespace milvus {
namespace codec {

class DefaultVectorIndexFormat : public VectorIndexFormat {
 public:
    DefaultVectorIndexFormat() = default;

    void
    read(const storage::FSHandlerPtr& fs_ptr, const std::string& location,
         segment::VectorIndexPtr& vector_index) override;

    void
    write(const storage::FSHandlerPtr& fs_ptr, const std::string& location,
          const segment::VectorIndexPtr& vector_index) override;

    // No copy and move
    DefaultVectorIndexFormat(const DefaultVectorIndexFormat&) = delete;
    DefaultVectorIndexFormat(DefaultVectorIndexFormat&&) = delete;

    DefaultVectorIndexFormat&
    operator=(const DefaultVectorIndexFormat&) = delete;
    DefaultVectorIndexFormat&
    operator=(DefaultVectorIndexFormat&&) = delete;

 private:
    knowhere::VecIndexPtr
    read_internal(const storage::FSHandlerPtr& fs_ptr, const std::string& path);

 private:
    std::mutex mutex_;

    const std::string vector_index_extension_ = "";
};

}  // namespace codec
}  // namespace milvus

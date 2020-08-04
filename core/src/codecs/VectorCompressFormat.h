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

#include "knowhere/common/BinarySet.h"
#include "storage/FSHandler.h"

namespace milvus {
namespace codec {

class VectorCompressFormat {
 public:
    VectorCompressFormat() = default;

    static std::string
    FilePostfix();

    void
    Read(const storage::FSHandlerPtr& fs_ptr, const std::string& file_path, knowhere::BinaryPtr& compress);

    void
    Write(const storage::FSHandlerPtr& fs_ptr, const std::string& file_path, const knowhere::BinaryPtr& compress);

    // No copy and move
    VectorCompressFormat(const VectorCompressFormat&) = delete;
    VectorCompressFormat(VectorCompressFormat&&) = delete;

    VectorCompressFormat&
    operator=(const VectorCompressFormat&) = delete;
    VectorCompressFormat&
    operator=(VectorCompressFormat&&) = delete;
};

using VectorCompressFormatPtr = std::shared_ptr<VectorCompressFormat>;

}  // namespace codec
}  // namespace milvus

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
#include <vector>

#include "codecs/VectorIndexFormat.h"
#include "storage/FSHandler.h"

namespace milvus {
namespace codec {

class SSVectorIndexFormat {
 public:
    SSVectorIndexFormat() = default;

    void
    read_raw(const storage::FSHandlerPtr& fs_ptr, const std::string& location, knowhere::BinaryPtr& data);

    void
    read_index(const storage::FSHandlerPtr& fs_ptr, const std::string& location, knowhere::BinarySet& data);

    void
    read_compress(const storage::FSHandlerPtr& fs_ptr, const std::string& location, knowhere::BinaryPtr& data);

    void
    convert_raw(const std::vector<uint8_t>& raw, knowhere::BinaryPtr& data);

    void
    construct_index(const std::string& index_name, knowhere::BinarySet& index_data, knowhere::BinaryPtr& raw_data,
                    knowhere::BinaryPtr& compress_data, knowhere::VecIndexPtr& index);

    void
    write_index(const storage::FSHandlerPtr& fs_ptr, const std::string& location, const knowhere::VecIndexPtr& index);

    void
    write_compress(const storage::FSHandlerPtr& fs_ptr, const std::string& location,
                   const knowhere::VecIndexPtr& index);

    // No copy and move
    SSVectorIndexFormat(const SSVectorIndexFormat&) = delete;
    SSVectorIndexFormat(SSVectorIndexFormat&&) = delete;

    SSVectorIndexFormat&
    operator=(const SSVectorIndexFormat&) = delete;
    SSVectorIndexFormat&
    operator=(SSVectorIndexFormat&&) = delete;
};

using SSVectorIndexFormatPtr = std::shared_ptr<SSVectorIndexFormat>;

}  // namespace codec
}  // namespace milvus

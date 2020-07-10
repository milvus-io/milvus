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

#include "knowhere/common/BinarySet.h"
#include "segment/Vectors.h"
#include "storage/FSHandler.h"

namespace milvus {
namespace codec {

class SSVectorsFormat {
 public:
    SSVectorsFormat() = default;

    void
    read(const storage::FSHandlerPtr& fs_ptr, segment::VectorsPtr& vectors_read);

    void
    write(const storage::FSHandlerPtr& fs_ptr, const segment::VectorsPtr& vectors);

    void
    read_uids(const storage::FSHandlerPtr& fs_ptr, const std::string& file_path, std::vector<segment::doc_id_t>& uids);

    void
    read_vectors(const storage::FSHandlerPtr& fs_ptr, knowhere::BinaryPtr& raw_vectors);

    void
    read_vectors(const storage::FSHandlerPtr& fs_ptr, const std::string& file_path, off_t offset, size_t num_bytes,
                 std::vector<uint8_t>& raw_vectors);

    // No copy and move
    SSVectorsFormat(const SSVectorsFormat&) = delete;
    SSVectorsFormat(SSVectorsFormat&&) = delete;

    SSVectorsFormat&
    operator=(const SSVectorsFormat&) = delete;
    SSVectorsFormat&
    operator=(SSVectorsFormat&&) = delete;

 private:
    void
    read_vectors_internal(const storage::FSHandlerPtr& fs_ptr, const std::string& file_path, off_t offset, size_t num,
                          std::vector<uint8_t>& raw_vectors);

    void
    read_vectors_internal(const storage::FSHandlerPtr& fs_ptr, const std::string& file_path,
                          knowhere::BinaryPtr& raw_vectors);

    void
    read_uids_internal(const storage::FSHandlerPtr& fs_ptr, const std::string& file_path,
                       std::vector<segment::doc_id_t>& uids);

 private:
    const std::string raw_vector_extension_ = ".rv";
    const std::string user_id_extension_ = ".uid";
};

using SSVectorsFormatPtr = std::shared_ptr<SSVectorsFormat>;

}  // namespace codec
}  // namespace milvus

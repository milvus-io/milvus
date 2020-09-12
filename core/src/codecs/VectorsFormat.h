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
#include <vector>

#include "segment/Vectors.h"
#include "storage/FSHandler.h"

namespace milvus {
namespace codec {

class VectorsFormat {
 public:
    virtual void
    read(const storage::FSHandlerPtr& fs_ptr, segment::VectorsPtr& vectors_read) = 0;

    virtual void
    write(const storage::FSHandlerPtr& fs_ptr, const segment::VectorsPtr& vectors) = 0;

    virtual void
    read_uids(const storage::FSHandlerPtr& fs_ptr, std::vector<segment::doc_id_t>& uids) = 0;

    virtual void
    read_vectors(const storage::FSHandlerPtr& fs_ptr, off_t offset, size_t num_bytes,
                 std::vector<uint8_t>& raw_vectors) = 0;
};

using VectorsFormatPtr = std::shared_ptr<VectorsFormat>;

}  // namespace codec
}  // namespace milvus

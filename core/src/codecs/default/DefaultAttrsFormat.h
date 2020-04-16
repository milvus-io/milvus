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
#include <vector>

#include "codecs/AttrsFormat.h"
#include "segment/Attrs.h"

namespace milvus {
namespace codec {

class DefaultAttrsFormat : public AttrsFormat {
 public:
    DefaultAttrsFormat() = default;

    void
    read(const storage::FSHandlerPtr& fs_ptr, segment::AttrsPtr& attrs_read) override;

    void
    write(const storage::FSHandlerPtr& fs_ptr, const segment::AttrsPtr& attr) override;

    void
    read_uids(const storage::FSHandlerPtr& fs_ptr, std::vector<int64_t>& uids) override;

    // No copy and move
    DefaultAttrsFormat(const DefaultAttrsFormat&) = delete;
    DefaultAttrsFormat(DefaultAttrsFormat&&) = delete;

    DefaultAttrsFormat&
    operator=(const DefaultAttrsFormat&) = delete;
    DefaultAttrsFormat&
    operator=(DefaultAttrsFormat&&) = delete;

 private:
    void
    read_attrs_internal(const std::string&, off_t, size_t, std::vector<uint8_t>&, size_t&);

    void
    read_uids_internal(const std::string&, std::vector<int64_t>&);

 private:
    std::mutex mutex_;

    const std::string raw_attr_extension_ = ".ra";
    const std::string user_id_extension_ = ".uid";
};

}  // namespace codec
}  // namespace milvus

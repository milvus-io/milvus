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

#include "segment/Attrs.h"
#include "storage/FSHandler.h"

namespace milvus {
namespace codec {

class AttrsFormat {
 public:
    virtual void
    read(const storage::FSHandlerPtr& fs_ptr, segment::AttrsPtr& attrs_read) = 0;

    virtual void
    write(const storage::FSHandlerPtr& fs_ptr, const segment::AttrsPtr& attr) = 0;

    virtual void
    read_uids(const storage::FSHandlerPtr& fs_ptr, std::vector<int64_t>& uids) = 0;
};

using AttrsFormatPtr = std::shared_ptr<AttrsFormat>;

}  // namespace codec
}  // namespace milvus

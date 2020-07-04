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

#include <src/db/meta/MetaTypes.h>
#include <string>
#include <vector>

#include "codecs/AttrsIndexFormat.h"
#include "segment/AttrsIndex.h"

namespace milvus {
namespace codec {

class DefaultAttrsIndexFormat : public AttrsIndexFormat {
 public:
    DefaultAttrsIndexFormat() = default;

    void
    read(const storage::FSHandlerPtr& fs_ptr, segment::AttrsIndexPtr& attr_index) override;

    void
    write(const storage::FSHandlerPtr& fs_ptr, const segment::AttrsIndexPtr& attr_index) override;

    // No copy and move
    DefaultAttrsIndexFormat(const DefaultAttrsIndexFormat&) = delete;
    DefaultAttrsIndexFormat(DefaultAttrsIndexFormat&&) = delete;

    DefaultAttrsIndexFormat&
    operator=(const DefaultAttrsIndexFormat&) = delete;
    DefaultAttrsIndexFormat&
    operator=(DefaultAttrsIndexFormat&&) = delete;

 private:
    void
    read_internal(const milvus::storage::FSHandlerPtr& fs_ptr, const std::string& path, knowhere::IndexPtr& index,
                  engine::meta::hybrid::DataType& attr_type);

    knowhere::IndexPtr
    create_structured_index(const engine::meta::hybrid::DataType data_type);

 private:
    const std::string attr_index_extension_ = ".idx";
};

}  // namespace codec
}  // namespace milvus

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

#include "db/meta/MetaTypes.h"
#include "knowhere/index/Index.h"
#include "storage/FSHandler.h"

namespace milvus {
namespace codec {

class SSStructuredIndexFormat {
 public:
    SSStructuredIndexFormat() = default;

    std::string
    FilePostfix();

    void
    Read(const storage::FSHandlerPtr& fs_ptr, const std::string& file_path, knowhere::IndexPtr& index);

    void
    Write(const storage::FSHandlerPtr& fs_ptr, const std::string& file_path, engine::meta::hybrid::DataType data_type,
          const knowhere::IndexPtr& index);

    // No copy and move
    SSStructuredIndexFormat(const SSStructuredIndexFormat&) = delete;
    SSStructuredIndexFormat(SSStructuredIndexFormat&&) = delete;

    SSStructuredIndexFormat&
    operator=(const SSStructuredIndexFormat&) = delete;
    SSStructuredIndexFormat&
    operator=(SSStructuredIndexFormat&&) = delete;

 private:
    knowhere::IndexPtr
    CreateStructuredIndex(const engine::meta::hybrid::DataType data_type);
};

using SSStructuredIndexFormatPtr = std::shared_ptr<SSStructuredIndexFormat>;

}  // namespace codec
}  // namespace milvus

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

#include "VectorSource.h"
#include "db/engine/ExecutionEngine.h"
#include "db/meta/Meta.h"
#include "utils/Status.h"

#include <memory>
#include <string>

namespace milvus {
namespace engine {

class MemTableFile {
 public:
    MemTableFile(const std::string& table_id, const meta::MetaPtr& meta, const DBOptions& options);

    Status
    Add(VectorSourcePtr& source);

    size_t
    GetCurrentMem();

    size_t
    GetMemLeft();

    bool
    IsFull();

    Status
    Serialize();

 private:
    Status
    CreateTableFile();

 private:
    const std::string table_id_;
    meta::TableFileSchema table_file_schema_;
    meta::MetaPtr meta_;
    DBOptions options_;
    size_t current_mem_;

    ExecutionEnginePtr execution_engine_;
};  // MemTableFile

using MemTableFilePtr = std::shared_ptr<MemTableFile>;

}  // namespace engine
}  // namespace milvus

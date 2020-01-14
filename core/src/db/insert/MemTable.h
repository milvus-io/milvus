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

#include "MemTableFile.h"
#include "VectorSource.h"
#include "utils/Status.h"

#include <memory>
#include <mutex>
#include <string>
#include <vector>

namespace milvus {
namespace engine {

class MemTable {
 public:
    using MemTableFileList = std::vector<MemTableFilePtr>;

    MemTable(const std::string& table_id, const meta::MetaPtr& meta, const DBOptions& options);

    Status
    Add(VectorSourcePtr& source);

    void
    GetCurrentMemTableFile(MemTableFilePtr& mem_table_file);

    size_t
    GetTableFileCount();

    Status
    Serialize();

    bool
    Empty();

    const std::string&
    GetTableId() const;

    size_t
    GetCurrentMem();

 private:
    const std::string table_id_;

    MemTableFileList mem_table_file_list_;

    meta::MetaPtr meta_;

    DBOptions options_;

    std::mutex mutex_;
};  // MemTable

using MemTablePtr = std::shared_ptr<MemTable>;

}  // namespace engine
}  // namespace milvus

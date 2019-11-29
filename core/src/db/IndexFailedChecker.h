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

#include "db/Types.h"
#include "meta/Meta.h"
#include "utils/Status.h"

#include <map>
#include <mutex>
#include <string>
#include <vector>

namespace milvus {
namespace engine {

class IndexFailedChecker {
 public:
    Status
    CleanFailedIndexFileOfTable(const std::string& table_id);

    Status
    GetFailedIndexFileOfTable(const std::string& table_id, std::vector<std::string>& failed_files);

    Status
    MarkFailedIndexFile(const meta::TableFileSchema& file);

    Status
    MarkSucceedIndexFile(const meta::TableFileSchema& file);

    Status
    IgnoreFailedIndexFiles(meta::TableFilesSchema& table_files);

 private:
    std::mutex mutex_;
    Table2Files index_failed_files_;  // table id mapping to (file id mapping to failed times)
};

}  // namespace engine
}  // namespace milvus

// Copyright (C) 2019-2020 Zilliz. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software distributed under the License
// is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
// or implied. See the License for the specific language governing permissions and limitations under the License.

#pragma once

#include "db/Types.h"
#include "meta/Meta.h"
#include "utils/Status.h"

#include <map>
#include <mutex>
#include <set>
#include <string>

namespace milvus {
namespace engine {

class OngoingFileChecker : public meta::Meta::CleanUpFilter {
 public:
    Status
    MarkOngoingFile(const meta::TableFileSchema& table_file);

    Status
    MarkOngoingFiles(const meta::TableFilesSchema& table_files);

    Status
    UnmarkOngoingFile(const meta::TableFileSchema& table_file);

    Status
    UnmarkOngoingFiles(const meta::TableFilesSchema& table_files);

    bool
    IsIgnored(const meta::TableFileSchema& schema) override;

 private:
    Status
    MarkOngoingFileNoLock(const meta::TableFileSchema& table_file);

    Status
    UnmarkOngoingFileNoLock(const meta::TableFileSchema& table_file);

 private:
    std::mutex mutex_;
    Table2FileRef ongoing_files_;  // table id mapping to (file id mapping to ongoing ref-count)
};

}  // namespace engine
}  // namespace milvus

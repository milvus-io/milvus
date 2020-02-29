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

#include <segment/SegmentWriter.h>

#include <memory>
#include <string>
#include <vector>

#include "VectorSource.h"
#include "db/engine/ExecutionEngine.h"
#include "db/meta/Meta.h"
#include "utils/Status.h"

namespace milvus {
namespace engine {

class MemTableFile {
 public:
    MemTableFile(const std::string& table_id, const meta::MetaPtr& meta, const DBOptions& options);

    Status
    Add(const VectorSourcePtr& source);

    Status
    Delete(segment::doc_id_t doc_id);

    Status
    Delete(const std::vector<segment::doc_id_t>& doc_ids);

    size_t
    GetCurrentMem();

    size_t
    GetMemLeft();

    bool
    IsFull();

    Status
    Serialize(uint64_t wal_lsn);

    const std::string&
    GetSegmentId() const;

 private:
    Status
    CreateTableFile();

 private:
    const std::string table_id_;
    meta::TableFileSchema table_file_schema_;
    meta::MetaPtr meta_;
    DBOptions options_;
    size_t current_mem_;

    //    ExecutionEnginePtr execution_engine_;
    segment::SegmentWriterPtr segment_writer_ptr_;
};  // MemTableFile

using MemTableFilePtr = std::shared_ptr<MemTableFile>;

}  // namespace engine
}  // namespace milvus

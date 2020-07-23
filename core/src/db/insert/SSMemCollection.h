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

#include <atomic>
#include <memory>
#include <mutex>
#include <set>
#include <string>
#include <vector>

#include "config/ConfigMgr.h"
#include "db/insert/SSMemSegment.h"
#include "db/insert/SSVectorSource.h"
#include "utils/Status.h"

namespace milvus {
namespace engine {

class SSMemCollection : public ConfigObserver {
 public:
    using SSMemCollectionFileList = std::vector<SSMemSegmentPtr>;

    SSMemCollection(int64_t collection_id, int64_t partition_id, const DBOptions& options);

    ~SSMemCollection();

    Status
    Add(const SSVectorSourcePtr& source);

    Status
    Delete(segment::doc_id_t doc_id);

    Status
    Delete(const std::vector<segment::doc_id_t>& doc_ids);

    void
    GetCurrentMemSegment(SSMemSegmentPtr& mem_segment);

    size_t
    GetTableFileCount();

    Status
    Serialize(uint64_t wal_lsn);

    bool
    Empty();

    int64_t
    GetCollectionId() const;

    int64_t
    GetPartitionId() const;

    size_t
    GetCurrentMem();

    uint64_t
    GetLSN();

    void
    SetLSN(uint64_t lsn);

 protected:
    void
    ConfigUpdate(const std::string& name) override;

 private:
    Status
    ApplyDeletes();

 private:
    int64_t collection_id_;
    int64_t partition_id_;

    SSMemCollectionFileList mem_segment_list_;

    DBOptions options_;

    std::mutex mutex_;

    std::set<segment::doc_id_t> doc_ids_to_delete_;

    std::atomic<uint64_t> lsn_;
};  // SSMemCollection

using SSMemCollectionPtr = std::shared_ptr<SSMemCollection>;

}  // namespace engine
}  // namespace milvus

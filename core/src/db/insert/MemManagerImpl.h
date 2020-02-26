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

#include "MemManager.h"
#include "MemTable.h"
#include "db/meta/Meta.h"
#include "server/Config.h"
#include "utils/Status.h"

#include <ctime>
#include <map>
#include <memory>
#include <mutex>
#include <set>
#include <string>
#include <vector>

namespace milvus {
namespace engine {

class MemManagerImpl : public MemManager {
 public:
    using Ptr = std::shared_ptr<MemManagerImpl>;

    MemManagerImpl(const meta::MetaPtr& meta, const DBOptions& options) : meta_(meta), options_(options) {
        server::Config& config = server::Config::GetInstance();
        config.GenUniqueIdentityID("MemManagerImpl", identity_);

        server::ConfigCallBackF lambda = [this](const std::string& value) -> Status {
            server::Config& config = server::Config::GetInstance();
            int64_t buffer_size;
            auto status = config.GetCacheConfigInsertBufferSize(buffer_size);
            if (status.ok()) {
                options_.insert_buffer_size_ = buffer_size * ONE_GB;
            }

            return status;
        };

        config.RegisterCallBack(server::CONFIG_CACHE, server::CONFIG_CACHE_INSERT_BUFFER_SIZE, identity_, lambda);
    }

    ~MemManagerImpl() {
        server::Config& config = server::Config::GetInstance();
        config.CancelCallBack(server::CONFIG_CACHE, server::CONFIG_CACHE_INSERT_BUFFER_SIZE, identity_);
    }

    Status
    InsertVectors(const std::string& table_id, VectorsData& vectors) override;

    Status
    Serialize(std::set<std::string>& table_ids) override;

    Status
    EraseMemVector(const std::string& table_id) override;

    size_t
    GetCurrentMutableMem() override;

    size_t
    GetCurrentImmutableMem() override;

    size_t
    GetCurrentMem() override;

 private:
    MemTablePtr
    GetMemByTable(const std::string& table_id);

    Status
    InsertVectorsNoLock(const std::string& table_id, VectorsData& vectors);
    Status
    ToImmutable();

    using MemIdMap = std::map<std::string, MemTablePtr>;
    using MemList = std::vector<MemTablePtr>;
    std::string identity_;
    MemIdMap mem_id_map_;
    MemList immu_mem_list_;
    meta::MetaPtr meta_;
    DBOptions options_;
    std::mutex mutex_;
    std::mutex serialization_mtx_;
};  // NewMemManager

}  // namespace engine
}  // namespace milvus

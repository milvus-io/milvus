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

#include "db/Options.h"
#include "utils/Status.h"
#include "db/SnapshotHandlers.h"
#include "db/snapshot/Context.h"
#include "db/snapshot/ResourceTypes.h"
#include "db/snapshot/Resources.h"
#include "wal/WalManager.h"

namespace milvus {
namespace engine {

class SSDBImpl {
 public:
    explicit SSDBImpl(const DBOptions& options);

    Status
    CreateCollection(const snapshot::CreateCollectionContext& context);

    Status
    DropCollection(const std::string& name);

    Status
    DescribeCollection(
        const std::string& collection_name, snapshot::CollectionPtr& collection,
        std::map<snapshot::FieldPtr, std::vector<snapshot::FieldElementPtr>>& fields_schema);

    Status
    HasCollection(const std::string& collection_name, bool& has_or_not);

    Status
    AllCollections(std::vector<std::string>& names);

    Status
    PreloadCollection(const std::shared_ptr<server::Context>& context, const std::string& collection_name,
                bool force = false);

    Status
    CreatePartition(const std::string& collection_name, const std::string& partition_name);

    Status
    DropPartition(const std::string& collection_name, const std::string& partition_name);

    Status
    ShowPartitions(const std::string& collection_name, std::vector<std::string>& partition_names);

    ~SSDBImpl();

    Status
    Start();

    Status
    Stop();

 private:
    DBOptions options_;
    std::atomic<bool> initialized_;
    std::shared_ptr<wal::WalManager> wal_mgr_;
};  // SSDBImpl

}  // namespace engine
}  // namespace milvus

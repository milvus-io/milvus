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

#include "db/snapshot/SnapshotPolicyFactory.h"
#include "db/Utils.h"

namespace milvus {
namespace engine {
namespace snapshot {

constexpr TS_TYPE US_PER_MS = 1000;
constexpr TS_TYPE MS_PER_S = 1000;
constexpr TS_TYPE US_PER_S = US_PER_MS * MS_PER_S;

SnapshotPolicyPtr
SnapshotPolicyFactory::Build(ServerConfig& server_config) {
    auto is_cluster = server_config.cluster.enable();
    if (!is_cluster | server_config.cluster.role() == ClusterRole::RO) {
        auto nums = server_config.general.stale_snapshots_count();
        return std::make_shared<SnapshotNumPolicy>(nums + 1);
    }

    auto duration = server_config.general.stale_snapshots_duration();
    return std::make_shared<SnapshotDurationPolicy>(duration * US_PER_S);
}

}  // namespace snapshot
}  // namespace engine
}  // namespace milvus

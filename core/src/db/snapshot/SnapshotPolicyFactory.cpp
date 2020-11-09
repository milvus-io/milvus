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
#include <fiu/fiu-local.h>

namespace milvus {
namespace engine {
namespace snapshot {

constexpr TS_TYPE US_PER_MS = 1000;
constexpr TS_TYPE MS_PER_S = 1000;
constexpr TS_TYPE US_PER_S = US_PER_MS * MS_PER_S;

SnapshotPolicyPtr
SnapshotPolicyFactory::Build(ServerConfig& server_config) {
    auto is_cluster = server_config.cluster.enable();
    auto role = server_config.cluster.role();
    fiu_do_on("snapshot.policy.ro_cluster", { is_cluster = true; role = ClusterRole::RO; });
    fiu_do_on("snapshot.policy.w_cluster", { is_cluster = true; role = ClusterRole::RW; });
    if (!is_cluster | role == ClusterRole::RO) {
        auto nums = server_config.general.stale_snapshots_count();
        fiu_do_on("snapshot.policy.stale_count_0", { nums = 0; });
        fiu_do_on("snapshot.policy.stale_count_1", { nums = 1; });
        fiu_do_on("snapshot.policy.stale_count_4", { nums = 4; });
        fiu_do_on("snapshot.policy.stale_count_8", { nums = 8; });
        fiu_do_on("snapshot.policy.stale_count_16", { nums = 16; });
        fiu_do_on("snapshot.policy.stale_count_32", { nums = 32; });
        return std::make_shared<SnapshotNumPolicy>(nums + 1);
    }

    auto duration = server_config.general.stale_snapshots_duration() * US_PER_S;
    fiu_do_on("snapshot.policy.duration_1ms", { duration = MS_PER_S; });
    fiu_do_on("snapshot.policy.duration_10ms", { duration = 10 * MS_PER_S; });
    fiu_do_on("snapshot.policy.duration_50ms", { duration = 50 * MS_PER_S; });
    fiu_do_on("snapshot.policy.duration_100ms", { duration = 100 * MS_PER_S; });
    fiu_do_on("snapshot.policy.duration_500ms", { duration = 500 * MS_PER_S; });
    return std::make_shared<SnapshotDurationPolicy>(duration);
}

}  // namespace snapshot
}  // namespace engine
}  // namespace milvus

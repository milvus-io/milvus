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

#include "db/snapshot/Context.h"
#include <sstream>

namespace milvus {
namespace engine {
namespace snapshot {

static constexpr const char* INNER_DELIMITER = ":";

std::string
PartitionContext::ToString() const {
    std::stringstream ss;
    if (id != 0) {
        ss << "PID=" << id;
    } else if (name != "") {
        ss << "PNAME=\"" << name << "\"";
    }

    return ss.str();
}

std::string
OperationContext::ToString() const {
    std::stringstream ss;
    if (new_collection_commit) {
        ss << "N_CC=" << new_collection_commit->GetID();
    }
    if (new_collection) {
        ss << ",N_CID=" << new_collection->GetID();
        ss << ",N_CNAME=\"" << new_collection->GetName() << "\"";
    }
    if (stale_partition_commit) {
        ss << ",S_PC=" << stale_partition_commit->GetID();
    }
    if (new_partition_commit) {
        ss << ",N_PC=" << new_partition_commit->GetID();
    }
    if (new_partition) {
        ss << ",N_PID=" << new_partition->GetID();
        ss << ",N_PNAME=\"" << new_partition->GetName() << "\"";
    }
    if (new_segment_commit) {
        ss << ",N_SC=" << new_segment_commit->GetID();
    }
    if (new_segment) {
        ss << ",N_SE=" << new_segment->GetID();
    }
    if (stale_segments.size()) {
        ss << ",S_SE=[";
        bool first = true;
        for (auto& f : stale_segments) {
            if (!first) {
                ss << INNER_DELIMITER;
            }
            ss << f->GetID();
            first = false;
        }
        ss << "]";
    }

    if (stale_segment_files.size() > 0) {
        ss << ",S_SF=[";
        bool first = true;
        for (auto& f : new_segment_files) {
            if (!first) {
                ss << INNER_DELIMITER;
            }
            ss << f->GetID();
            first = false;
        }
        ss << "]";
    }

    if (new_segment_files.size() > 0) {
        ss << ",N_SF=[";
        bool first = true;
        for (auto& f : new_segment_files) {
            if (!first) {
                ss << INNER_DELIMITER;
            }
            ss << f->GetID();
            first = false;
        }
        ss << "]";
    }

    return ss.str();
}

std::string
CreateCollectionContext::ToString() const {
    std::stringstream ss;
    if (collection) {
        ss << "CID=" << collection->GetID();
        ss << ",CNAME=\"" << collection->GetName() << "\"";
    }
    return ss.str();
}

}  // namespace snapshot
}  // namespace engine
}  // namespace milvus

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

#include "scheduler/resource/Node.h"

#include <atomic>
#include <utility>

namespace milvus {
namespace scheduler {

Node::Node() {
    static std::atomic_uint_fast8_t counter(0);
    id_ = counter++;
}

std::vector<Neighbour>
Node::GetNeighbours() {
    std::lock_guard<std::mutex> lk(mutex_);
    std::vector<Neighbour> ret;
    for (auto& e : neighbours_) {
        ret.push_back(e.second);
    }
    return ret;
}

json
Node::Dump() const {
    json neighbours;
    for (auto& neighbour : neighbours_) {
        json n;
        n["id"] = neighbour.first;
        n["connection"] = neighbour.second.connection.Dump();
        neighbours.push_back(n);
    }

    json ret{
        {"id", id_},
        {"neighbours", neighbours},
    };
    return ret;
}

void
Node::AddNeighbour(const NeighbourNodePtr& neighbour_node, Connection& connection) {
    std::lock_guard<std::mutex> lk(mutex_);
    neighbours_.emplace(std::make_pair(neighbour_node->id_, Neighbour(neighbour_node, connection)));
    // else do nothing, consider it..
}

}  // namespace scheduler
}  // namespace milvus

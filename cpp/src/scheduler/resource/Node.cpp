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


#include <atomic>
#include "Node.h"


namespace zilliz {
namespace milvus {
namespace engine {

Node::Node() {
    static std::atomic_uint_fast8_t counter(0);
    id_ = counter++;
}

std::vector<Neighbour> Node::GetNeighbours() {
    std::lock_guard<std::mutex> lk(mutex_);
    std::vector<Neighbour> ret;
    for (auto &e : neighbours_) {
        ret.push_back(e.second);
    }
    return ret;
}

std::string Node::Dump() {
    std::stringstream ss;
    ss << "<Node, id=" << std::to_string(id_) << ">::neighbours:" << std::endl;
    for (auto &neighbour : neighbours_) {
        ss << "\t<Neighbour, id=" << std::to_string(neighbour.first);
        ss << ", connection: " << neighbour.second.connection.Dump() << ">" << std::endl;
    }
    return ss.str();
}

void Node::AddNeighbour(const NeighbourNodePtr &neighbour_node, Connection &connection) {
    std::lock_guard<std::mutex> lk(mutex_);
    if (auto s = neighbour_node.lock()) {
        neighbours_.emplace(std::make_pair(s->id_, Neighbour(neighbour_node, connection)));
    }
    // else do nothing, consider it..
}

}
}
}

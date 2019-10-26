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

#pragma once

#include <map>
#include <memory>
#include <string>
#include <vector>

#include "Connection.h"
#include "scheduler/TaskTable.h"
#include "scheduler/interface/interfaces.h"

namespace milvus {
namespace scheduler {

class Node;

using NeighbourNodePtr = std::weak_ptr<Node>;

struct Neighbour {
    Neighbour(NeighbourNodePtr nei, Connection conn) : neighbour_node(nei), connection(conn) {
    }

    NeighbourNodePtr neighbour_node;
    Connection connection;
};

// TODO(lxj): return type void -> Status
class Node : public interface::dumpable {
 public:
    Node();

    void
    AddNeighbour(const NeighbourNodePtr& neighbour_node, Connection& connection);

    std::vector<Neighbour>
    GetNeighbours();

 public:
    json
    Dump() const override;

 private:
    std::mutex mutex_;
    uint8_t id_;
    std::map<uint8_t, Neighbour> neighbours_;
};

using NodePtr = std::shared_ptr<Node>;
using NodeWPtr = std::weak_ptr<Node>;

}  // namespace scheduler
}  // namespace milvus

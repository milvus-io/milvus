/*******************************************************************************
 * Copyright 上海赜睿信息科技有限公司(Zilliz) - All Rights Reserved
 * Unauthorized copying of this file, via any medium is strictly prohibited.
 * Proprietary and confidential.
 ******************************************************************************/
#pragma once

#include <vector>
#include <memory>
#include <map>

#include "../TaskTable.h"
#include "Connection.h"


namespace zilliz {
namespace milvus {
namespace engine {

class Node;

using NeighbourNodePtr = std::weak_ptr<Node>;

struct Neighbour {
    Neighbour(NeighbourNodePtr nei, Connection conn) 
    : neighbour_node(nei), connection(conn) {}

    NeighbourNodePtr neighbour_node;
    Connection connection;
};

// TODO(linxj): return type void -> Status
class Node {
public:
    Node();

    void
    AddNeighbour(const NeighbourNodePtr &neighbour_node, Connection &connection);

    void
    DelNeighbour(const NeighbourNodePtr &neighbour_ptr);

    bool
    IsNeighbour(const NeighbourNodePtr& neighbour_ptr);

    std::vector<Neighbour>
    GetNeighbours();

public:
    std::string
    Dump();

private:
    std::mutex mutex_;
    uint8_t id_;
    std::map<uint8_t, Neighbour> neighbours_;
};

using NodePtr = std::shared_ptr<Node>;
using NodeWPtr = std::weak_ptr<Node>;

}
}
}

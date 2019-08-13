/*******************************************************************************
 * Copyright 上海赜睿信息科技有限公司(Zilliz) - All Rights Reserved
 * Unauthorized copying of this file, via any medium is strictly prohibited.
 * Proprietary and confidential.
 ******************************************************************************/
#pragma once

#include <vector>
#include <memory>

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

class Node {
public:
    void
    AddNeighbour(const NeighbourNodePtr &neighbour_node, Connection &connection) {
        Neighbour neighbour(neighbour_node, connection);
        neighbours_.emplace_back(neighbour);
    }

    void
    DelNeighbour(NeighbourNodePtr neighbour_ptr) {}

    bool
    IsNeighbour(NeighbourNodePtr neighbour_ptr) {}

    const std::vector<Neighbour> &
    GetNeighbours() {}

public:
    std::string
    Dump();

private:
    std::vector<Neighbour> neighbours_;
};

using NodePtr = std::shared_ptr<Node>;
using NodeWPtr = std::weak_ptr<Node>;

}
}
}

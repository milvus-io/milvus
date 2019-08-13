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
    NeighbourNodePtr neighbour_node;
    Connection connection;
};

class Node {
public:
    void
    AddNeighbour(const NeighbourNodePtr &neighbour_node, Connection &connection) {
        Neighbour neighbour{.neighbour_node = neighbour_node, .connection = connection};
        neighbours_.push_back(neighbour);
    }

    void
    DelNeighbour(NeighbourNodePtr &neighbour_ptr);

    bool
    IsNeighbour(NeighbourNodePtr &neighbour_ptr);

    std::vector<NeighbourNodePtr>
    GetNeighbours();

public:
    std::string
    Dump();

private:
    std::vector<Neighbour> neighbours_;
};

}
}
}

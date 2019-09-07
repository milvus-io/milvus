/*******************************************************************************
 * Copyright 上海赜睿信息科技有限公司(Zilliz) - All Rights Reserved
 * Unauthorized copying of this file, via any medium is strictly prohibited.
 * Proprietary and confidential.
 ******************************************************************************/

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

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

void Node::DelNeighbour(const NeighbourNodePtr &neighbour_ptr) {
    std::lock_guard<std::mutex> lk(mutex_);
    if (auto s = neighbour_ptr.lock()) {
        auto search = neighbours_.find(s->id_);
        if (search != neighbours_.end()) {
            neighbours_.erase(search);
        }
    }
}

bool Node::IsNeighbour(const NeighbourNodePtr &neighbour_ptr) {
    std::lock_guard<std::mutex> lk(mutex_);
    if (auto s = neighbour_ptr.lock()) {
        auto search = neighbours_.find(s->id_);
        if (search != neighbours_.end()) {
            return true;
        }
    }
    return false;
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
    // TODO(linxj): what's that?
    return std::__cxx11::string();
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

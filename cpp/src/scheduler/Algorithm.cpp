/*******************************************************************************
 * Copyright 上海赜睿信息科技有限公司(Zilliz) - All Rights Reserved
 * Unauthorized copying of this file, via any medium is strictly prohibited.
 * Proprietary and confidential.
 ******************************************************************************/

#include "Algorithm.h"

namespace zilliz {
namespace milvus {
namespace engine {

std::vector<std::string>
ShortestPath(const ResourcePtr &src, const ResourcePtr& dest) {
    auto node = std::static_pointer_cast<Node>(src);
    auto neighbours = node->GetNeighbours();
    for (auto &neighbour : neighbours) {
        neighbour.connection.speed()
    }
}

}
}
}
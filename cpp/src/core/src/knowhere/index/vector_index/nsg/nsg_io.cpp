////////////////////////////////////////////////////////////////////////////////
// Copyright 上海赜睿信息科技有限公司(Zilliz) - All Rights Reserved
// Unauthorized copying of this file, via any medium is strictly prohibited.
// Proprietary and confidential.
////////////////////////////////////////////////////////////////////////////////

#include <cstring>

#include "knowhere/index/vector_index/nsg/nsg_io.h"


namespace zilliz {
namespace knowhere {
namespace algo {

void write_index(NsgIndex *index, MemoryIOWriter &writer) {
    writer(&index->ntotal, sizeof(index->ntotal), 1);
    writer(&index->dimension, sizeof(index->dimension), 1);
    writer(&index->navigation_point, sizeof(index->navigation_point), 1);
    writer(index->ori_data_, sizeof(float) * index->ntotal * index->dimension, 1);
    writer(index->ids_, sizeof(long) * index->ntotal, 1);

    for (unsigned i = 0; i < index->ntotal; ++i) {
        auto neighbor_num = (node_t) index->nsg[i].size();
        writer(&neighbor_num, sizeof(node_t), 1);
        writer(index->nsg[i].data(), neighbor_num * sizeof(node_t), 1);
    }
}

NsgIndex *read_index(MemoryIOReader &reader) {
    size_t ntotal;
    size_t dimension;
    reader(&ntotal, sizeof(size_t), 1);
    reader(&dimension, sizeof(size_t), 1);
    auto index = new NsgIndex(dimension, ntotal);
    reader(&index->navigation_point, sizeof(index->navigation_point), 1);

    index->ori_data_ = new float[index->ntotal * index->dimension];
    index->ids_ = new long[index->ntotal];
    reader(index->ori_data_, sizeof(float) * index->ntotal * index->dimension, 1);
    reader(index->ids_, sizeof(long) * index->ntotal, 1);

    index->nsg.reserve(index->ntotal);
    index->nsg.resize(index->ntotal);
    node_t neighbor_num;
    for (unsigned i = 0; i < index->ntotal; ++i) {
        reader(&neighbor_num, sizeof(node_t), 1);
        index->nsg[i].reserve(neighbor_num);
        index->nsg[i].resize(neighbor_num);
        reader(index->nsg[i].data(), neighbor_num * sizeof(node_t), 1);
    }

    index->is_trained = true;
    return index;
}

}
}
}

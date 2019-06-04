/*******************************************************************************
 * Copyright 上海赜睿信息科技有限公司(Zilliz) - All Rights Reserved
 * Unauthorized copying of this file, via any medium is strictly prohibited.
 * Proprietary and confidential.
 ******************************************************************************/
#pragma once

#include "SearchContext.h"
#include "utils/BlockingQueue.h"
#include "../FaissExecutionEngine.h"
#include "../Traits.h"

#include <memory>

namespace zilliz {
namespace vecwise {
namespace engine {

#ifdef GPU_VERSION
using IndexTraitClass = IVFIndexTrait;
#else
using IndexTraitClass = IDMapIndexTrait;
#endif

using IndexClass = FaissExecutionEngine<IndexTraitClass>;
using IndexEnginePtr = std::shared_ptr<IndexClass>;

template <typename trait>
class SearchTask {
public:
    bool DoSearch();

public:
    size_t index_id_ = 0;
    int index_type_ = 0; //for metrics
    IndexEnginePtr index_engine_;
    std::vector<SearchContextPtr> search_contexts_;
};

using SearchTaskClass = SearchTask<IndexTraitClass>;
using SearchTaskPtr = std::shared_ptr<SearchTaskClass>;

class SearchTaskQueue : public server::BlockingQueue<SearchTaskPtr> {
private:
    SearchTaskQueue() {}

    SearchTaskQueue(const SearchTaskQueue &rhs) = delete;

    SearchTaskQueue &operator=(const SearchTaskQueue &rhs) = delete;

public:
    static SearchTaskQueue& GetInstance();

private:

};


}
}
}

#include "SearchTaskQueue.inl"
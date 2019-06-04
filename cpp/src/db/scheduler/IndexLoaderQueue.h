/*******************************************************************************
 * Copyright 上海赜睿信息科技有限公司(Zilliz) - All Rights Reserved
 * Unauthorized copying of this file, via any medium is strictly prohibited.
 * Proprietary and confidential.
 ******************************************************************************/
#pragma once

#include "SearchContext.h"

#include <condition_variable>
#include <iostream>
#include <queue>
#include <list>


namespace zilliz {
namespace vecwise {
namespace engine {


class IndexLoaderContext {
public:
    TableFileSchemaPtr file_;
    std::vector<SearchContextPtr> search_contexts_;
};
using IndexLoaderContextPtr = std::shared_ptr<IndexLoaderContext>;

class IndexLoaderQueue {
private:
    IndexLoaderQueue() : mtx(), full_(), empty_() {}

    IndexLoaderQueue(const IndexLoaderQueue &rhs) = delete;

    IndexLoaderQueue &operator=(const IndexLoaderQueue &rhs) = delete;

public:
    using LoaderQueue = std::list<IndexLoaderContextPtr>;

    static IndexLoaderQueue& GetInstance();

    void Put(const SearchContextPtr &search_context);

    IndexLoaderContextPtr Take();

    IndexLoaderContextPtr Front();

    IndexLoaderContextPtr Back();

    size_t Size();

    bool Empty();

    void SetCapacity(const size_t capacity);

private:
    mutable std::mutex mtx;
    std::condition_variable full_;
    std::condition_variable empty_;

    LoaderQueue queue_;
    size_t capacity_ = 1000000;
};

}
}
}

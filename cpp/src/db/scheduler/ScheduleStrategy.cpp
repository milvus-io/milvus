/*******************************************************************************
 * Copyright 上海赜睿信息科技有限公司(Zilliz) - All Rights Reserved
 * Unauthorized copying of this file, via any medium is strictly prohibited.
 * Proprietary and confidential.
 ******************************************************************************/


#include "ScheduleStrategy.h"
#include "cache/CpuCacheMgr.h"
#include "utils/Error.h"
#include "utils/Log.h"

namespace zilliz {
namespace vecwise {
namespace engine {

class MemScheduleStrategy : public IScheduleStrategy {
public:
    bool Schedule(const SearchContextPtr &search_context, IndexLoaderQueue::LoaderQueue& loader_list) override;
};

ScheduleStrategyPtr CreateStrategy() {
    ScheduleStrategyPtr strategy(new MemScheduleStrategy());
    return strategy;
}

bool MemScheduleStrategy::Schedule(const SearchContextPtr &search_context,
                                   IndexLoaderQueue::LoaderQueue &loader_list) {
    if(search_context == nullptr) {
        return false;
    }

    SearchContext::Id2IndexMap index_files = search_context->GetIndexMap();
    //some index loader alread exists
    for(auto iter = loader_list.begin(); iter != loader_list.end(); ++iter) {
        if(index_files.find((*iter)->file_->id) != index_files.end()){
            SERVER_LOG_INFO << "Append SearchContext to exist IndexLoaderContext";
            index_files.erase((*iter)->file_->id);
            (*iter)->search_contexts_.push_back(search_context);
        }
    }

    //index_files still contains some index files, create new loader
    for(auto iter = index_files.begin(); iter != index_files.end(); ++iter) {
        SERVER_LOG_INFO << "Create new IndexLoaderContext for: " << iter->second->location;
        IndexLoaderContextPtr new_loader = std::make_shared<IndexLoaderContext>();
        new_loader->search_contexts_.push_back(search_context);
        new_loader->file_ = iter->second;

        auto index  = zilliz::vecwise::cache::CpuCacheMgr::GetInstance()->GetIndex(iter->second->location);
        if(index != nullptr) {
            //if the index file has been in memory, increase its priority
            loader_list.push_front(new_loader);
        } else {
            //index file not in memory, put it to tail
            loader_list.push_back(new_loader);
        }
    }

    return true;
}

}
}
}
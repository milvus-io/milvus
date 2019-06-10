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
    bool Schedule(const SearchContextPtr &search_context, IndexLoaderQueue::LoaderQueue& loader_list) override {
        if(search_context == nullptr) {
            return false;
        }

        SearchContext::Id2IndexMap index_files = search_context->GetIndexMap();
        //some index loader alread exists
        for(auto& loader : loader_list) {
            if(index_files.find(loader->file_->id_) != index_files.end()){
                SERVER_LOG_INFO << "Append SearchContext to exist IndexLoaderContext";
                index_files.erase(loader->file_->id_);
                loader->search_contexts_.push_back(search_context);
            }
        }

        //index_files still contains some index files, create new loader
        for(auto& pair : index_files) {
            SERVER_LOG_INFO << "Create new IndexLoaderContext for: " << pair.second->location_;
            IndexLoaderContextPtr new_loader = std::make_shared<IndexLoaderContext>();
            new_loader->search_contexts_.push_back(search_context);
            new_loader->file_ = pair.second;

            auto index  = zilliz::vecwise::cache::CpuCacheMgr::GetInstance()->GetIndex(pair.second->location_);
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
};


////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
ScheduleStrategyPtr StrategyFactory::CreateMemStrategy() {
    ScheduleStrategyPtr strategy(new MemScheduleStrategy());
    return strategy;
}

}
}
}
/*******************************************************************************
 * Copyright 上海赜睿信息科技有限公司(Zilliz) - All Rights Reserved
 * Unauthorized copying of this file, via any medium is strictly prohibited.
 * Proprietary and confidential.
 ******************************************************************************/
#include "TaskDispatchStrategy.h"
#include "context/SearchContext.h"
#include "context/DeleteContext.h"
#include "task/IndexLoadTask.h"
#include "task/DeleteTask.h"
#include "cache/CpuCacheMgr.h"
#include "utils/Error.h"
#include "db/Log.h"

namespace zilliz {
namespace milvus {
namespace engine {

class ReuseCacheIndexStrategy {
public:
    bool Schedule(const SearchContextPtr &context, std::list<ScheduleTaskPtr>& task_list) {
        if(context == nullptr) {
            ENGINE_LOG_ERROR << "Task Dispatch context doesn't exist";
            return false;
        }

        SearchContext::Id2IndexMap index_files = context->GetIndexMap();
        //some index loader alread exists
        for(auto& task : task_list) {
            if(task->type() != ScheduleTaskType::kIndexLoad) {
                continue;
            }

            IndexLoadTaskPtr loader = std::static_pointer_cast<IndexLoadTask>(task);
            if(index_files.find(loader->file_->id_) != index_files.end()){
                ENGINE_LOG_DEBUG << "Append SearchContext to exist IndexLoaderContext";
                index_files.erase(loader->file_->id_);
                loader->search_contexts_.push_back(context);
            }
        }

        //index_files still contains some index files, create new loader
        for(auto& pair : index_files) {
            ENGINE_LOG_DEBUG << "Create new IndexLoaderContext for: " << pair.second->location_;
            IndexLoadTaskPtr new_loader = std::make_shared<IndexLoadTask>();
            new_loader->search_contexts_.push_back(context);
            new_loader->file_ = pair.second;

            auto index  = zilliz::milvus::cache::CpuCacheMgr::GetInstance()->GetIndex(pair.second->location_);
            if(index != nullptr) {
                //if the index file has been in memory, increase its priority
                task_list.push_front(new_loader);
            } else {
                //index file not in memory, put it to tail
                task_list.push_back(new_loader);
            }
        }

        return true;
    }
};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
class DeleteTableStrategy {
public:
    bool Schedule(const DeleteContextPtr &context, std::list<ScheduleTaskPtr> &task_list) {
        if (context == nullptr) {
            ENGINE_LOG_ERROR << "Task Dispatch context doesn't exist";
            return false;
        }

        DeleteTaskPtr delete_task = std::make_shared<DeleteTask>(context);
        if(task_list.empty()) {
            task_list.push_back(delete_task);
            return true;
        }

        std::string table_id = context->table_id();

        //put delete task to proper position
        //for example: task_list has 10 IndexLoadTask, only the No.5 IndexLoadTask is for table1
        //if user want to delete table1, the DeleteTask will be insert into No.6 position
        for(std::list<ScheduleTaskPtr>::reverse_iterator iter = task_list.rbegin(); iter != task_list.rend(); ++iter) {
            if((*iter)->type() != ScheduleTaskType::kIndexLoad) {
                continue;
            }

            IndexLoadTaskPtr loader = std::static_pointer_cast<IndexLoadTask>(*iter);
            if(loader->file_->table_id_ != table_id) {
                continue;
            }

            task_list.insert(iter.base(), delete_task);
            return true;
        }

        //no task is searching this table, put DeleteTask to front of list so that the table will be delete asap
        task_list.push_front(delete_task);
        return true;
    }
};


////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
bool TaskDispatchStrategy::Schedule(const ScheduleContextPtr &context_ptr,
                                    std::list<zilliz::milvus::engine::ScheduleTaskPtr> &task_list) {
    if(context_ptr == nullptr) {
        ENGINE_LOG_ERROR << "Task Dispatch context doesn't exist";
        return false;
    }

    switch(context_ptr->type()) {
        case ScheduleContextType::kSearch: {
            SearchContextPtr search_context = std::static_pointer_cast<SearchContext>(context_ptr);
            ReuseCacheIndexStrategy strategy;
            return strategy.Schedule(search_context, task_list);
        }
        case ScheduleContextType::kDelete: {
            DeleteContextPtr delete_context = std::static_pointer_cast<DeleteContext>(context_ptr);
            DeleteTableStrategy strategy;
            return strategy.Schedule(delete_context, task_list);
        }
        default:
            ENGINE_LOG_ERROR << "Invalid schedule task type";
            return false;
    }
}

}
}
}
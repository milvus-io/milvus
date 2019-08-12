/*******************************************************************************
 * Copyright 上海赜睿信息科技有限公司(Zilliz) - All Rights Reserved
 * Unauthorized copying of this file, via any medium is strictly prohibited.
 * Proprietary and confidential.
 ******************************************************************************/

#include "IndexLoadTask.h"
#include "SearchTask.h"
#include "db/Log.h"
#include "db/engine/EngineFactory.h"
#include "utils/TimeRecorder.h"
#include "metrics/Metrics.h"

namespace zilliz {
namespace milvus {
namespace engine {

namespace {
void CollectFileMetrics(int file_type, size_t file_size) {
    switch(file_type) {
        case meta::TableFileSchema::RAW:
        case meta::TableFileSchema::TO_INDEX: {
            server::Metrics::GetInstance().RawFileSizeHistogramObserve(file_size);
            server::Metrics::GetInstance().RawFileSizeTotalIncrement(file_size);
            server::Metrics::GetInstance().RawFileSizeGaugeSet(file_size);
            break;
        }
        default: {
            server::Metrics::GetInstance().IndexFileSizeHistogramObserve(file_size);
            server::Metrics::GetInstance().IndexFileSizeTotalIncrement(file_size);
            server::Metrics::GetInstance().IndexFileSizeGaugeSet(file_size);
            break;
        }
    }
}
}

IndexLoadTask::IndexLoadTask()
    : IScheduleTask(ScheduleTaskType::kIndexLoad) {

}

std::shared_ptr<IScheduleTask> IndexLoadTask::Execute() {
    server::TimeRecorder rc("");
    //step 1: load index
    ExecutionEnginePtr index_ptr = EngineFactory::Build(file_->dimension_,
                                                        file_->location_,
                                                        (EngineType)file_->engine_type_);

    try {
        index_ptr->Load();
    } catch (std::exception& ex) {
        //typical error: out of disk space or permition denied
        std::string msg = "Failed to load index file: " + std::string(ex.what());
        ENGINE_LOG_ERROR << msg;

        for(auto& context : search_contexts_) {
            context->IndexSearchDone(file_->id_);//mark as done avoid dead lock, even failed
        }

        return nullptr;
    }

    size_t file_size = index_ptr->PhysicalSize();

    std::string info = "Load file id:" + std::to_string(file_->id_) + " file type:" + std::to_string(file_->file_type_)
                       + " size:" + std::to_string(file_size) + " bytes from location: " + file_->location_ + " totally cost";
    double span = rc.ElapseFromBegin(info);
    for(auto& context : search_contexts_) {
        context->AccumLoadCost(span);
    }

    CollectFileMetrics(file_->file_type_, file_size);

    //step 2: return search task for later execution
    SearchTaskPtr task_ptr = std::make_shared<SearchTask>();
    task_ptr->index_id_ = file_->id_;
    task_ptr->index_type_ = file_->file_type_;
    task_ptr->index_engine_ = index_ptr;
    task_ptr->search_contexts_.swap(search_contexts_);
    return std::static_pointer_cast<IScheduleTask>(task_ptr);
}

}
}
}
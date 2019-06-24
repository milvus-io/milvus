/*******************************************************************************
 * Copyright 上海赜睿信息科技有限公司(Zilliz) - All Rights Reserved
 * Unauthorized copying of this file, via any medium is strictly prohibited.
 * Proprietary and confidential.
 ******************************************************************************/

#include "IndexLoadTask.h"
#include "SearchTask.h"
#include "db/Log.h"
#include "db/EngineFactory.h"
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
    ENGINE_LOG_INFO << "Loading index(" << file_->id_ << ") from location: " << file_->location_;

    server::TimeRecorder rc("Load index");
    //step 1: load index
    ExecutionEnginePtr index_ptr = EngineFactory::Build(file_->dimension_,
                                                        file_->location_,
                                                        (EngineType)file_->engine_type_);
    index_ptr->Load();

    rc.Record("load index file to memory");

    size_t file_size = index_ptr->PhysicalSize();
    LOG(DEBUG) << "Index file type " << file_->file_type_ << " Of Size: "
               << file_size/(1024*1024) << " M";

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
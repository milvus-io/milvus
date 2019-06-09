/*******************************************************************************
 * Copyright 上海赜睿信息科技有限公司(Zilliz) - All Rights Reserved
 * Unauthorized copying of this file, via any medium is strictly prohibited.
 * Proprietary and confidential.
 ******************************************************************************/

#include "SearchScheduler.h"
#include "IndexLoaderQueue.h"
#include "SearchTaskQueue.h"
#include "utils/Log.h"
#include "utils/TimeRecorder.h"
#include "metrics/Metrics.h"
#include "db/EngineFactory.h"

namespace zilliz {
namespace vecwise {
namespace engine {

SearchScheduler::SearchScheduler()
    : thread_pool_(2),
      stopped_(true) {
    Start();
}

SearchScheduler::~SearchScheduler() {
    Stop();
}

SearchScheduler& SearchScheduler::GetInstance() {
    static SearchScheduler s_instance;
    return s_instance;
}

bool
SearchScheduler::Start() {
    if(!stopped_) {
        return true;
    }

    thread_pool_.enqueue(&SearchScheduler::IndexLoadWorker, this);
    thread_pool_.enqueue(&SearchScheduler::SearchWorker, this);
    return true;
}

bool
SearchScheduler::Stop() {
    if(stopped_) {
        return true;
    }

    IndexLoaderQueue& index_queue = IndexLoaderQueue::GetInstance();
    index_queue.Put(nullptr);

    SearchTaskQueue& search_queue = SearchTaskQueue::GetInstance();
    search_queue.Put(nullptr);

    return true;
}

bool
SearchScheduler::ScheduleSearchTask(SearchContextPtr& search_context) {
    IndexLoaderQueue& index_queue = IndexLoaderQueue::GetInstance();
    index_queue.Put(search_context);

    return true;
}

bool
SearchScheduler::IndexLoadWorker() {
    IndexLoaderQueue& index_queue = IndexLoaderQueue::GetInstance();
    SearchTaskQueue& search_queue = SearchTaskQueue::GetInstance();
    while(true) {
        IndexLoaderContextPtr context = index_queue.Take();
        if(context == nullptr) {
            SERVER_LOG_INFO << "Stop thread for index loading";
            break;//exit
        }

        SERVER_LOG_INFO << "Loading index(" << context->file_->id << ") from location: " << context->file_->location;

        server::TimeRecorder rc("Load index");
        //load index
        ExecutionEnginePtr index_ptr = EngineFactory::Build(context->file_->dimension,
                context->file_->location,
                (EngineType)context->file_->engine_type_);
        index_ptr->Load();

        rc.Record("load index file to memory");

        size_t file_size = index_ptr->PhysicalSize();
        LOG(DEBUG) << "Index file type " << context->file_->file_type << " Of Size: "
                   << file_size/(1024*1024) << " M";

        //metric
        switch(context->file_->file_type) {
            case meta::TableFileSchema::RAW: {
                server::Metrics::GetInstance().RawFileSizeHistogramObserve(file_size);
                server::Metrics::GetInstance().RawFileSizeTotalIncrement(file_size);
                server::Metrics::GetInstance().RawFileSizeGaugeSet(file_size);
                break;
            }
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

        //put search task to another queue
        SearchTaskPtr task_ptr = std::make_shared<SearchTask>();
        task_ptr->index_id_ = context->file_->id;
        task_ptr->index_type_ = context->file_->file_type;
        task_ptr->index_engine_ = index_ptr;
        task_ptr->search_contexts_.swap(context->search_contexts_);
        search_queue.Put(task_ptr);
    }

    return true;
}

bool
SearchScheduler::SearchWorker() {
    SearchTaskQueue& search_queue = SearchTaskQueue::GetInstance();
    while(true) {
        SearchTaskPtr task_ptr = search_queue.Take();
        if(task_ptr == nullptr) {
            SERVER_LOG_INFO << "Stop thread for searching";
            break;//exit
        }

        SERVER_LOG_INFO << "Searching in index(" << task_ptr->index_id_<< ") with "
            << task_ptr->search_contexts_.size() << " tasks";

        //do search
        auto start_time = METRICS_NOW_TIME;
        task_ptr->DoSearch();
        auto end_time = METRICS_NOW_TIME;
        auto total_time = METRICS_MICROSECONDS(start_time, end_time);
        switch(task_ptr->index_type_) {
            case meta::TableFileSchema::RAW: {
                server::Metrics::GetInstance().SearchRawDataDurationSecondsHistogramObserve(total_time);
                break;
            }
            case meta::TableFileSchema::TO_INDEX: {
                server::Metrics::GetInstance().SearchRawDataDurationSecondsHistogramObserve(total_time);
                break;
            }
            default: {
                server::Metrics::GetInstance().SearchIndexDataDurationSecondsHistogramObserve(total_time);
                break;
            }
        }
    }

    return true;
}

}
}
}
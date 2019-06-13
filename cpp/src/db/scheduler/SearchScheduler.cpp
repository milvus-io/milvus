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

void CollectDurationMetrics(int index_type, double total_time) {
    switch(index_type) {
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

}

SearchScheduler::SearchScheduler()
    : stopped_(true) {
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

    stopped_ = false;

    search_queue_.SetCapacity(2);

    index_load_thread_ = std::make_shared<std::thread>(&SearchScheduler::IndexLoadWorker, this);
    search_thread_  = std::make_shared<std::thread>(&SearchScheduler::SearchWorker, this);

    return true;
}

bool
SearchScheduler::Stop() {
    if(stopped_) {
        return true;
    }

    if(index_load_thread_) {
        index_load_queue_.Put(nullptr);
        index_load_thread_->join();
        index_load_thread_ = nullptr;
    }

    if(search_thread_) {
        search_queue_.Put(nullptr);
        search_thread_->join();
        search_thread_ = nullptr;
    }

    stopped_ = true;

    return true;
}

bool
SearchScheduler::ScheduleSearchTask(SearchContextPtr& search_context) {
    index_load_queue_.Put(search_context);

    return true;
}

bool
SearchScheduler::IndexLoadWorker() {
    while(true) {
        IndexLoaderContextPtr context = index_load_queue_.Take();
        if(context == nullptr) {
            SERVER_LOG_INFO << "Stop thread for index loading";
            break;//exit
        }

        SERVER_LOG_INFO << "Loading index(" << context->file_->id_ << ") from location: " << context->file_->location_;

        server::TimeRecorder rc("Load index");
        //step 1: load index
        ExecutionEnginePtr index_ptr = EngineFactory::Build(context->file_->dimension_,
                context->file_->location_,
                (EngineType)context->file_->engine_type_);
        index_ptr->Load();

        rc.Record("load index file to memory");

        size_t file_size = index_ptr->PhysicalSize();
        LOG(DEBUG) << "Index file type " << context->file_->file_type_ << " Of Size: "
                   << file_size/(1024*1024) << " M";

        CollectFileMetrics(context->file_->file_type_, file_size);

        //step 2: put search task into another queue
        SearchTaskPtr task_ptr = std::make_shared<SearchTask>();
        task_ptr->index_id_ = context->file_->id_;
        task_ptr->index_type_ = context->file_->file_type_;
        task_ptr->index_engine_ = index_ptr;
        task_ptr->search_contexts_.swap(context->search_contexts_);
        search_queue_.Put(task_ptr);
    }

    return true;
}

bool
SearchScheduler::SearchWorker() {
    while(true) {
        SearchTaskPtr task_ptr = search_queue_.Take();
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
        CollectDurationMetrics(task_ptr->index_type_, total_time);
    }

    return true;
}

}
}
}
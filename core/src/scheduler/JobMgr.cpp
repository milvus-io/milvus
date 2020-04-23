// Copyright (C) 2019-2020 Zilliz. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software distributed under the License
// is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
// or implied. See the License for the specific language governing permissions and limitations under the License.

#include "scheduler/JobMgr.h"

#include "src/db/Utils.h"
#include "src/segment/SegmentReader.h"

#include <limits>
#include <utility>

#include "SchedInst.h"
#include "TaskCreator.h"
#include "optimizer/Optimizer.h"
#include "scheduler/Algorithm.h"
#include "scheduler/optimizer/Optimizer.h"
#include "scheduler/tasklabel/SpecResLabel.h"
#include "task/Task.h"

namespace milvus {
namespace scheduler {

JobMgr::JobMgr(ResourceMgrPtr res_mgr) : res_mgr_(std::move(res_mgr)) {
}

void
JobMgr::Start() {
    if (not running_) {
        running_ = true;
        worker_thread_ = std::thread(&JobMgr::worker_function, this);
    }
}

void
JobMgr::Stop() {
    if (running_) {
        this->Put(nullptr);
        worker_thread_.join();
        running_ = false;
    }
}

json
JobMgr::Dump() const {
    json ret{
        {"running", running_},
        {"event_queue_length", queue_.size()},
    };
    return ret;
}

void
JobMgr::Put(const JobPtr& job) {
    {
        std::lock_guard<std::mutex> lock(mutex_);
        queue_.push(job);
    }
    cv_.notify_one();
}

void
JobMgr::worker_function() {
    SetThreadName("jobmgr_thread");
    while (running_) {
        std::unique_lock<std::mutex> lock(mutex_);
        cv_.wait(lock, [this] { return !queue_.empty(); });
        auto job = queue_.front();
        queue_.pop();
        lock.unlock();
        if (job == nullptr) {
            break;
        }

        auto tasks = build_task(job);

        // TODO(zhiru): if the job is search by ids, pass any task where the ids don't exist
        auto search_job = std::dynamic_pointer_cast<SearchJob>(job);
        if (search_job != nullptr) {
            scheduler::ResultIds ids(search_job->nq() * search_job->topk(), -1);
            scheduler::ResultDistances distances(search_job->nq() * search_job->topk(),
                                                 std::numeric_limits<float>::max());
            search_job->GetResultIds() = ids;
            search_job->GetResultDistances() = distances;

            if (search_job->vectors().float_data_.empty() && search_job->vectors().binary_data_.empty() &&
                !search_job->vectors().id_array_.empty()) {
                for (auto task = tasks.begin(); task != tasks.end();) {
                    auto search_task = std::static_pointer_cast<XSearchTask>(*task);
                    auto location = search_task->GetLocation();

                    // Load bloom filter
                    std::string segment_dir;
                    engine::utils::GetParentPath(location, segment_dir);
                    segment::SegmentReader segment_reader(segment_dir);
                    segment::IdBloomFilterPtr id_bloom_filter_ptr;
                    segment_reader.LoadBloomFilter(id_bloom_filter_ptr);

                    // Check if the id is present.
                    bool pass = true;
                    for (auto& id : search_job->vectors().id_array_) {
                        if (id_bloom_filter_ptr->Check(id)) {
                            pass = false;
                            break;
                        }
                    }

                    if (pass) {
                        //                        std::cout << search_task->GetIndexId() << std::endl;
                        search_job->SearchDone(search_task->GetIndexId());
                        task = tasks.erase(task);
                    } else {
                        task++;
                    }
                }
            }
        }

        //        for (auto &task : tasks) {
        //            if ...
        //            search_job->SearchDone(task->id);
        //            tasks.erase(task);
        //        }

        for (auto& task : tasks) {
            OptimizerInst::GetInstance()->Run(task);
        }

        for (auto& task : tasks) {
            calculate_path(res_mgr_, task);
        }

        // disk resources NEVER be empty.
        if (auto disk = res_mgr_->GetDiskResources()[0].lock()) {
            // if (auto disk = res_mgr_->GetCpuResources()[0].lock()) {
            for (auto& task : tasks) {
                disk->task_table().Put(task, nullptr);
            }
        }
    }
}

std::vector<TaskPtr>
JobMgr::build_task(const JobPtr& job) {
    return TaskCreator::Create(job);
}

void
JobMgr::calculate_path(const ResourceMgrPtr& res_mgr, const TaskPtr& task) {
    if (task->type_ != TaskType::SearchTask && task->type_ != TaskType::BuildIndexTask) {
        return;
    }

    if (task->label()->Type() != TaskLabelType::SPECIFIED_RESOURCE) {
        return;
    }

    std::vector<std::string> path;
    auto spec_label = std::static_pointer_cast<SpecResLabel>(task->label());
    auto src = res_mgr->GetDiskResources()[0];
    auto dest = spec_label->resource();
    ShortestPath(src.lock(), dest.lock(), res_mgr, path);
    task->path() = Path(path, path.size() - 1);
}

}  // namespace scheduler
}  // namespace milvus

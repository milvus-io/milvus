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

#include "scheduler/task/SSSearchTask.h"

#include <fiu-local.h>

#include <algorithm>
#include <memory>
#include <string>
#include <thread>
#include <unordered_map>
#include <utility>

#include "db/Utils.h"
#include "db/engine/SSExecutionEngineImpl.h"
#include "scheduler/SchedInst.h"
#include "scheduler/job/SSSearchJob.h"
#include "segment/SegmentReader.h"
#include "utils/Log.h"
#include "utils/TimeRecorder.h"

namespace milvus {
namespace scheduler {

XSSSearchTask::XSSSearchTask(const server::ContextPtr& context, const std::string& dir_root,
                             const engine::SegmentVisitorPtr& visitor, TaskLabelPtr label)
    : Task(TaskType::SearchTask, std::move(label)), context_(context), visitor_(visitor) {
    engine_ = std::make_shared<engine::SSExecutionEngineImpl>(dir_root, visitor);
}

void
XSSSearchTask::Load(LoadType type, uint8_t device_id) {
    auto seg_id = visitor_->GetSegment()->GetID();
    TimeRecorder rc(LogOut("[%s][%ld]", "search", seg_id));
    Status stat = Status::OK();
    std::string error_msg;
    std::string type_str;

    try {
        fiu_do_on("XSearchTask.Load.throw_std_exception", throw std::exception());
        if (type == LoadType::DISK2CPU) {
            stat = engine_->Load(nullptr);
            // stat = engine_->LoadAttr();
            type_str = "DISK2CPU";
        } else if (type == LoadType::CPU2GPU) {
            stat = engine_->CopyToGpu(device_id);
            type_str = "CPU2GPU" + std::to_string(device_id);
        } else if (type == LoadType::GPU2CPU) {
            // stat = engine_->CopyToCpu();
            type_str = "GPU2CPU";
        } else {
            error_msg = "Wrong load type";
            stat = Status(SERVER_UNEXPECTED_ERROR, error_msg);
        }
    } catch (std::exception& ex) {
        // typical error: out of disk space or permition denied
        error_msg = "Failed to load index file: " + std::string(ex.what());
        LOG_ENGINE_ERROR_ << LogOut("[%s][%ld] Encounter exception: %s", "search", 0, error_msg.c_str());
        stat = Status(SERVER_UNEXPECTED_ERROR, error_msg);
    }
    fiu_do_on("XSearchTask.Load.out_of_memory", stat = Status(SERVER_UNEXPECTED_ERROR, "out of memory"));

    if (!stat.ok()) {
        Status s;
        if (stat.ToString().find("out of memory") != std::string::npos) {
            error_msg = "out of memory: " + type_str + " : " + stat.message();
            s = Status(SERVER_OUT_OF_MEMORY, error_msg);
        } else {
            error_msg = "Failed to load index file: " + type_str + " : " + stat.message();
            s = Status(SERVER_UNEXPECTED_ERROR, error_msg);
        }

        if (auto job = job_.lock()) {
            auto search_job = std::static_pointer_cast<scheduler::SSSearchJob>(job);
            search_job->SearchDone(seg_id);
            search_job->status() = s;
        }

        return;
    }

    std::string info = "Search task load segment id: " + std::to_string(seg_id) + " " + type_str + " totally cost";
    rc.ElapseFromBegin(info);
}

void
XSSSearchTask::Execute() {
    auto seg_id = visitor_->GetSegment()->GetID();
    milvus::server::ContextFollower tracer(context_, "XSearchTask::Execute " + std::to_string(seg_id));
    TimeRecorder rc(LogOut("[%s][%ld] DoSearch file id:%ld", "search", 0, seg_id));

    engine::QueryResult result;
    double span;

    if (auto job = job_.lock()) {
        auto search_job = std::static_pointer_cast<scheduler::SSSearchJob>(job);

        if (engine_ == nullptr) {
            search_job->SearchDone(seg_id);
            return;
        }

        fiu_do_on("XSearchTask.Execute.throw_std_exception", throw std::exception());

        try {
            /* step 2: search */
            Status s = engine_->Search(search_job->query_ptr(), result);

            fiu_do_on("XSearchTask.Execute.search_fail", s = Status(SERVER_UNEXPECTED_ERROR, ""));
            if (!s.ok()) {
                search_job->SearchDone(seg_id);
                search_job->status() = s;
                return;
            }

            span = rc.RecordSection("search done");

            /* step 3: pick up topk result */
            // auto spec_k = file_->row_count_ < topk ? file_->row_count_ : topk;
            // if (spec_k == 0) {
            //     LOG_ENGINE_WARNING_ << LogOut("[%s][%ld] Searching in an empty file. file location = %s",
            //     "search", 0,
            //                                   file_->location_.c_str());
            // } else {
            //     std::unique_lock<std::mutex> lock(search_job->mutex());
            //   XSearchTask::MergeTopkToResultSet(result, spec_k, nq, topk, ascending_, search_job->GetQueryResult());
            // }

            span = rc.RecordSection("reduce topk done");
        } catch (std::exception& ex) {
            LOG_ENGINE_ERROR_ << LogOut("[%s][%ld] SearchTask encounter exception: %s", "search", 0, ex.what());
            search_job->status() = Status(SERVER_UNEXPECTED_ERROR, ex.what());
        }

        /* step 4: notify to send result to client */
        search_job->SearchDone(seg_id);
    }

    rc.ElapseFromBegin("totally cost");

    // release engine resource
    engine_ = nullptr;
}

void
XSSSearchTask::MergeTopkToResultSet(const engine::QueryResult& src_result, size_t src_k, size_t nq, size_t topk,
                                    bool ascending, engine::QueryResult& tar_result) {
    const engine::ResultIds& src_ids = src_result.result_ids_;
    const engine::ResultDistances& src_distances = src_result.result_distances_;
    engine::ResultIds& tar_ids = tar_result.result_ids_;
    engine::ResultDistances& tar_distances = tar_result.result_distances_;

    if (src_ids.empty()) {
        LOG_ENGINE_DEBUG_ << LogOut("[%s][%d] Search result is empty.", "search", 0);
        return;
    }

    size_t tar_k = tar_ids.size() / nq;
    size_t buf_k = std::min(topk, src_k + tar_k);

    scheduler::ResultIds buf_ids(nq * buf_k, -1);
    scheduler::ResultDistances buf_distances(nq * buf_k, 0.0);
    for (uint64_t i = 0; i < nq; i++) {
        size_t buf_k_j = 0, src_k_j = 0, tar_k_j = 0;
        size_t buf_idx, src_idx, tar_idx;

        size_t buf_k_multi_i = buf_k * i;
        size_t src_k_multi_i = topk * i;
        size_t tar_k_multi_i = tar_k * i;

        while (buf_k_j < buf_k && src_k_j < src_k && tar_k_j < tar_k) {
            src_idx = src_k_multi_i + src_k_j;
            tar_idx = tar_k_multi_i + tar_k_j;
            buf_idx = buf_k_multi_i + buf_k_j;

            if ((tar_ids[tar_idx] == -1) ||  // initialized value
                (ascending && src_distances[src_idx] < tar_distances[tar_idx]) ||
                (!ascending && src_distances[src_idx] > tar_distances[tar_idx])) {
                buf_ids[buf_idx] = src_ids[src_idx];
                buf_distances[buf_idx] = src_distances[src_idx];
                src_k_j++;
            } else {
                buf_ids[buf_idx] = tar_ids[tar_idx];
                buf_distances[buf_idx] = tar_distances[tar_idx];
                tar_k_j++;
            }
            buf_k_j++;
        }

        if (buf_k_j < buf_k) {
            if (src_k_j < src_k) {
                while (buf_k_j < buf_k && src_k_j < src_k) {
                    buf_idx = buf_k_multi_i + buf_k_j;
                    src_idx = src_k_multi_i + src_k_j;
                    buf_ids[buf_idx] = src_ids[src_idx];
                    buf_distances[buf_idx] = src_distances[src_idx];
                    src_k_j++;
                    buf_k_j++;
                }
            } else {
                while (buf_k_j < buf_k && tar_k_j < tar_k) {
                    buf_idx = buf_k_multi_i + buf_k_j;
                    tar_idx = tar_k_multi_i + tar_k_j;
                    buf_ids[buf_idx] = tar_ids[tar_idx];
                    buf_distances[buf_idx] = tar_distances[tar_idx];
                    tar_k_j++;
                    buf_k_j++;
                }
            }
        }
    }
    tar_ids.swap(buf_ids);
    tar_distances.swap(buf_distances);
}

}  // namespace scheduler
}  // namespace milvus

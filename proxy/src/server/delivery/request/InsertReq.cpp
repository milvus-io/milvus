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

#include "server/delivery/request/InsertReq.h"
#include "server/ValidationUtil.h"
#include "utils/CommonUtil.h"
#include "utils/Log.h"
#include "utils/TimeRecorder.h"
#include "server/delivery/ReqScheduler.h"
#include "server/MessageWrapper.h"
#include "server/MetaWrapper.h"

#include <memory>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>
#include <unistd.h>
#include "utils/CommonUtil.h"
#include "nlohmann/json.hpp"

#ifdef ENABLE_CPU_PROFILING
#include <gperftools/profiler.h>
#endif

namespace milvus {
namespace server {

InsertReq::InsertReq(const ContextPtr &context, const ::milvus::grpc::InsertParam *insert_param)
    : BaseReq(context, ReqType::kInsert),
      insert_param_(insert_param) {
}

BaseReqPtr
InsertReq::Create(const ContextPtr &context, const ::milvus::grpc::InsertParam *insert_param) {
  return std::shared_ptr<BaseReq>(new InsertReq(context, insert_param));
}

Status
InsertReq::OnExecute() {
#ifndef BENCHMARK
#define BENCHMARK
#endif

#ifdef BENCHMARK
  const uint64_t count_msg_num = 50000 * 10;
  const double MB = 1024 * 1024;
  using stdclock = std::chrono::high_resolution_clock;
  static uint64_t inserted_count, inserted_size = 0;
  static stdclock::time_point start, end;
  const int interval = 2;
  const int per_log_records = 10000 * 100;
  static uint64_t ready_log_records = 0;
  static int log_flag = 0;
  static bool shouldBenchmark = false;
  static std::stringstream log;
//  char buff[128];
//  auto r = getcwd(buff, 128);
  auto path = std::string("/tmp");
  std::ofstream file(path + "/proxy.benchmark", std::fstream::app);
#endif

  LOG_SERVER_INFO_ << LogOut("[%s][%ld] ", "insert", 0) << "Execute InsertReq.";
  auto &msg_client = MessageWrapper::GetInstance().MessageClient();
  auto segment_id = [](const std::string &collection_name,
                       uint64_t channel_id,
                       uint64_t timestamp) {
    return MetaWrapper::GetInstance().AskSegmentId(collection_name, channel_id, timestamp);
  };

#ifdef BENCHMARK
  if (inserted_count >= count_msg_num && !shouldBenchmark) {
    shouldBenchmark = true;
    start = stdclock::now();
    inserted_count = 0;
    inserted_size = 0;
  }
#endif

  Status status;
  status = msg_client->SendMutMessage(*insert_param_, timestamp_, segment_id);

#ifdef BENCHMARK
  inserted_count += insert_param_->rows_data_size();
  inserted_size += insert_param_->ByteSize();
  if (shouldBenchmark) {
    end = stdclock::now();
    ready_log_records += inserted_count;
    auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(end - start).count() / 1000.0;
    if (duration > interval) {
      nlohmann::json json;
      json["InsertTime"] = milvus::CommonUtil::TimeToString(start);
      json["DurationInMilliseconds"] = duration * 1000;
      json["SizeInMB"] =  inserted_size / MB;
      json["ThroughputInMB"] = double(inserted_size) / duration / MB;
      json["NumRecords"] = inserted_count;
      file << json.dump() << std::endl;
//      log << "[" << milvus::CommonUtil::TimeToString(start) << "] "
//          << "Insert "
//          << inserted_count << " records, "
//          << "size: " << inserted_size / MB << "MB, "
//          << "cost: " << duration << "s, "
//          << "throughput: "
//          << double(inserted_size) / duration / MB
//          << "M/s\n";
      auto new_flag = ready_log_records / per_log_records;
      if (new_flag != log_flag) {
        log_flag = new_flag;
        file << log.str();
        file.flush();
        log.str("");
      }
      inserted_size = 0;
      inserted_count = 0;
      start = stdclock::now();
    }
  }
#endif
  return status;
}

Status InsertReq::OnPostExecute() {
  ReqScheduler::GetInstance().UpdateLatestDeliveredReqTime(timestamp_);
  return Status::OK();
}

}  // namespace server
}  // namespace milvus

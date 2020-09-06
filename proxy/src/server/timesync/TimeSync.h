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

#pragma once

#include <functional>
#include <cstdint>
#include <thread>
#include <string>
#include "grpc/message.pb.h"

namespace milvus {
namespace timesync {

class TimeSync {
 public:
  TimeSync(int64_t id,
           std::function<uint64_t()> timestamp,
           const int interval,
           const std::string &pulsar_addr,
           const std::string &time_sync_topic);
  virtual ~TimeSync();

  void Stop();
  bool IsStop() const;
 private:
  std::function<int64_t()> timestamp_;
  const int interval_;
  const std::string pulsar_addr_;
  const std::string time_sync_topic_;
  bool stop_ = false;
  std::thread timer_;
  milvus::grpc::TimeSyncMsg sync_msg_;
};

} // namespace timesync
} // namespace milvus
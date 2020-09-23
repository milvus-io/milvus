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

#include <chrono>
#include "config/ServerConfig.h"
#include "TimeSync.h"
#include "message_client/Producer.h"

namespace milvus {
namespace timesync {

TimeSync::TimeSync(int64_t id,
                   std::function<uint64_t()> timestamp,
                   const int interval,
                   const std::string &pulsar_addr,
                   const std::string &time_sync_topic) :
    timestamp_(timestamp), interval_(interval), pulsar_addr_(pulsar_addr), time_sync_topic_(time_sync_topic) {
  sync_msg_.set_peer_id(id);
  auto timer = [&]() {
    std::shared_ptr<milvus::message_client::MsgClient>
        client = std::make_shared<milvus::message_client::MsgClient>(this->pulsar_addr_);
    milvus::message_client::MsgProducer producer(client, this->time_sync_topic_);

    for (;;) {
      if (this->stop_) break;
      this->sync_msg_.set_peer_id(config.proxy_id());
      this->sync_msg_.set_timestamp(this->timestamp_());
      this->sync_msg_.set_sync_type(milvus::grpc::READ);
      auto rst = producer.send(sync_msg_.SerializeAsString());
      if (rst != pulsar::ResultOk) {
        //TODO, add log
      }
      std::this_thread::sleep_for(std::chrono::milliseconds(this->interval_));
    }
    auto rst = producer.close();
    if (rst != pulsar::ResultOk) {
      //TODO, add log or throw exception
    }
    rst = client->close();
    if (rst != pulsar::ResultOk) {
      //TODO, add log or throw exception
    }
  };
  timer_ = std::thread(timer);
}

TimeSync::~TimeSync() {
  stop_ = true;
  timer_.join();
}

void TimeSync::Stop() {
  stop_ = true;
}

bool TimeSync::IsStop() const {
  return stop_;
}

} // namespace timesync
} // namespace milvus
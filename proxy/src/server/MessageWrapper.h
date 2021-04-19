#pragma once
#include "message_client/ClientV2.h"
#include "timesync/TimeSync.h"

namespace milvus {
namespace server {

class MessageWrapper {

 public:
  static MessageWrapper& GetInstance();

  Status Init();

  const std::shared_ptr<message_client::MsgClientV2>&
  MessageClient();

  void Stop();

 private:
  MessageWrapper() = default;

 private:
  std::shared_ptr<message_client::MsgClientV2> msg_client_;
  std::shared_ptr<milvus::timesync::TimeSync> time_sync_;
};

}
}
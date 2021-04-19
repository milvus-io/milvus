#pragma once
#include "message_client/ClientV2.h"

namespace milvus {
namespace server {

class MessageWrapper {

 public:
  static MessageWrapper& GetInstance();

  Status Init();

  const std::shared_ptr<message_client::MsgClientV2>&
  MessageClient();

 private:
  MessageWrapper() = default;

 private:
  std::shared_ptr<message_client::MsgClientV2> msg_client_;
};

}
}
#include "MessageWrapper.h"
#include "config/ServerConfig.h"

namespace milvus {
namespace server {

MessageWrapper &MessageWrapper::GetInstance() {
  static MessageWrapper wrapper;
  return wrapper;
}

Status MessageWrapper::Init() {
  std::string pulsar_server_addr
      (std::string{"pulsar://"} + config.pulsar.address() + ":" + std::to_string(config.pulsar.port()));
  int client_id = config.proxy_id();
  msg_client_ = std::make_shared<message_client::MsgClientV2>(client_id, pulsar_server_addr, config.pulsar.topicnum());
  auto status = msg_client_->Init("InsertOrDelete", "Search", "TimeSync", "SearchById", "SearchResult");
  return status;
}
const std::shared_ptr<message_client::MsgClientV2> &MessageWrapper::MessageClient() {
  return msg_client_;
}

}
}
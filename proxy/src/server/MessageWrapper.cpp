#include "server/delivery/ReqScheduler.h"
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

  Status status;
  if (config.pulsar.authentication) {
      std::string insert_or_delete_topic_name = "InsertOrDelete-" + config.pulsar.user.value;
      std::string search_topic_name = "Search-" + config.pulsar.user.value;
      std::string search_by_id_topic_name = "SearchById-" + config.pulsar.user.value;
      std::string search_result = "SearchResult-" + config.pulsar.user.value;
      status = msg_client_->Init(insert_or_delete_topic_name,
                                  search_topic_name,
                                  "TimeSync",
                                  search_by_id_topic_name,
                                  search_result);
  } else {
      status = msg_client_->Init("InsertOrDelete", "Search", "TimeSync", "SearchById", "SearchResult");
  }

  // timeSync
  time_sync_ = std::make_shared<timesync::TimeSync>(client_id, GetMessageTimeSyncTime, config.timesync.interval(), pulsar_server_addr, "TimeSync");

  if (!status.ok()){
    return status;
  }

  return status;
}
const std::shared_ptr<message_client::MsgClientV2> &MessageWrapper::MessageClient() {
  return msg_client_;
}

void MessageWrapper::Stop() {
  if (time_sync_ != nullptr){
    time_sync_->Stop();
    time_sync_= nullptr;
  }
  msg_client_ = nullptr;
}

}
}
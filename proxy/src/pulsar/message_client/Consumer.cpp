
#include "Consumer.h"
#include "pb/pulsar.pb.h"

namespace message_client {

MsgConsumer::MsgConsumer(std::shared_ptr<MsgClient> &client, std::string subscription_name, const ConsumerConfiguration conf)
        :client_(client), config_(conf), subscription_name_(subscription_name){}

Result MsgConsumer::subscribe(const std::string &topic) {
  return client_->subscribe(topic, subscription_name_, config_, consumer_);
}

Result MsgConsumer::subscribe(const std::vector<std::string> &topics) {
  return client_->subscribe(topics, subscription_name_, config_, consumer_);
}

Result MsgConsumer::unsubscribe() {
  return consumer_.unsubscribe();
}

Result MsgConsumer::receive(Message &msg) {
  return consumer_.receive(msg);
}

std::shared_ptr<void> MsgConsumer::receive_proto(ConsumerType consumer_type) {
  Message msg;
  receive(msg);
  acknowledge(msg);
  switch (consumer_type) {
    case INSERT: {
      pb::InsertMsg insert_msg;
      insert_msg.ParseFromString(msg.getDataAsString());
      auto message = std::make_shared<pb::InsertMsg>(insert_msg);
      return std::shared_ptr<void>(message);
    }
    case DELETE: {
      pb::DeleteMsg delete_msg;
      delete_msg.ParseFromString(msg.getDataAsString());
      auto message = std::make_shared<pb::DeleteMsg>(delete_msg);
      return std::shared_ptr<void>(message);
    }
    case SEARCH_RESULT: {
      pb::SearchResultMsg search_res_msg;
      search_res_msg.ParseFromString(msg.getDataAsString());
      auto message = std::make_shared<pb::SearchResultMsg>(search_res_msg);
      return std::shared_ptr<void>(message);
    }
    case TEST:
      pb::TestData test_msg;
      test_msg.ParseFromString(msg.getDataAsString());
      auto message = std::make_shared<pb::TestData>(test_msg);
      return std::shared_ptr<void>(message);
  }
  return nullptr;
}

Result MsgConsumer::close() {
  return consumer_.close();
}

Result MsgConsumer::acknowledge(const Message &message) {
  return consumer_.acknowledge(message);
}

}


#include "producer.h"

namespace message_client {

MsgProducer::MsgProducer(std::shared_ptr<MsgClient> &client, const std::string &topic, const ProducerConfiguration conf) : client_(client), config_(conf){
  createProducer(topic);
}

Result MsgProducer::createProducer(const std::string &topic) {
  return client_->createProducer(topic, producer_);
}

Result MsgProducer::send(const Message &msg) {
  return producer_.send(msg);
}

Result MsgProducer::send(const std::string &msg) {
  auto pulsar_msg = pulsar::MessageBuilder().setContent(msg).build();
  return send(pulsar_msg);
}

Result MsgProducer::close() {
  return producer_.close();
}

}
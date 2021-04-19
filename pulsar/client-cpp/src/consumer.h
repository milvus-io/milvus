#pragma once

#include "pulsar/Consumer.h"
#include "client.h"

namespace message_client {

enum ConsumerType {
  INSERT = 0,
  DELETE = 1,
  SEARCH_RESULT = 2,
  TEST = 3,
};

using Consumer = pulsar::Consumer;
using ConsumerConfiguration = pulsar::ConsumerConfiguration;

class MsgConsumer{
public:
  MsgConsumer(std::shared_ptr<message_client::MsgClient> &client, std::string consumer_name,
          const pulsar::ConsumerConfiguration conf = ConsumerConfiguration());

  Result subscribe(const std::string& topic);
  Result subscribe(const std::vector<std::string>& topics);
  Result unsubscribe();
  Result receive(Message& msg);
  std::shared_ptr<void> receive_proto(ConsumerType consumer_type);
  Result acknowledge(const Message& message);
  Result close();

  const Consumer&
  consumer() const {return consumer_; }

private:
  Consumer consumer_;
  std::shared_ptr<MsgClient> client_;
  ConsumerConfiguration config_;
  std::string subscription_name_;
};

}
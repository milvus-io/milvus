#pragma once

#include "pulsar/Consumer.h"
#include "Client.h"
#include "grpc/gen-milvus/suvlim.pb.h"

namespace milvus {
namespace message_client {

enum ConsumerType {
  INSERT = 0,
  DELETE = 1,
  SEARCH_RESULT = 2,
};

using Consumer = pulsar::Consumer;
using ConsumerConfiguration = pulsar::ConsumerConfiguration;

class MsgConsumer{
public:
  MsgConsumer(std::shared_ptr<MsgClient> &client, std::string consumer_name,
          const pulsar::ConsumerConfiguration conf = ConsumerConfiguration());

  Result subscribe(const std::string& topic);
  Result subscribe(const std::vector<std::string>& topics);
  Result unsubscribe();
  Result receive(Message& msg);
  Result receive(milvus::grpc::QueryResult &res);
  Result receive(milvus::grpc::EntityIds &res);
  Result receive(milvus::grpc::Entities &res);
  Result receive(milvus::grpc::Status &res);
  Result acknowledge(const Message& message);
  Result close();

  const Consumer&
  consumer() const {return consumer_; }

private:
  Consumer consumer_;
  std::shared_ptr<pulsar::Client> client_;
  ConsumerConfiguration config_;
  std::string subscription_name_;
};

}
}
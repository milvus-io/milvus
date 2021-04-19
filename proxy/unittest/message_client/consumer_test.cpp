#include <gtest/gtest.h>
#include "message_client/Consumer.h"
#include "grpc/message.pb.h"

TEST(CLIENT_CPP, CONSUMER) {
  auto client= std::make_shared<milvus::message_client::MsgClient>("pulsar://localhost:6650");
  milvus::message_client::MsgConsumer consumer(client, "my_consumer");
  consumer.subscribe("test");
  milvus::grpc::Status msg;
  auto res = consumer.receive(msg);
//  pb::TestData* data = (pb::TestData*)(msg.get());
  std::cout <<  "Received:  with payload  reason" <<  msg.reason();
  consumer.close();
  client->close();
}

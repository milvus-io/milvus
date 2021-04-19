#include <gtest/gtest.h>
#include "pulsar/message_client/Producer.h"
#include "pulsar/message_client/pb/pulsar.pb.h"

TEST(CLIENT_CPP, Producer) {
  auto client= std::make_shared<message_client::MsgClient>("pulsar://localhost:6650");
  message_client::MsgProducer producer(client,"test");
  pb::TestData data;
  data.set_id("test");
  data.set_name("hahah");
  std::string to_string = data.SerializeAsString();
  producer.send(to_string);
  producer.close();
  client->close();
}

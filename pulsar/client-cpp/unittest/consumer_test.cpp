#include <gtest/gtest.h>
#include "consumer.h"
#include "pb/pulsar.pb.h"

TEST(CLIENT_CPP, CONSUMER) {
  auto client= std::make_shared<message_client::MsgClient>("pulsar://localhost:6650");
  message_client::MsgConsumer consumer(client, "my_consumer");
  consumer.subscribe("test");
  auto msg = consumer.receive_proto(message_client::TEST);
  pb::TestData* data = (pb::TestData*)(msg.get());
  std::cout <<  "Received: " << msg << "  with payload '" <<  data->name()<< ";" << data->id();
  consumer.close();
  client->close();
}

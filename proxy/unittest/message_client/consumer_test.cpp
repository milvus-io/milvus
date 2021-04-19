#include <gtest/gtest.h>
#include "message_client/Consumer.h"
#include "message_client/Producer.h"
#include "grpc/message.pb.h"

TEST(CLIENT_CPP, CONSUMER) {
    auto client = std::make_shared<milvus::message_client::MsgClient>("pulsar://localhost:6650");
    milvus::message_client::MsgProducer producer(client, "test");
    milvus::grpc::Status msg;
    msg.set_error_code(::milvus::grpc::SUCCESS);
    msg.set_reason("no reason");
    std::string to_string = msg.SerializeAsString();
    producer.send(to_string);
    producer.close();

    milvus::message_client::MsgConsumer consumer(client, "my_consumer");
    consumer.subscribe("test");
    auto res = consumer.receive(msg);
//  pb::TestData* data = (pb::TestData*)(msg.get());
    std::cout << "Received:  with payload  reason" << msg.reason();
    consumer.close();
    client->close();
}

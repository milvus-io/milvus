#include <gtest/gtest.h>
#include "message_client/Producer.h"
#include "grpc/message.pb.h"

TEST(CLIENT_CPP, Producer) {
  auto client= std::make_shared<milvus::message_client::MsgClient>("pulsar://localhost:6650");
  milvus::message_client::MsgProducer producer(client,"test");
  milvus::grpc::Status msg;
  msg.set_error_code(::milvus::grpc::SUCCESS);
  msg.set_reason("no reason");
  std::string to_string = msg.SerializeAsString();
  producer.send(to_string);
  producer.close();
  client->close();
}

TEST(CLIENT_CPP, PRODUCE_INSERT) {
  auto client= std::make_shared<milvus::message_client::MsgClient>("pulsar://localhost:6650");
  milvus::message_client::MsgProducer producer(client,"InsertOrDelete");
  int64_t offset = 1;
  milvus::grpc::RowData data;
  milvus::grpc::InsertOrDeleteMsg msg;
  while (offset <= 1000) {
    data.set_blob("a blob");
    msg.set_collection_name("zilliz");
    msg.set_partition_tag("milvus");
    msg.set_segment_id(0);
    msg.set_channel_id(0);
    msg.set_client_id(0);
    msg.set_uid(offset);
    msg.set_timestamp(offset);
    msg.set_op(milvus::grpc::INSERT);

    std::string to_string = msg.SerializeAsString();
    producer.send(to_string);
//    if (offset % 20 == 0)
//      usleep(200000);
    offset++;
  }
//  producer.close();
  client->close();
}

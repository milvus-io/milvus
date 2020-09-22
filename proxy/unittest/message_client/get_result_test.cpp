#include <gtest/gtest.h>
#include "src/message_client/ClientV2.h"

TEST(CLIENT_CPP, GetResult) {
//    auto client= std::make_shared<milvus::message_client::MsgClient>("pulsar://localhost:6650");
//    milvus::message_client::MsgProducer producer(client,"test");
//    producer.createProducer("result");
//    producer.send("result");
//    milvus::grpc::QueryResult fake_message;
//    milvus::grpc::QueryResult fake_message2;
//
//    milvus::grpc::Status fake_status;
//    fake_status.set_error_code(milvus::grpc::ErrorCode::SUCCESS);
//    std::string aaa = "hahaha";
//    fake_status.set_reason(aaa);
//
//    milvus::grpc::RowData fake_row_data;
//    fake_row_data.set_blob("fake_row_data");
//
//    milvus::grpc::Entities fake_entities;
////    fake_entities.set_allocated_status(&fake_status);
//    fake_entities.mutable_status()->CopyFrom(fake_status);
//    for (int i = 0; i < 10; i++){
//        fake_entities.add_ids(i);
//        fake_entities.add_valid_row(true);
//        fake_entities.add_rows_data()->CopyFrom(fake_row_data);
//    }
//
//    int64_t fake_row_num = 10;
//
//    float fake_scores[10] = {1.1, 2.2, 3.3, 4.4, 5.5, 6.6, 7.7, 8.8, 9.9, 0.0};
//    float fake_distance[10] = {9.7, 9.6, 9.5, 9.8, 8.7, 8.8, 9.9, 8.8, 9.7, 8.9};
//
//    std::vector<milvus::grpc::KeyValuePair> fake_extra_params;
//    milvus::grpc::KeyValuePair keyValuePair;
//    for (int i = 0; i < 10; ++i) {
//        keyValuePair.set_key(std::to_string(i));
//        keyValuePair.set_value(std::to_string(i + 10));
//        fake_extra_params.push_back(keyValuePair);
//    }
//
//    int64_t fake_query_id = 10;
//    int64_t fake_client_id = 1;
//
//    fake_message.mutable_status()->CopyFrom(fake_status);
//    fake_message.mutable_entities()->CopyFrom(fake_entities);
//    fake_message.set_row_num(fake_row_num);
//    for (int i = 0; i < 10; i++) {
//        fake_message.add_scores(fake_scores[i]);
//        fake_message.add_distances(fake_distance[i]);
//        fake_message.add_extra_params()->CopyFrom(fake_extra_params[i]);
//    }
//
//    fake_message.set_query_id(fake_query_id);
//    fake_message.set_client_id(fake_client_id);
//
//    float fake_scores2[10] = {2.2, 3.3, 4.4, 5.5, 6.6, 7.7, 8.8, 9.9, 0.0, 1.1};
//    float fake_distance2[10] = {9.8, 8.6, 9.6, 9.7, 8.9, 8.8, 9.0, 9.8, 9.7, 8.9};
//
//    fake_message2.mutable_status()->CopyFrom(fake_status);
//    fake_message2.mutable_entities()->CopyFrom(fake_entities);
//    fake_message2.set_row_num(fake_row_num);
//    for (int j = 0; j < 10; ++j) {
//        fake_message2.add_scores(fake_scores2[j]);
//        fake_message2.add_distances(fake_distance2[j]);
//        fake_message2.add_extra_params()->CopyFrom(fake_extra_params[j]);
//    }
//
//    fake_message2.set_query_id(fake_query_id);
//    fake_message2.set_client_id(fake_client_id);
//
//    producer.send(fake_message.SerializeAsString());
//    producer.send(fake_message2.SerializeAsString());

    int64_t query_id = 10;
    milvus::message_client::MsgClientV2 client_v2(1, "pulsar://localhost:6650");
    auto init_status = client_v2.Init("insert_delete", "search", "time_sync", "result", "result");

//    client_v2.SendQueryMessage();
    milvus::grpc::SearchParam request;
    auto status_send = client_v2.SendQueryMessage(request, 10, query_id);

    milvus::grpc::QueryResult result;
    auto status = client_v2.GetQueryResult(query_id, &result);

    std::cout << result.client_id() << std::endl;
    for (int k = 0; k < result.distances_size(); ++k) {
        std::cout << result.distances(k) << "\t";
    }
    std::cout << "hahah" << std::endl;
}


#include "ClientV2.h"
#include "pulsar/Result.h"
#include "PartitionPolicy.h"
#include "utils/CommonUtil.h"
#include <omp.h>

namespace milvus::message_client {

std::map<int64_t, std::vector<std::shared_ptr<grpc::QueryResult>>> total_results;

MsgClientV2::MsgClientV2(int64_t client_id, const std::string &service_url, const uint32_t mut_parallelism, const pulsar::ClientConfiguration &config)
    : client_id_(client_id), service_url_(service_url), mut_parallelism_(mut_parallelism) {}

Status MsgClientV2::Init(const std::string &insert_delete,
                         const std::string &search,
                         const std::string &time_sync,
                         const std::string &search_by_id,
                         const std::string &search_result) {
  //create pulsar client
  auto pulsar_client = std::make_shared<MsgClient>(service_url_);
  //create pulsar producer
  ProducerConfiguration producerConfiguration;
  producerConfiguration.setPartitionsRoutingMode(ProducerConfiguration::CustomPartition);
  producerConfiguration.setMessageRouter(std::make_shared<PartitionPolicy>());
  // insert_delete_producer_ = std::make_shared<MsgProducer>(pulsar_client, insert_delete, producerConfiguration);
  search_producer_ = std::make_shared<MsgProducer>(pulsar_client, search, producerConfiguration);
  search_by_id_producer_ = std::make_shared<MsgProducer>(pulsar_client, search_by_id, producerConfiguration);
  time_sync_producer_ = std::make_shared<MsgProducer>(pulsar_client, time_sync);

  for (auto i = 0; i < mut_parallelism_; i++) {
    paralle_mut_producers_.emplace_back(std::make_shared<MsgProducer>(pulsar_client,
                                                                      insert_delete,
                                                                      producerConfiguration));
  }
  //create pulsar consumer
  std::string subscribe_name = std::to_string(CommonUtil::RandomUINT64());
  consumer_ = std::make_shared<MsgConsumer>(pulsar_client, search_result + subscribe_name);

  auto result = consumer_->subscribe(search_result);
  if (result != pulsar::Result::ResultOk) {
    return Status(SERVER_UNEXPECTED_ERROR,
                  "Pulsar message client init occur error, " + std::string(pulsar::strResult(result)));
  }
  return Status::OK();
}

int64_t GetQueryNodeNum() {
    return 1;
}

milvus::grpc::QueryResult Aggregation(std::vector<std::shared_ptr<grpc::QueryResult>> &results) {
  //TODO: QueryNode has only one
  int64_t length = results.size();
  std::vector<float> all_scores;
  std::vector<float> all_distance;
  std::vector<grpc::KeyValuePair> all_kv_pairs;
  std::vector<int> index(length * results[0]->distances_size());

  for (int n = 0; n < length * results[0]->distances_size(); ++n) {
    index[n] = n;
  }

  for (int i = 0; i < length; i++) {
    for (int j = 0; j < results[i]->distances_size(); j++) {
//      all_scores.push_back(results[i]->scores()[j]);
      all_distance.push_back(results[i]->distances()[j]);
//      all_kv_pairs.push_back(results[i]->extra_params()[j]);
    }
  }

  for (int k = 0; k < all_distance.size() - 1; ++k) {
    for (int l = k + 1; l < all_distance.size(); ++l) {

      if (all_distance[l] > all_distance[k]) {
        float distance_temp = all_distance[k];
        all_distance[k] = all_distance[l];
        all_distance[l] = distance_temp;

        int index_temp = index[k];
        index[k] = index[l];
        index[l] = index_temp;
      }
    }
  }

    grpc::QueryResult result;

    result.mutable_status()->CopyFrom(results[0]->status());
    result.mutable_entities()->CopyFrom(results[0]->entities());
    result.set_row_num(results[0]->row_num());

    for (int m = 0; m < results[0]->distances_size(); ++m) {
//        result.add_scores(all_scores[index[m]]);
        result.add_distances(all_distance[m]);
//        result.add_extra_params();
//        result.mutable_extra_params(m)->CopyFrom(all_kv_pairs[index[m]]);
    }

//  result.set_query_id(results[0]->query_id());
//  result.set_client_id(results[0]->client_id());

  return result;
}

Status MsgClientV2::GetQueryResult(int64_t query_id, milvus::grpc::QueryResult* result) {

    int64_t query_node_num = GetQueryNodeNum();

    while (true) {
        auto received_result = total_results[query_id];
        if (received_result.size() == query_node_num) {
            break;
        }
        Message msg;
        consumer_->receive(msg);

        grpc::QueryResult search_res_msg;
        auto status = search_res_msg.ParseFromString(msg.getDataAsString());
        if (status) {
            auto message = std::make_shared<grpc::QueryResult>(search_res_msg);
            total_results[message->query_id()].push_back(message);
            consumer_->acknowledge(msg);
        } else {
            return Status(DB_ERROR, "can't parse message which from pulsar!");
        }
    }
    *result = Aggregation(total_results[query_id]);
    return Status::OK();
}

Status MsgClientV2::SendMutMessage(const milvus::grpc::InsertParam &request, uint64_t timestamp) {
  // may have retry policy?
  auto row_count = request.rows_data_size();
  // TODO: Get the segment from master
  int64_t segment = 0;
  auto stats = std::vector<pulsar::Result>(ParallelNum);

#pragma omp parallel for default(none), shared(row_count, request, timestamp, segment, stats), num_threads(ParallelNum)
  for (auto i = 0; i < row_count; i++) {
    milvus::grpc::InsertOrDeleteMsg mut_msg;
    int this_thread = omp_get_thread_num();
    mut_msg.set_op(milvus::grpc::OpType::INSERT);
    mut_msg.set_uid(request.entity_id_array(i));
    mut_msg.set_client_id(client_id_);
    mut_msg.set_timestamp(timestamp);
    mut_msg.set_collection_name(request.collection_name());
    mut_msg.set_partition_tag(request.partition_tag());
    mut_msg.set_segment_id(segment);
    mut_msg.mutable_rows_data()->CopyFrom(request.rows_data(i));
    mut_msg.mutable_extra_params()->CopyFrom(request.extra_params());

    auto result = paralle_mut_producers_[this_thread]->send(mut_msg);
    if (result != pulsar::ResultOk) {
      stats[this_thread] = result;
    }
  }
  for (auto &stat : stats) {
    if (stat == pulsar::ResultOk) {
      return Status(DB_ERROR, pulsar::strResult(stat));
    }
  }
  return Status::OK();
}

Status MsgClientV2::SendMutMessage(const milvus::grpc::DeleteByIDParam &request, uint64_t timestamp) {
  auto stats = std::vector<pulsar::Result>(ParallelNum);
#pragma omp parallel for default(none), shared( request, timestamp, stats), num_threads(ParallelNum)
  for (auto i = 0; i < request.id_array_size(); i++) {
    milvus::grpc::InsertOrDeleteMsg mut_msg;
    mut_msg.set_op(milvus::grpc::OpType::DELETE);
    mut_msg.set_uid(GetUniqueQId());
    mut_msg.set_client_id(client_id_);
    mut_msg.set_uid(request.id_array(i));
    mut_msg.set_collection_name(request.collection_name());
    mut_msg.set_timestamp(timestamp);

    int this_thread = omp_get_thread_num();
    auto result = paralle_mut_producers_[this_thread]->send(mut_msg);
    if (result != pulsar::ResultOk) {
      stats[this_thread] = result;
    }
  }
  for (auto &stat : stats) {
    if (stat == pulsar::ResultOk) {
      return Status(DB_ERROR, pulsar::strResult(stat));
    }
  }
  return Status::OK();
}

Status MsgClientV2::SendQueryMessage(const milvus::grpc::SearchParam &request, uint64_t timestamp, int64_t &query_id) {
    milvus::grpc::SearchMsg search_msg;

    query_id = GetUniqueQId();
    search_msg.set_collection_name(request.collection_name());
    search_msg.set_uid(query_id);
    //TODO: get client id from master
    search_msg.set_client_id(1);
    search_msg.set_timestamp(timestamp);
    search_msg.set_dsl(request.dsl());

    //TODO: get data type from master
    milvus::grpc::VectorRowRecord vector_row_recode;
    std::vector<float> vectors_records;
    for (int i = 0; i < request.vector_param_size(); ++i) {
        search_msg.add_json(request.vector_param(i).json());
        for (int j = 0; j < request.vector_param(i).row_record().records_size(); ++j) {
            for (int k = 0; k < request.vector_param(i).row_record().records(j).float_data_size(); ++k) {
                vector_row_recode.add_float_data(request.vector_param(i).row_record().records(j).float_data(k));
            }
            vector_row_recode.set_binary_data(request.vector_param(i).row_record().records(j).binary_data());
        }
    }

    search_msg.mutable_records()->CopyFrom(vector_row_recode);

    for (int m = 0; m < request.partition_tag_size(); ++m) {
        search_msg.add_partition_tag(request.partition_tag(m));
    }

    for (int l = 0; l < request.extra_params_size(); ++l) {
        search_msg.mutable_extra_params(l)->CopyFrom(request.extra_params(l));
    }

    std::cout << search_msg.collection_name() << std::endl;
    auto result = search_producer_->send(search_msg);
    if (result != pulsar::Result::ResultOk) {
        return Status(DB_ERROR, pulsar::strResult(result));
    }

    return Status::OK();

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
//    search_by_id_producer_->send(fake_message.SerializeAsString());
//    search_by_id_producer_->send(fake_message2.SerializeAsString());
//    return Status::OK();
}

MsgClientV2::~MsgClientV2() {
  // insert_delete_producer_->close();
  for (auto& producer: paralle_mut_producers_){
    producer->close();
  }
  search_producer_->close();
  search_by_id_producer_->close();
  time_sync_producer_->close();
  consumer_->close();
}
}
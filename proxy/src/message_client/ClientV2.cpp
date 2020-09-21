#include "ClientV2.h"
#include "pulsar/Result.h"
#include "PartitionPolicy.h"
#include "utils/CommonUtil.h"
#include "config/ServerConfig.h"
#include <omp.h>
#include <numeric>
#include <algorithm>

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
    return config.query_node_num();
}

Status
Aggregation(std::vector<std::shared_ptr<grpc::QueryResult>> results, milvus::grpc::QueryResult* result) {
  if (results.empty()) {
      return  Status(DB_ERROR, "The result is null!");
  }

  std::vector<float> all_scores;
  std::vector<float> all_distance;
  std::vector<int64_t> all_entities_ids;
  std::vector<bool> all_valid_row;
  std::vector<grpc::RowData> all_row_data;
  std::vector<grpc::KeyValuePair> all_kv_pairs;

    grpc::Status status;
    int row_num = 0;

  for (auto & result_per_node : results) {
      if (result_per_node->status().error_code() != grpc::ErrorCode::SUCCESS){
//    if (one_node_res->status().error_code() != grpc::ErrorCode::SUCCESS ||
//        one_node_res->entities().status().error_code() != grpc::ErrorCode::SUCCESS) {
          return Status(DB_ERROR, "QueryNode return wrong status!");
      }
      for (int j = 0; j < result_per_node->distances_size(); j++) {
          all_scores.push_back(result_per_node->scores()[j]);
          all_distance.push_back(result_per_node->distances()[j]);
//          all_kv_pairs.push_back(result_per_node->extra_params()[j]);
      }
      for (int k = 0; k < result_per_node->entities().ids_size(); ++k) {
          all_entities_ids.push_back(result_per_node->entities().ids(k));
//          all_valid_row.push_back(result_per_node->entities().valid_row(k));
//          all_row_data.push_back(result_per_node->entities().rows_data(k));
      }
      if (result_per_node->row_num() > row_num){
          row_num = result_per_node->row_num();
      }
      status = result_per_node->status();
  }

    std::vector<int> index(all_distance.size());

    iota(index.begin(), index.end(), 0);

    std::stable_sort(index.begin(), index.end(),
                     [&all_distance](size_t i1, size_t i2) {return all_distance[i1] > all_distance[i2];});

    grpc::Entities result_entities;

    for (int m = 0; m < result->row_num(); ++m) {
        result->add_scores(all_scores[index[m]]);
        result->add_distances(all_distance[index[m]]);
//        result->add_extra_params();
//        result->mutable_extra_params(m)->CopyFrom(all_kv_pairs[index[m]]);

        result_entities.add_ids(all_entities_ids[index[m]]);
//        result_entities.add_valid_row(all_valid_row[index[m]]);
//        result_entities.add_rows_data();
//        result_entities.mutable_rows_data(m)->CopyFrom(all_row_data[index[m]]);
    }

    result_entities.mutable_status()->CopyFrom(status);

    result->set_row_num(row_num);
    result->mutable_entities()->CopyFrom(result_entities);
    result->set_query_id(results[0]->query_id());
//  result->set_client_id(results[0]->client_id());

  return Status::OK();
}

Status MsgClientV2::GetQueryResult(int64_t query_id, milvus::grpc::QueryResult* result) {

    int64_t query_node_num = GetQueryNodeNum();

    auto t1 = std::chrono::high_resolution_clock::now();

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
    auto status = Aggregation(total_results[query_id], result);

    return status;
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
    if (stat != pulsar::ResultOk) {
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
    if (stat != pulsar::ResultOk) {
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

    milvus::grpc::VectorRowRecord vector_row_recode;
    std::vector<float> vectors_records;
    std::string binary_data;
    for (int i = 0; i < request.vector_param_size(); ++i) {
        search_msg.add_json(request.vector_param(i).json());
        for (int j = 0; j < request.vector_param(i).row_record().records_size(); ++j) {
            for (int k = 0; k < request.vector_param(i).row_record().records(j).float_data_size(); ++k) {
                vector_row_recode.add_float_data(request.vector_param(i).row_record().records(j).float_data(k));
            }
            binary_data.append(request.vector_param(i).row_record().records(j).binary_data());
        }
    }
    vector_row_recode.set_binary_data(binary_data);

    search_msg.mutable_records()->CopyFrom(vector_row_recode);

    for (int m = 0; m < request.partition_tag_size(); ++m) {
        search_msg.add_partition_tag(request.partition_tag(m));
    }

    for (int l = 0; l < request.extra_params_size(); ++l) {
        search_msg.mutable_extra_params(l)->CopyFrom(request.extra_params(l));
    }

    auto result = search_producer_->send(search_msg);
    if (result != pulsar::Result::ResultOk) {
        return Status(DB_ERROR, pulsar::strResult(result));
    }

    return Status::OK();
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
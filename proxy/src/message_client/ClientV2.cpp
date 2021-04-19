#include "ClientV2.h"
#include "pulsar/Result.h"
#include "PartitionPolicy.h"
#include "utils/CommonUtil.h"
#include "config/ServerConfig.h"


namespace milvus::message_client {

MsgClientV2 &MsgClientV2::GetInstance() {
  // TODO: do not hardcode pulsar message configure and init
  std::string pulsar_server_addr(std::string {"pulsar://"} + config.pulsar.address() + ":" + std::to_string(config.pulsar.port()));
// "pulsar://localhost:6650"

  int64_t client_id = 0;
  static MsgClientV2 msg_client(client_id, pulsar_server_addr);
  return msg_client;
}

MsgClientV2::MsgClientV2(int64_t client_id, std::string &service_url, const pulsar::ClientConfiguration &config)
    : client_id_(client_id), service_url_(service_url) {}

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
  insert_delete_producer_ = std::make_shared<MsgProducer>(pulsar_client, insert_delete, producerConfiguration);
  search_producer_ = std::make_shared<MsgProducer>(pulsar_client, search, producerConfiguration);
  search_by_id_producer_ = std::make_shared<MsgProducer>(pulsar_client, search_by_id, producerConfiguration);
  time_sync_producer_ = std::make_shared<MsgProducer>(pulsar_client, time_sync);
  //create pulsar consumer
  std::string subscribe_name = std::to_string(CommonUtil::RandomUINT64());
  consumer_ = std::make_shared<MsgConsumer>(pulsar_client, search_result+subscribe_name);

  auto result = consumer_->subscribe(search_result);
  if (result != pulsar::Result::ResultOk) {
    return Status(SERVER_UNEXPECTED_ERROR, "Pulsar message client init occur error, " + std::string(pulsar::strResult(result)));
  }
  return Status::OK();
}

int64_t GetQueryNodeNum() {
    return 2;
}

milvus::grpc::QueryResult Aggregation(std::vector<std::shared_ptr<grpc::QueryResult>> &results){
    //TODO: QueryNode has only one
    int64_t length = results.size();
    std::vector<float> all_scores;
    std::vector<float> all_distance;
    std::vector<grpc::KeyValuePair> all_kv_pairs;
    std::vector<int> index(length * results[0]->scores_size());

    for (int n = 0; n < length * results[0]->scores_size(); ++n) {
        index[n] = n;
    }

    for (int i = 0; i < length; i++){
        for (int j = 0; j < results[i]->scores_size(); j++){
            all_scores.push_back(results[i]->scores()[j]);
            all_distance.push_back(results[i]->distances()[j]);
            all_kv_pairs.push_back(results[i]->extra_params()[j]);
        }
    }

    for (int k = 0; k < all_distance.size() - 1; ++k) {
        for (int l = k + 1; l < all_distance.size(); ++l) {

            if (all_distance[l] > all_distance[k]){
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
//    result_prt->set_allocated_status(const_cast<milvus::grpc::Status*>(&results[0]->status()));
    result.mutable_status()->CopyFrom(results[0]->status());
    result.mutable_entities()->CopyFrom(results[0]->entities());
    result.set_row_num(results[0]->row_num());

    for (int m = 0; m < results[0]->scores_size(); ++m) {
        result.add_scores(all_scores[index[m]]);
        result.add_distances(all_distance[m]);
        result.add_extra_params();
        result.mutable_extra_params(m)->CopyFrom(all_kv_pairs[index[m]]);
    }

    result.set_query_id(results[0]->query_id());
    result.set_client_id(results[0]->client_id());

    return result;
}

milvus::grpc::QueryResult MsgClientV2::GetQueryResult(int64_t query_id) {
//    Result result = MsgProducer::createProducer("result-partition-0");
    auto client = std::make_shared<MsgClient>("pulsar://localhost:6650");
    MsgConsumer consumer(client, "my_consumer");
    consumer.subscribe("result");

    std::vector<std::shared_ptr<grpc::QueryResult>> results;

    int64_t query_node_num = GetQueryNodeNum();

    std::map<int64_t, std::vector<std::shared_ptr<grpc::QueryResult>>> total_results;

    while (true) {
        auto received_result = total_results[query_id];
        if (received_result.size() == query_node_num) {
            break;
        }
        Message msg;
        consumer.receive(msg);

        grpc::QueryResult search_res_msg;
        auto status = search_res_msg.ParseFromString(msg.getDataAsString());
        if (status) {
            auto message = std::make_shared<grpc::QueryResult>(search_res_msg);
            total_results[message->query_id()].push_back(message);
            consumer.acknowledge(msg);
        }
    }

    return Aggregation(total_results[query_id]);
}

Status MsgClientV2::SendMutMessage(const milvus::grpc::InsertParam &request, uint64_t timestamp) {
  // may have retry policy?
  auto row_count = request.rows_data_size();
  // TODO: Get the segment from master
  int64_t segment = 0;
  milvus::grpc::InsertOrDeleteMsg mut_msg;
  for (auto i = 0; i < row_count; i++) {
    mut_msg.set_op(milvus::grpc::OpType::INSERT);
    mut_msg.set_uid(GetUniqueQId());
    mut_msg.set_client_id(client_id_);
    // TODO: add channel id
    auto channel_id = 0;
    mut_msg.set_channel_id(channel_id);
    mut_msg.set_timestamp(timestamp);
    mut_msg.set_collection_name(request.collection_name());
    mut_msg.set_partition_tag(request.partition_tag());
    mut_msg.set_segment_id(segment);
    mut_msg.mutable_rows_data()->CopyFrom(request.rows_data(i));
    mut_msg.mutable_extra_params()->CopyFrom(request.extra_params());

    auto result = insert_delete_producer_->send(mut_msg);
    if (result != pulsar::ResultOk) {
      // TODO: error code
      return Status(DB_ERROR, pulsar::strResult(result));
    }
  }
  return Status::OK();
}

Status MsgClientV2::SendMutMessage(const milvus::grpc::DeleteByIDParam &request, uint64_t timestamp) {
  milvus::grpc::InsertOrDeleteMsg mut_msg;
  for (auto id: request.id_array()) {
    mut_msg.set_op(milvus::grpc::OpType::DELETE);
    mut_msg.set_uid(GetUniqueQId());
    mut_msg.set_client_id(client_id_);
    mut_msg.set_uid(id);
    mut_msg.set_collection_name(request.collection_name());
    mut_msg.set_timestamp(timestamp);

    auto result = insert_delete_producer_->send(mut_msg);
    if (result != pulsar::ResultOk) {
      // TODO: error code
      return Status(DB_ERROR, pulsar::strResult(result));
    }
  }
  return Status::OK();
}

MsgClientV2::~MsgClientV2() {
  insert_delete_producer_->close();
  search_producer_->close();
  search_by_id_producer_->close();
  time_sync_producer_->close();
  consumer_->close();
}
}
#include "ClientV2.h"
#include "pulsar/Result.h"
#include "PartitionPolicy.h"
#include "utils/CommonUtil.h"
#include "config/ServerConfig.h"

namespace {
int64_t gen_channe_id(int64_t uid) {
  // TODO: murmur3 hash from pulsar source code
  return 0;
}
}

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

void MsgClientV2::GetQueryResult(int64_t query_id) {
  throw std::exception();
}

Status MsgClientV2::SendMutMessage(const milvus::grpc::InsertParam &request) {
  // may have retry policy?
  auto row_count = request.rows_data_size();
  // TODO: Get the segment from master
  int64_t segment = 0;
  milvus::grpc::InsertOrDeleteMsg mut_msg;
  for (auto i = 0; i < row_count; i++) {
    mut_msg.set_op(milvus::grpc::OpType::INSERT);
    mut_msg.set_uid(GetUniqueQId());
    mut_msg.set_client_id(client_id_);
    auto channel_id = gen_channe_id(request.entity_id_array(i));
    mut_msg.set_channel_id(channel_id);
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

Status MsgClientV2::SendMutMessage(const milvus::grpc::DeleteByIDParam &request) {
  milvus::grpc::InsertOrDeleteMsg mut_msg;
  for (auto id: request.id_array()) {
    mut_msg.set_op(milvus::grpc::OpType::DELETE);
    mut_msg.set_uid(GetUniqueQId());
    mut_msg.set_client_id(client_id_);
    mut_msg.set_uid(id);
    mut_msg.set_collection_name(request.collection_name());

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
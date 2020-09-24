#include "MetaWrapper.h"
#include "config/ServerConfig.h"
#include "nlohmann/json.hpp"
#include <mutex>
#include <google/protobuf/text_format.h>
#include <boost/filesystem.hpp>

using Collection = masterpb::Collection;
using Schema = milvus::grpc::Schema;
using SegmentInfo = masterpb::Segment;
using JSON = nlohmann::json;

namespace milvus {
namespace server {

namespace {
void ParseSegmentInfo(const std::string &json_str, SegmentInfo &segment_info) {
  auto json = JSON::parse(json_str);
  segment_info.set_segment_id(json["segment_id"].get<uint64_t>());
  segment_info.set_partition_tag(json["partition_tag"].get<std::string>());
  segment_info.set_channel_start(json["channel_start"].get<int32_t>());
  segment_info.set_channel_end(json["channel_end"].get<int32_t>());
  segment_info.set_open_timestamp(json["open_timestamp"].get<uint64_t>());
  segment_info.set_close_timestamp(json["close_timestamp"].get<uint64_t>());
  segment_info.set_collection_id(json["collection_id"].get<uint64_t>());
  segment_info.set_collection_name(json["collection_name"].get<std::string>());
  segment_info.set_rows(json["rows"].get<std::int64_t>());
}

void ParseCollectionSchema(const std::string &json_str, Collection &collection) {
  auto json = JSON::parse(json_str);
  auto proto_str = json["grpc_marshal_string"].get<std::string>();
  auto suc = google::protobuf::TextFormat::ParseFromString(proto_str, &collection);
  if (!suc) {
    std::cerr << "unmarshal failed" << std::endl;
  }
}
}

bool MetaWrapper::IsCollectionMetaKey(const std::string &key) {
  return key.rfind(collection_path_, 0) == 0;
}

bool MetaWrapper::IsSegmentMetaKey(const std::string &key) {
  return key.rfind(segment_path_, 0) == 0;
}

MetaWrapper &MetaWrapper::GetInstance() {
  static MetaWrapper wrapper;
  return wrapper;
}

Status MetaWrapper::Init() {
  try {
    etcd_root_path_ = config.etcd.rootpath() + "/";
    segment_path_ = (boost::filesystem::path(etcd_root_path_) / "segment/").string();
    collection_path_ = (boost::filesystem::path(etcd_root_path_) / "collection/").string();

    auto master_addr = config.master.address() + ":" + std::to_string(config.master.port());
    master_client_ = std::make_shared<milvus::master::GrpcClient>(master_addr);

    auto etcd_addr = config.etcd.address() + ":" + std::to_string(config.etcd.port());
    etcd_client_ = std::make_shared<milvus::master::EtcdClient>(etcd_addr);

    // init etcd watcher
    auto f = [&](const etcdserverpb::WatchResponse &res) {
        UpdateMeta(res);
    };
    watcher_ = std::make_shared<milvus::master::Watcher>(etcd_addr, segment_path_, f, true);
    return SyncMeta();
  }
  catch (const std::exception &e) {
    return Status(DB_ERROR, "Can not connect to meta server");
  }
}

std::shared_ptr<milvus::master::GrpcClient> MetaWrapper::MetaClient() {
  return master_client_;
}

void MetaWrapper::UpdateMeta(const etcdserverpb::WatchResponse &res) {
  for (auto &event: res.events()) {
    auto &event_key = event.kv().key();
    auto &event_value = event.kv().value();

    if (event.type() == etcdserverpb::Event_EventType::Event_EventType_PUT) {
      if (event_key.rfind(segment_path_, 0) == 0) {
        // segment info
        SegmentInfo segment_info;
        ParseSegmentInfo(event_value, segment_info);
        std::unique_lock lock(mutex_);
        segment_infos_[segment_info.segment_id()] = segment_info;
        lock.unlock();
      } else {
        // table scheme
        Collection collection;
        ParseCollectionSchema(event_value, collection);
        std::unique_lock lock(mutex_);
        schemas_[collection.name()] = collection;
        lock.unlock();
      }
    }
    // TODO: Delete event type
  }
}

uint64_t MetaWrapper::AskSegmentId(const std::string &collection_name, uint64_t channel_id, uint64_t timestamp) {
  // TODO: may using some multi index data structure to speed up search
  // index timestamp: no-unique, seems close timestamp is enough
  // index collection_name: no-unique
  // index channel_id: must satisfy channel_start <= channel_id < channel_end
  std::shared_lock lock(mutex_);
  for (auto &item: segment_infos_) {
    auto &segment_info = item.second;
    uint64_t open_ts = segment_info.open_timestamp();
    uint64_t close_ts = segment_info.close_timestamp();
    if (channel_id >= segment_info.channel_start() && channel_id < segment_info.channel_end()
        && timestamp >= (open_ts << 18) && timestamp < (close_ts << 18)
        && std::string(segment_info.collection_name()) == collection_name) {
      return segment_info.segment_id();
    }
  }
  throw std::runtime_error("Can't find eligible segment");
}

const Schema &MetaWrapper::AskCollectionSchema(const std::string &collection_name) {
  std::shared_lock lock(mutex_);
  if (schemas_.find(collection_name) != schemas_.end()) {
    return schemas_[collection_name].schema();
  }
  throw std::runtime_error("Collection " + collection_name + " not existed");
}

Status MetaWrapper::SyncMeta() {
  ::etcdserverpb::RangeRequest request;
  request.set_key(etcd_root_path_);
  std::string range_end(etcd_root_path_);
  int ascii = (int) range_end[range_end.length() - 1];
  range_end.back() = ascii + 1;
  request.set_range_end(range_end);

  ::etcdserverpb::RangeResponse response;
  auto status = etcd_client_->Range(request, response);
  if (status.ok()) {
    for (auto &kv : response.kvs()) {
      if (IsCollectionMetaKey(kv.key())) {
        Collection collection;
        ParseCollectionSchema(kv.value(), collection);
        std::unique_lock lock(mutex_);
        schemas_[collection.name()] = collection;
        lock.unlock();
      } else {
        assert(IsSegmentMetaKey(kv.key()));
        SegmentInfo segment_info;
        ParseSegmentInfo(kv.value(), segment_info);
        std::unique_lock lock(mutex_);
        segment_infos_[segment_info.segment_id()] = segment_info;
        lock.unlock();
      }
    }
  }
  return status;
}

int64_t MetaWrapper::CountCollection(const std::string &collection_name) {
  uint64_t count = 0;
  // TODO: index to speed up
  for (const auto &segment_info : segment_infos_) {
    if (segment_info.second.collection_name() == collection_name) {
      count += segment_info.second.rows();
    }
  }
  return count;
}

void MetaWrapper::Stop() {
  watcher_->Cancel();
}

}
}
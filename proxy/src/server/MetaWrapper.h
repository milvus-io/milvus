#include "utils/Status.h"
#include "meta/master/GrpcClient.h"
#include "grpc/message.pb.h"
#include "grpc/master.pb.h"
#include "meta/etcd_watcher/Watcher.h"
#include "meta/etcd_client/Etcd_client.h"
#include "config/ServerConfig.h"

#include <shared_mutex>

namespace milvus {
namespace server {

class MetaWrapper {
 public:
  static MetaWrapper &
  GetInstance();

  Status
  Init();

  std::shared_ptr<milvus::master::GrpcClient>
  MetaClient();

  uint64_t
  AskSegmentId(const std::string &collection_name, uint64_t channel_id, uint64_t timestamp);

  const milvus::grpc::Schema &
  AskCollectionSchema(const std::string &collection_name);

  Status
  SyncMeta();

 private:
  bool IsCollectionMetaKey(const std::string &key);

  bool IsSegmentMetaKey(const std::string &key);

  void UpdateMeta(const etcdserverpb::WatchResponse &res);

 private:
  std::shared_ptr<milvus::master::GrpcClient> master_client_;
  std::shared_ptr<milvus::master::EtcdClient> etcd_client_;
  std::unordered_map<std::string, masterpb::Collection> schemas_;
  std::unordered_map<uint64_t, masterpb::Segment> segment_infos_;
  std::shared_ptr<milvus::master::Watcher> watcher_;
  std::shared_mutex mutex_;

  std::string etcd_root_path_;
  std::string segment_path_;
  std::string collection_path_;
};

}
}

#include "grpc/etcd.grpc.pb.h"
#include "utils/Status.h"

namespace milvus {
namespace master {

class EtcdClient {
 public:
  explicit EtcdClient(const std::string &addr);
  Status
  Range(const etcdserverpb::RangeRequest& request, etcdserverpb::RangeResponse& response);

 private:
  std::unique_ptr<etcdserverpb::KV::Stub> stub_;
};

}
}

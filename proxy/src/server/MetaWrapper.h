#include "utils/Status.h"
#include "meta/master/GrpcClient.h"

namespace milvus{
namespace server{

class MetaWrapper {
 public:
  static MetaWrapper&
  GetInstance();

  Status
  Init();

  std::shared_ptr<milvus::master::GrpcClient>
  MetaClient();

 private:
  std::shared_ptr<milvus::master::GrpcClient> client_;
};


}
}

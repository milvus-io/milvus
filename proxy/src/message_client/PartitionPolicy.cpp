#include "PartitionPolicy.h"
#include "M3_hash.h"

namespace milvus {
namespace message_client {

int PartitionPolicy::getPartition(const pulsar::Message &msg, const pulsar::TopicMetadata &topicMetadata) {
  int32_t partiton_id = makeHash(msg.getPartitionKey());
  return partiton_id;
}
}
}


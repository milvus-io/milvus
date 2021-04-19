#include "PartitionPolicy.h"
#include "M3_hash.h"
#include <iostream>

namespace milvus {
namespace message_client {

int PartitionPolicy::getPartition(const pulsar::Message &msg, const pulsar::TopicMetadata &topicMetadata) {
  return makeHash(msg.getPartitionKey()) % topicMetadata.getNumPartitions();
}
}
}


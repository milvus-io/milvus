#pragma once

#include <istream>
#include "pulsar/MessageRoutingPolicy.h"
#include "pulsar/Message.h"
#include "pulsar/TopicMetadata.h"

namespace milvus {
namespace message_client {
class PartitionPolicy : public pulsar::MessageRoutingPolicy {

  int getPartition(const pulsar::Message& msg, const pulsar::TopicMetadata& topicMetadata);

};
}
}

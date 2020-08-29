#include "Partition.h"

namespace milvus::dog_segment {

Partition::Partition(std::string& partition_name):
      partition_name_(partition_name) {}

void
Partition::AddNewSegment(uint64_t segment_id) {
  auto segment = CreateSegment();
  segment->set_segment_id(segment_id);
  segments_.emplace_back(segment);
}

Partition*
CreatePartition() {

}

}

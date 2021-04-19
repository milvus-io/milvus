#pragma once

#include "SegmentBase.h"

namespace milvus::dog_segment {

class Partition {
public:
    explicit Partition(std::string& partition_name);

    const std::vector<SegmentBasePtr> &segments() const {
      return segments_;
    }

    void AddNewSegment(uint64_t segment_id);

private:
    std::string partition_name_;
    std::vector<SegmentBasePtr> segments_;
};

using PartitionPtr = std::shared_ptr<Partition>;

Partition* CreatePartiton();

}
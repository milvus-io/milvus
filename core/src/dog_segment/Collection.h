#pragma once

#include "SegmentDefs.h"

//////////////////////////////////////////////////////////////////

class Partition {
public:
    const std::deque<SegmentBasePtr>& segments() const {
      return segments_;
    }

private:
    std::string name_;
    std::deque<SegmentBasePtr> segments_;
};

using PartitionPtr = std::shard_ptr<Partition>;

//////////////////////////////////////////////////////////////////

class Collection {
public:
    explicit Collection(std::string name): name_(name){}

    // TODO: set index
    set_index() {}

    set_schema(std::string config) {
      // TODO: config to schema
      schema_ = null;
    }

public:
//    std::vector<int64_t> Insert() {
//       for (auto partition: partitions_) {
//         for (auto segment: partition.segments()) {
//           if (segment.Status == Status.open) {
//             segment.Insert()
//           }
//         }
//       }
//    }

private:
    // TODO: Index ptr
    IndexPtr index_ = nullptr;
    std::string name_;
    SchemaPtr schema_;
    std::vector<PartitionPtr> partitions_;
};

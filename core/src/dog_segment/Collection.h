#pragma once

#include "SegmentDefs.h"
#include "SegmentBase.h"

//////////////////////////////////////////////////////////////////

namespace milvus::dog_segment {

class Partition {
public:
    explicit Partition(std::string& partition_name): partition_name_(partition_name) {}

    const std::vector<SegmentBasePtr> &segments() const {
      return segments_;
    }

private:
    std::string partition_name_;
    std::vector<SegmentBasePtr> segments_;
};

using PartitionPtr = std::shared_ptr<Partition>;

//////////////////////////////////////////////////////////////////

class Collection {
public:
    explicit Collection(std::string &collection_name, std::string &schema)
        : collection_name_(collection_name), schema_json_(schema) {}

    // TODO: set index
    void set_index() {}

    void parse() {
      // TODO: config to schema
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
    // TODO: add Index ptr
    // IndexPtr index_ = nullptr;
    std::string collection_name_;
    std::string schema_json_;
    milvus::dog_segment::SchemaPtr schema_;
    std::vector<PartitionPtr> partitions_;
};

}

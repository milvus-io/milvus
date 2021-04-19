#pragma once

#include "SegmentBase.h"

namespace milvus::dog_segment {

class Partition {
public:
    explicit Partition(std::string& partition_name, SchemaPtr& schema, IndexMetaPtr& index);

public:
    SchemaPtr& get_schema() {
      return schema_;
    }

    IndexMetaPtr& get_index() {
      return index_;
    }

    std::string& get_partition_name() {
      return partition_name_;
    }

private:
    std::string partition_name_;
    SchemaPtr schema_;
    IndexMetaPtr index_;
};

using PartitionPtr = std::unique_ptr<Partition>;

}
#include "Partition.h"

namespace milvus::dog_segment {

Partition::Partition(std::string& partition_name, SchemaPtr& schema, IndexMetaPtr& index):
      partition_name_(partition_name), schema_(schema), index_(index) {}

}

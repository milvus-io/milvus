#include "Partition.h"

namespace milvus::segcore {

Partition::Partition(std::string& partition_name, SchemaPtr& schema, IndexMetaPtr& index)
    : partition_name_(partition_name), schema_(schema), index_(index) {
}

}  // namespace milvus::segcore

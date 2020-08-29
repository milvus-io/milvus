#include "partition_c.h"
#include "Partition.h"
#include "Collection.h"

CPartition
NewPartition(CCollection collection, const char* partition_name) {
  auto name = std::string(partition_name);
  auto partition = new milvus::dog_segment::Partition(name);

  auto co = (milvus::dog_segment::Collection*)collection;
  co->AddNewPartition();
}

#include "partition_c.h"
#include "Partition.h"
#include "Collection.h"

CPartition
NewPartition(CCollection collection, const char* partition_name) {
  auto c = (milvus::dog_segment::Collection*)collection;

  auto name = std::string(partition_name);

  auto schema = c->get_schema();

  auto partition = std::make_unique<milvus::dog_segment::Partition>(name, schema);

  return (void*)partition.release();
}

void DeletePartition(CPartition partition) {
  auto p = (milvus::dog_segment::Partition*)partition;

  delete p;
}

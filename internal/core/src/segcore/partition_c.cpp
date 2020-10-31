#include "partition_c.h"
#include "Partition.h"
#include "Collection.h"

CPartition
NewPartition(CCollection collection, const char* partition_name) {
    auto c = (milvus::segcore::Collection*)collection;

    auto name = std::string(partition_name);

    auto schema = c->get_schema();

    auto index = c->get_index();

    auto partition = std::make_unique<milvus::segcore::Partition>(name, schema, index);

    // TODO: delete print
    std::cout << "create partition " << name << std::endl;
    return (void*)partition.release();
}

void
DeletePartition(CPartition partition) {
    auto p = (milvus::segcore::Partition*)partition;

    // TODO: delete print
    std::cout << "delete partition " << p->get_partition_name() << std::endl;
    delete p;
}

#include "scheduler/ResourceFactory.h"
#include <gtest/gtest.h>


using namespace zilliz::milvus::engine;

TEST(resource_factory_test, create) {
    auto disk = ResourceFactory::Create("ssd", "DISK", 0);
    auto cpu = ResourceFactory::Create("cpu", "CPU", 0);
    auto gpu = ResourceFactory::Create("gpu", "GPU", 0);

    ASSERT_TRUE(std::dynamic_pointer_cast<DiskResource>(disk));
    ASSERT_TRUE(std::dynamic_pointer_cast<CpuResource>(cpu));
    ASSERT_TRUE(std::dynamic_pointer_cast<GpuResource>(gpu));
}

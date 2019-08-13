#include "scheduler/ResourceFactory.h"
#include <gtest/gtest.h>


using namespace zilliz::milvus::engine;

TEST(resource_factory_test, create) {
    auto disk = ResourceFactory::Create("disk");
    auto cpu = ResourceFactory::Create("cpu");
    auto gpu = ResourceFactory::Create("gpu");

    ASSERT_TRUE(std::dynamic_pointer_cast<DiskResource>(disk));
    ASSERT_TRUE(std::dynamic_pointer_cast<CpuResource>(cpu));
    ASSERT_TRUE(std::dynamic_pointer_cast<GpuResource>(gpu));
}

/*******************************************************************************
 * Copyright 上海赜睿信息科技有限公司(Zilliz) - All Rights Reserved
 * Unauthorized copying of this file, via any medium is strictly prohibited.
 * Proprietary and confidential.
 ******************************************************************************/
#include "TestResource.h"


namespace zilliz {
namespace milvus {
namespace engine {

std::ostream &operator<<(std::ostream &out, const TestResource &resource) {
    out << resource.Dump();
    return out;
}

TestResource::TestResource(std::string name, uint64_t device_id, bool enable_loader, bool enable_executor)
    : Resource(std::move(name), ResourceType::TEST, device_id, enable_loader, enable_executor) {
}

void TestResource::LoadFile(TaskPtr task) {
    task->Load(LoadType::TEST, 0);
}

void TestResource::Process(TaskPtr task) {
    task->Execute();
}

}
}
}


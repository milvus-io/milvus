/*******************************************************************************
 * Copyright 上海赜睿信息科技有限公司(Zilliz) - All Rights Reserved
 * Unauthorized copying of this file, via any medium is strictly prohibited.
 * Proprietary and confidential.
 ******************************************************************************/

#include "CpuResource.h"


namespace zilliz {
namespace milvus {
namespace engine {

std::ostream &operator<<(std::ostream &out, const CpuResource &resource) {
    out << resource.Dump();
    return out;
}

CpuResource::CpuResource(std::string name)
    : Resource(std::move(name), ResourceType::CPU) {}

void CpuResource::LoadFile(TaskPtr task) {
    task->Load(LoadType::DISK2CPU, 0);
}

void CpuResource::Process(TaskPtr task) {
    task->Execute();
}

}
}
}
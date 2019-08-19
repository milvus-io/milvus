/*******************************************************************************
 * Copyright 上海赜睿信息科技有限公司(Zilliz) - All Rights Reserved
 * Unauthorized copying of this file, via any medium is strictly prohibited.
 * Proprietary and confidential.
 ******************************************************************************/

#include "GpuResource.h"


namespace zilliz {
namespace milvus {
namespace engine {

std::ostream &operator<<(std::ostream &out, const GpuResource &resource) {
    out << resource.Dump();
    return out;
}

GpuResource::GpuResource(std::string name)
    : Resource(std::move(name), ResourceType::GPU) {}

void GpuResource::LoadFile(TaskPtr task) {
    task->Load(LoadType::CPU2GPU, 0);
}

void GpuResource::Process(TaskPtr task) {
    task->Execute();
}

}
}
}

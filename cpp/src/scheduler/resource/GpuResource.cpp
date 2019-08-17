/*******************************************************************************
 * Copyright 上海赜睿信息科技有限公司(Zilliz) - All Rights Reserved
 * Unauthorized copying of this file, via any medium is strictly prohibited.
 * Proprietary and confidential.
 ******************************************************************************/

#include "GpuResource.h"


namespace zilliz {
namespace milvus {
namespace engine {


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

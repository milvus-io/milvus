/*******************************************************************************
 * Copyright 上海赜睿信息科技有限公司(Zilliz) - All Rights Reserved
 * Unauthorized copying of this file, via any medium is strictly prohibited.
 * Proprietary and confidential.
 ******************************************************************************/
#include "DiskResource.h"


namespace zilliz {
namespace milvus {
namespace engine {

std::ostream &operator<<(std::ostream &out, const DiskResource &resource) {
    out << resource.Dump();
    return out;
}

DiskResource::DiskResource(std::string name, uint64_t device_id, bool enable_loader, bool enable_executor)
    : Resource(std::move(name), ResourceType::DISK, device_id, enable_loader, enable_executor) {
}

void DiskResource::LoadFile(TaskPtr task) {

}

void DiskResource::Process(TaskPtr task) {

}

}
}
}
 

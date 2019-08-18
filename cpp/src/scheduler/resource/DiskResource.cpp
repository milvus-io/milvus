/*******************************************************************************
 * Copyright 上海赜睿信息科技有限公司(Zilliz) - All Rights Reserved
 * Unauthorized copying of this file, via any medium is strictly prohibited.
 * Proprietary and confidential.
 ******************************************************************************/
#include "DiskResource.h"


namespace zilliz {
namespace milvus {
namespace engine {


DiskResource::DiskResource(std::string name)
    : Resource(std::move(name), ResourceType::DISK, true, false) {
}

void DiskResource::LoadFile(TaskPtr task) {

}

void DiskResource::Process(TaskPtr task) {

}

}
}
}
 

/*******************************************************************************
 * Copyright 上海赜睿信息科技有限公司(Zilliz) - All Rights Reserved
 * Unauthorized copying of this file, via any medium is strictly prohibited.
 * Proprietary and confidential.
 ******************************************************************************/

#include "CpuResource.h"


namespace zilliz {
namespace milvus {
namespace engine {


CpuResource::CpuResource(std::string name)
    : Resource(std::move(name), ResourceType::CPU) {}

void CpuResource::LoadFile(TaskPtr task) {
    task->Load(LoadType::DISK2CPU, 0);
    //if (src.type == DISK) {
    //    fd = open(filename);
    //    content = fd.read();
    //    close(fd);
    //} else if (src.type == CPU) {
    //    memcpy(src, dest, len);
    //} else if (src.type == GPU) {
    //    cudaMemcpyD2H(src, dest);
    //} else {
    //    // unknown type, exception
    //}
}

void CpuResource::Process(TaskPtr task) {
    task->Execute();
}

}
}
}
/*******************************************************************************
 * Copyright 上海赜睿信息科技有限公司(Zilliz) - All Rights Reserved
 * Unauthorized copying of this file, via any medium is strictly prohibited.
 * Proprietary and confidential.
 ******************************************************************************/
#pragma once

#include <string>
#include <memory>

#include "resource/Resource.h"
#include "resource/CpuResource.h"
#include "resource/GpuResource.h"
#include "resource/DiskResource.h"


namespace zilliz {
namespace milvus {
namespace engine {

class ResourceFactory {
public:
    static std::shared_ptr<Resource>
    Create(const std::string &name,
           const std::string &type,
           uint64_t device_id,
           bool enable_loader = true,
           bool enable_executor = true);
};


}
}
}


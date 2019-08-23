/*******************************************************************************
 * Copyright 上海赜睿信息科技有限公司(Zilliz) - All Rights Reserved
 * Unauthorized copying of this file, via any medium is strictly prohibited.
 * Proprietary and confidential.
 ******************************************************************************/

#include "ResourceFactory.h"


namespace zilliz {
namespace milvus {
namespace engine {

std::shared_ptr<Resource>
ResourceFactory::Create(const std::string &name,
                        const std::string &type,
                        uint64_t device_id,
                        bool enable_loader,
                        bool enable_executor) {
    if (type == "DISK") {
        return std::make_shared<DiskResource>(name, device_id, enable_loader, enable_executor);
    } else if (type == "CPU") {
        return std::make_shared<CpuResource>(name, device_id, enable_loader, enable_executor);
    } else if (type == "GPU") {
        return std::make_shared<GpuResource>(name, device_id, enable_loader, enable_executor);
    } else {
        return nullptr;
    }
}

}
}
}

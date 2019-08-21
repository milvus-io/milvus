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
                        const std::string &alias,
                        bool enable_loader,
                        bool enable_executor) {
    if (name == "disk") {
        return std::make_shared<DiskResource>(alias, enable_loader, enable_executor);
    } else if (name == "cpu") {
        return std::make_shared<CpuResource>(alias, enable_loader, enable_executor);
    } else if (name == "gpu") {
        return std::make_shared<GpuResource>(alias, enable_loader, enable_executor);
    } else {
        return nullptr;
    }
}

}
}
}

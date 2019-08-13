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
ResourceFactory::Create(const std::string &name, const std::string &alias) {
    if (name == "disk") {
        return std::make_shared<CpuResource>(alias);
    } else if (name == "cpu") {
        return std::make_shared<CpuResource>(alias);
    } else if (name == "gpu") {
        return std::make_shared<CpuResource>(alias);
    } else {
        return nullptr;
    }
}

}
}
}

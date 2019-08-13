/*******************************************************************************
 * Copyright 上海赜睿信息科技有限公司(Zilliz) - All Rights Reserved
 * Unauthorized copying of this file, via any medium is strictly prohibited.
 * Proprietary and confidential.
 ******************************************************************************/
#pragma once


#include "Resource.h"


namespace zilliz {
namespace milvus {
namespace engine {

class DiskResource : public Resource {
public:
    explicit
    DiskResource(std::string name)
        : Resource(std::move(name), ResourceType::DISK) {}

protected:
    void
    LoadFile(TaskPtr task) override {}

    void
    Process(TaskPtr task) override {}
};

}
}
}

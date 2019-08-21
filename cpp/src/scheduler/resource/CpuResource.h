/*******************************************************************************
 * Copyright 上海赜睿信息科技有限公司(Zilliz) - All Rights Reserved
 * Unauthorized copying of this file, via any medium is strictly prohibited.
 * Proprietary and confidential.
 ******************************************************************************/
#pragma once

#include <string>

#include "Resource.h"


namespace zilliz {
namespace milvus {
namespace engine {

class CpuResource : public Resource {
public:
    explicit
    CpuResource(std::string name, bool enable_loader, bool enable_executor);

    inline std::string
    Dump() const override {
        return "<CpuResource>";
    }

    friend std::ostream &operator<<(std::ostream &out, const CpuResource &resource);

protected:
    void
    LoadFile(TaskPtr task) override;

    void
    Process(TaskPtr task) override;
};

}
}
}

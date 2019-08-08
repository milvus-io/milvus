/*******************************************************************************
 * Copyright 上海赜睿信息科技有限公司(Zilliz) - All Rights Reserved
 * Unauthorized copying of this file, via any medium is strictly prohibited.
 * Proprietary and confidential.
 ******************************************************************************/
#pragma once

#include "IScheduleContext.h"
#include "db/meta/Meta.h"

namespace zilliz {
namespace milvus {
namespace engine {

class DeleteContext : public IScheduleContext {
public:
    DeleteContext(const std::string& table_id, meta::Meta::Ptr& meta_ptr);

    std::string table_id() const { return table_id_; }
    meta::Meta::Ptr meta() const { return meta_ptr_; }

private:
    std::string table_id_;
    meta::Meta::Ptr meta_ptr_;
};

using DeleteContextPtr = std::shared_ptr<DeleteContext>;

}
}
}

/*******************************************************************************
 * Copyright 上海赜睿信息科技有限公司(Zilliz) - All Rights Reserved
 * Unauthorized copying of this file, via any medium is strictly prohibited.
 * Proprietary and confidential.
 ******************************************************************************/

#include "DeleteContext.h"

namespace zilliz {
namespace milvus {
namespace engine {

DeleteContext::DeleteContext(const std::string& table_id, meta::Meta::Ptr& meta_ptr)
    : IScheduleContext(ScheduleContextType::kDelete),
      table_id_(table_id),
      meta_ptr_(meta_ptr) {

}

}
}
}
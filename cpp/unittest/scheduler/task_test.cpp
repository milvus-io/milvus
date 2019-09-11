/*******************************************************************************
 * Copyright 上海赜睿信息科技有限公司(Zilliz) - All Rights Reserved
 * Unauthorized copying of this file, via any medium is strictly prohibited.
 * Proprietary and confidential.
 ******************************************************************************/

#include "scheduler/task/SearchTask.h"
#include <gtest/gtest.h>


namespace zilliz {
namespace milvus {
namespace engine {


TEST(TaskTest, INVALID_INDEX) {
    auto search_task = std::make_shared<XSearchTask>(nullptr);
    search_task->Load(LoadType::TEST, 10);
}

}
}
}

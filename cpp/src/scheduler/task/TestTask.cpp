/*******************************************************************************
 * Copyright 上海赜睿信息科技有限公司(Zilliz) - All Rights Reserved
 * Unauthorized copying of this file, via any medium is strictly prohibited.
 * Proprietary and confidential.
 ******************************************************************************/

#include "TestTask.h"

namespace zilliz {
namespace milvus {
namespace engine {

void
TestTask::Load(LoadType type, uint8_t device_id) {
    load_count_++;
}

void
TestTask::Execute() {
    exec_count_++;
}

}
}
}


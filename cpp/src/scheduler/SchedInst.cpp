/*******************************************************************************
 * Copyright 上海赜睿信息科技有限公司(Zilliz) - All Rights Reserved
 * Unauthorized copying of this file, via any medium is strictly prohibited.
 * Proprietary and confidential.
 ******************************************************************************/

#include "SchedInst.h"


namespace zilliz {
namespace milvus {
namespace engine {

ResourceMgrPtr ResMgrInst::instance = nullptr;
std::mutex ResMgrInst::mutex_;

SchedulerPtr SchedInst::instance = nullptr;
std::mutex SchedInst::mutex_;

}
}
}

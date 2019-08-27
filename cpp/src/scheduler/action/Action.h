/*******************************************************************************
 * copyright 上海赜睿信息科技有限公司(zilliz) - all rights reserved
 * unauthorized copying of this file, via any medium is strictly prohibited.
 * proprietary and confidential.
 ******************************************************************************/
#pragma once

#include "../resource/Resource.h"


namespace zilliz {
namespace milvus {
namespace engine {

class Action {
public:
    static void
    PushTaskToNeighbourRandomly(const TaskPtr &task, const ResourcePtr &self);

    static void
    PushTaskToAllNeighbour(const TaskPtr &task, const ResourcePtr &self);

    static void
    PushTaskToResource(const TaskPtr &task, const ResourcePtr &dest);
};


}
}
}

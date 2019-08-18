/*******************************************************************************
 * Copyright 上海赜睿信息科技有限公司(Zilliz) - All Rights Reserved
 * Unauthorized copying of this file, via any medium is strictly prohibited.
 * Proprietary and confidential.
 ******************************************************************************/

#include "Event.h"
#include "StartUpEvent.h"
#include "CopyCompletedEvent.h"
#include "FinishTaskEvent.h"
#include "TaskTableUpdatedEvent.h"


namespace zilliz {
namespace milvus {
namespace engine {

std::ostream &operator<<(std::ostream &out, const Event &event) {
    out << event.Dump();
    return out;
}

std::ostream &operator<<(std::ostream &out, const StartUpEvent &event) {
    out << event.Dump();
    return out;
}

std::ostream &operator<<(std::ostream &out, const CopyCompletedEvent &event) {
    out << event.Dump();
    return out;
}

std::ostream &operator<<(std::ostream &out, const FinishTaskEvent &event) {
    out << event.Dump();
    return out;
}

std::ostream &operator<<(std::ostream &out, const TaskTableUpdatedEvent &event) {
    out << event.Dump();
    return out;
}

}
}
}

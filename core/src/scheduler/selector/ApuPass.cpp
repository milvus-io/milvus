// Created by ezeharia on 5/18/21.
//

#ifdef MILVUS_APU_VERSION

#include <scheduler/SchedInst.h>
#include <scheduler/tasklabel/SpecResLabel.h>
#include "ApuPass.h"

namespace milvus {
namespace scheduler {


void
ApuPass::Init() {

}

bool
ApuPass::Run(const TaskPtr &task) {

    auto task_type = task->Type();
    auto search_task = std::static_pointer_cast<XSearchTask>(task);

    if (task_type != TaskType::SearchTask ||  search_task->file_->engine_type_ != (int)engine::EngineType::FAISS_BIN_IDMAP ){
        return false;
    }

    auto apu = ResMgrInst::GetInstance()->GetResource(ResourceType::FPGA , 0);
    auto lable = std::make_shared<SpecResLabel>(apu);
    task->label()=lable;


    return true;
}
}
}//milvus
#endif


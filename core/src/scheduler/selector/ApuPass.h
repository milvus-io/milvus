//
// Created by ezeharia on 5/18/21.
//
#ifdef MILVUS_APU_VERSION
#pragma once

#include "Pass.h"

namespace milvus {
    namespace scheduler {

class ApuPass : public Pass {

public:
    ApuPass() = default;

public:
    void Init() override;

    bool Run(const TaskPtr &task) override;

};

}// scheduler
}// milvus
#endif
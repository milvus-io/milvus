//
// Created by ezeharia on 5/12/21.
//
#ifdef MILVUS_APU_VERSION
#pragma once


#include "Apu.h"
#include <memory>
#include <mutex>

namespace Fpga {
class ApuInst {
 public:

    static
    ApuInterfacePtr getInstance () {
        if (instance== nullptr){
            std::lock_guard<std::mutex> lock(mutex_);
            if (instance == nullptr) {
                instance = std::make_shared<ApuInterface>();
            }
        }
        return instance;
    }

 private:
    static ApuInterfacePtr instance;
    static std::mutex mutex_;

};
}// Fpga
#endif
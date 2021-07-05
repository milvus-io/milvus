//
// Created by ezeharia on 5/12/21.
//
#ifdef MILVUS_APU_VERSION

#include "knowhere/index/vector_index/fpga/ApuInst.h"

namespace Fpga{

    ApuInterfacePtr ApuInst::instance= nullptr;
    std::mutex ApuInst::mutex_;
}
#endif
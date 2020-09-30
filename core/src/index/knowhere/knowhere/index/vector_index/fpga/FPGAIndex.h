#pragma once

#include "knowhere/index/vector_index/VecIndex.h"

namespace milvus {
namespace knowhere {

class FPGAIndex {
 public:
    explicit FPGAIndex(const int& device_id) : fpga_id_(device_id) {
    }

    virtual VecIndexPtr
    CopyFpgaToCpu(const Config&) = 0;
  

    void
    SetFpgaDevice(const int& fpga_id) {
        fpga_id_ = fpga_id;
    }

    const int64_t
    GetFpgaDevice() {
        return fpga_id_;
    }

 protected:
    int64_t fpga_id_;

};

}  // namespace knowhere
}  // namespace milvus
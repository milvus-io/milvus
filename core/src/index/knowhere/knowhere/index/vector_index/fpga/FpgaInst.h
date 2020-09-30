#ifndef FPGA_INST_H
#define FPGA_INST_H 
#include<memory>
#include<mutex>
#include"Fpga.h"
namespace Fpga{
class FpgaInst {
 public:
    static FpgaInterfacePtr
    GetInstance() {
        if (instance == nullptr) {
            std::lock_guard<std::mutex> lock(mutex_);
            if (instance == nullptr) {
                instance = std::make_shared<FpgaInterface>();
            }
        }
        return instance;
    }

 private:
    static FpgaInterfacePtr instance;
    static std::mutex mutex_;
};
}
#endif

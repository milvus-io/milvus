#include"FpgaInst.h"

namespace Fpga {

 FpgaInterfacePtr FpgaInst::instance=nullptr;
 std::mutex FpgaInst::mutex_;
}

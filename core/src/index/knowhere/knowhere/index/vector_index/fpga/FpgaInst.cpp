#include"FpgaInst.h"

namespace Fpga {

 FpgaPtr FpgaInst::instance=nullptr;
 std::mutex FpgaInst::mutex_;
}

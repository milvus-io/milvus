#include "TSO.h"

namespace milvus {
namespace server {

TSOracle& TSOracle::GetInstance() {
  static TSOracle oracle;
  return oracle;
}

uint64_t TSOracle::GetTimeStamp() {
  std::lock_guard lock(mutex_);
  auto now = std::chrono::high_resolution_clock::now();
  uint64_t physical = GetPhysical(now);
  uint64_t ts = ComposeTs(physical, 0);

  if (last_time_stamp_ == ts) {
    logical_++;
    return ts + logical_;
  }
  last_time_stamp_ = ts;
  logical_ = 0;
  return ts;
}

uint64_t TSOracle::GetPhysical(const std::chrono::high_resolution_clock::time_point &t) {
  auto nano_time = std::chrono::duration_cast<std::chrono::milliseconds>(t.time_since_epoch());
  return nano_time.count();
}

uint64_t TSOracle::ComposeTs(uint64_t physical, uint64_t logical) {
  return uint64_t((physical << physical_shift_bits) + logical);
}

}
}
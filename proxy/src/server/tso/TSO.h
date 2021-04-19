#pragma once
#include <mutex>
#include <chrono>

namespace milvus {
namespace server {

const uint32_t physical_shift_bits = 18;

class TSOracle {
 public:
  static TSOracle& GetInstance();

  uint64_t GetTimeStamp();

 private:
  uint64_t GetPhysical(const std::chrono::high_resolution_clock::time_point &t);
  uint64_t ComposeTs(uint64_t physical, uint64_t logical);

 private:
  TSOracle() = default;

 private:
  std::mutex mutex_;
  uint64_t last_time_stamp_;
  uint64_t logical_;
};
}
}
#pragma once

#include <istream>
namespace milvus {
namespace message_client {

  int32_t makeHash(const std::string& key);
  uint32_t fmix(uint32_t h);
  uint32_t mixK1(uint32_t k1);
  uint32_t mixH1(uint32_t h1, uint32_t k1);
  uint32_t rotate_left(uint32_t x, uint8_t r);
  uint32_t makeHash(const void* key, const int64_t len);

}
}

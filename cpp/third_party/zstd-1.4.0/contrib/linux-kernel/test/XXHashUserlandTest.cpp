extern "C" {
#include <linux/errno.h>
#include <linux/xxhash.h>
}
#include <gtest/gtest.h>
#include <array>
#include <iostream>
#include <memory>
#include <string>
#define XXH_STATIC_LINKING_ONLY
#include <xxhash.h>

using namespace std;

namespace {
const std::array<std::string, 11> kTestInputs = {
  "",
  "0",
  "01234",
  "0123456789abcde",
  "0123456789abcdef",
  "0123456789abcdef0",
  "0123456789abcdef0123",
  "0123456789abcdef0123456789abcde",
  "0123456789abcdef0123456789abcdef",
  "0123456789abcdef0123456789abcdef0",
  "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
};

bool testXXH32(const void *input, const size_t length, uint32_t seed) {
  return XXH32(input, length, seed) == xxh32(input, length, seed);
}

bool testXXH64(const void *input, const size_t length, uint32_t seed) {
  return XXH64(input, length, seed) == xxh64(input, length, seed);
}

class XXH32State {
  struct xxh32_state kernelState;
  XXH32_state_t state;

public:
  explicit XXH32State(const uint32_t seed) { reset(seed); }
  XXH32State(XXH32State const& other) noexcept {
    xxh32_copy_state(&kernelState, &other.kernelState);
    XXH32_copyState(&state, &other.state);
  }
  XXH32State& operator=(XXH32State const& other) noexcept {
    xxh32_copy_state(&kernelState, &other.kernelState);
    XXH32_copyState(&state, &other.state);
    return *this;
  }

  void reset(const uint32_t seed) {
    xxh32_reset(&kernelState, seed);
    EXPECT_EQ(0, XXH32_reset(&state, seed));
  }

  void update(const void *input, const size_t length) {
    EXPECT_EQ(0, xxh32_update(&kernelState, input, length));
    EXPECT_EQ(0, (int)XXH32_update(&state, input, length));
  }

  bool testDigest() const {
    return xxh32_digest(&kernelState) == XXH32_digest(&state);
  }
};

class XXH64State {
  struct xxh64_state kernelState;
  XXH64_state_t state;

public:
  explicit XXH64State(const uint64_t seed) { reset(seed); }
  XXH64State(XXH64State const& other) noexcept {
    xxh64_copy_state(&kernelState, &other.kernelState);
    XXH64_copyState(&state, &other.state);
  }
  XXH64State& operator=(XXH64State const& other) noexcept {
    xxh64_copy_state(&kernelState, &other.kernelState);
    XXH64_copyState(&state, &other.state);
    return *this;
  }

  void reset(const uint64_t seed) {
    xxh64_reset(&kernelState, seed);
    EXPECT_EQ(0, XXH64_reset(&state, seed));
  }

  void update(const void *input, const size_t length) {
    EXPECT_EQ(0, xxh64_update(&kernelState, input, length));
    EXPECT_EQ(0, (int)XXH64_update(&state, input, length));
  }

  bool testDigest() const {
    return xxh64_digest(&kernelState) == XXH64_digest(&state);
  }
};
}

TEST(Simple, Null) {
  EXPECT_TRUE(testXXH32(NULL, 0, 0));
  EXPECT_TRUE(testXXH64(NULL, 0, 0));
}

TEST(Stream, Null) {
  struct xxh32_state state32;
  xxh32_reset(&state32, 0);
  EXPECT_EQ(-EINVAL, xxh32_update(&state32, NULL, 0));

  struct xxh64_state state64;
  xxh64_reset(&state64, 0);
  EXPECT_EQ(-EINVAL, xxh64_update(&state64, NULL, 0));
}

TEST(Simple, TestInputs) {
  for (uint32_t seed = 0; seed < 100000; seed = (seed + 1) * 3) {
    for (auto const input : kTestInputs) {
      EXPECT_TRUE(testXXH32(input.data(), input.size(), seed));
      EXPECT_TRUE(testXXH64(input.data(), input.size(), (uint64_t)seed));
    }
  }
}

TEST(Stream, TestInputs) {
  for (uint32_t seed = 0; seed < 100000; seed = (seed + 1) * 3) {
    for (auto const input : kTestInputs) {
      XXH32State s32(seed);
      XXH64State s64(seed);
      s32.update(input.data(), input.size());
      s64.update(input.data(), input.size());
      EXPECT_TRUE(s32.testDigest());
      EXPECT_TRUE(s64.testDigest());
    }
  }
}

TEST(Stream, MultipleTestInputs) {
  for (uint32_t seed = 0; seed < 100000; seed = (seed + 1) * 3) {
    XXH32State s32(seed);
    XXH64State s64(seed);
    for (auto const input : kTestInputs) {
      s32.update(input.data(), input.size());
      s64.update(input.data(), input.size());
    }
    EXPECT_TRUE(s32.testDigest());
    EXPECT_TRUE(s64.testDigest());
  }
}

TEST(Stream, CopyState) {
  for (uint32_t seed = 0; seed < 100000; seed = (seed + 1) * 3) {
    XXH32State s32(seed);
    XXH64State s64(seed);
    for (auto const input : kTestInputs) {
      auto t32(s32);
      t32.update(input.data(), input.size());
      s32 = t32;
      auto t64(s64);
      t64.update(input.data(), input.size());
      s64 = t64;
    }
    EXPECT_TRUE(s32.testDigest());
    EXPECT_TRUE(s64.testDigest());
  }
}

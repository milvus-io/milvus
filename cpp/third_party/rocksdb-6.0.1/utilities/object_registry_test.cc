// Copyright (c) 2016-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#ifndef ROCKSDB_LITE

#include "rocksdb/utilities/object_registry.h"
#include "util/testharness.h"

namespace rocksdb {

class EnvRegistryTest : public testing::Test {
 public:
  static int num_a, num_b;
};

int EnvRegistryTest::num_a = 0;
int EnvRegistryTest::num_b = 0;

static Registrar<Env> test_reg_a("a://.*",
                                 [](const std::string& /*uri*/,
                                    std::unique_ptr<Env>* /*env_guard*/) {
                                   ++EnvRegistryTest::num_a;
                                   return Env::Default();
                                 });

static Registrar<Env> test_reg_b("b://.*", [](const std::string& /*uri*/,
                                              std::unique_ptr<Env>* env_guard) {
  ++EnvRegistryTest::num_b;
  // Env::Default() is a singleton so we can't grant ownership directly to the
  // caller - we must wrap it first.
  env_guard->reset(new EnvWrapper(Env::Default()));
  return env_guard->get();
});

TEST_F(EnvRegistryTest, Basics) {
  std::unique_ptr<Env> env_guard;
  auto res = NewCustomObject<Env>("a://test", &env_guard);
  ASSERT_NE(res, nullptr);
  ASSERT_EQ(env_guard, nullptr);
  ASSERT_EQ(1, num_a);
  ASSERT_EQ(0, num_b);

  res = NewCustomObject<Env>("b://test", &env_guard);
  ASSERT_NE(res, nullptr);
  ASSERT_NE(env_guard, nullptr);
  ASSERT_EQ(1, num_a);
  ASSERT_EQ(1, num_b);

  res = NewCustomObject<Env>("c://test", &env_guard);
  ASSERT_EQ(res, nullptr);
  ASSERT_EQ(env_guard, nullptr);
  ASSERT_EQ(1, num_a);
  ASSERT_EQ(1, num_b);
}

}  // namespace rocksdb

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}

#else  // ROCKSDB_LITE
#include <stdio.h>

int main(int /*argc*/, char** /*argv*/) {
  fprintf(stderr, "SKIPPED as EnvRegistry is not supported in ROCKSDB_LITE\n");
  return 0;
}

#endif  // ROCKSDB_LITE

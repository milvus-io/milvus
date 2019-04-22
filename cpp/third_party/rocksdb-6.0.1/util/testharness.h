//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#pragma once

#ifdef OS_AIX
#include "gtest/gtest.h"
#else
#include <gtest/gtest.h>
#endif

#include <string>
#include "rocksdb/env.h"

namespace rocksdb {
namespace test {

// Return the directory to use for temporary storage.
std::string TmpDir(Env* env = Env::Default());

// A path unique within the thread
std::string PerThreadDBPath(std::string name);
std::string PerThreadDBPath(Env* env, std::string name);
std::string PerThreadDBPath(std::string dir, std::string name);

// Return a randomization seed for this run.  Typically returns the
// same number on repeated invocations of this binary, but automated
// runs may be able to vary the seed.
int RandomSeed();

::testing::AssertionResult AssertStatus(const char* s_expr, const Status& s);

#define ASSERT_OK(s) ASSERT_PRED_FORMAT1(rocksdb::test::AssertStatus, s)
#define ASSERT_NOK(s) ASSERT_FALSE((s).ok())
#define EXPECT_OK(s) EXPECT_PRED_FORMAT1(rocksdb::test::AssertStatus, s)
#define EXPECT_NOK(s) EXPECT_FALSE((s).ok())

}  // namespace test
}  // namespace rocksdb

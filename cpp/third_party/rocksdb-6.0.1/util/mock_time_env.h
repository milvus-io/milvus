// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#pragma once

#include "rocksdb/env.h"

namespace rocksdb {

class MockTimeEnv : public EnvWrapper {
 public:
  explicit MockTimeEnv(Env* base) : EnvWrapper(base) {}

  virtual Status GetCurrentTime(int64_t* time) override {
    assert(time != nullptr);
    assert(current_time_ <=
           static_cast<uint64_t>(std::numeric_limits<int64_t>::max()));
    *time = static_cast<int64_t>(current_time_);
    return Status::OK();
  }

  virtual uint64_t NowMicros() override {
    assert(current_time_ <= std::numeric_limits<uint64_t>::max() / 1000000);
    return current_time_ * 1000000;
  }

  virtual uint64_t NowNanos() override {
    assert(current_time_ <= std::numeric_limits<uint64_t>::max() / 1000000000);
    return current_time_ * 1000000000;
  }

  void set_current_time(uint64_t time) {
    assert(time >= current_time_);
    current_time_ = time;
  }

 private:
  std::atomic<uint64_t> current_time_{0};
};

}  // namespace rocksdb

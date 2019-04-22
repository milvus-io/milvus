//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#pragma once

#include <stdint.h>

#include <atomic>
#include <memory>
#include "rocksdb/rate_limiter.h"

namespace rocksdb {

class Env;
class WriteControllerToken;

// WriteController is controlling write stalls in our write code-path. Write
// stalls happen when compaction can't keep up with write rate.
// All of the methods here (including WriteControllerToken's destructors) need
// to be called while holding DB mutex
class WriteController {
 public:
  explicit WriteController(uint64_t _delayed_write_rate = 1024u * 1024u * 32u,
                           int64_t low_pri_rate_bytes_per_sec = 1024 * 1024)
      : total_stopped_(0),
        total_delayed_(0),
        total_compaction_pressure_(0),
        bytes_left_(0),
        last_refill_time_(0),
        low_pri_rate_limiter_(
            NewGenericRateLimiter(low_pri_rate_bytes_per_sec)) {
    set_max_delayed_write_rate(_delayed_write_rate);
  }
  ~WriteController() = default;

  // When an actor (column family) requests a stop token, all writes will be
  // stopped until the stop token is released (deleted)
  std::unique_ptr<WriteControllerToken> GetStopToken();
  // When an actor (column family) requests a delay token, total delay for all
  // writes to the DB will be controlled under the delayed write rate. Every
  // write needs to call GetDelay() with number of bytes writing to the DB,
  // which returns number of microseconds to sleep.
  std::unique_ptr<WriteControllerToken> GetDelayToken(
      uint64_t delayed_write_rate);
  // When an actor (column family) requests a moderate token, compaction
  // threads will be increased
  std::unique_ptr<WriteControllerToken> GetCompactionPressureToken();

  // these three metods are querying the state of the WriteController
  bool IsStopped() const;
  bool NeedsDelay() const { return total_delayed_.load() > 0; }
  bool NeedSpeedupCompaction() const {
    return IsStopped() || NeedsDelay() || total_compaction_pressure_ > 0;
  }
  // return how many microseconds the caller needs to sleep after the call
  // num_bytes: how many number of bytes to put into the DB.
  // Prerequisite: DB mutex held.
  uint64_t GetDelay(Env* env, uint64_t num_bytes);
  void set_delayed_write_rate(uint64_t write_rate) {
    // avoid divide 0
    if (write_rate == 0) {
      write_rate = 1u;
    } else if (write_rate > max_delayed_write_rate()) {
      write_rate = max_delayed_write_rate();
    }
    delayed_write_rate_ = write_rate;
  }

  void set_max_delayed_write_rate(uint64_t write_rate) {
    // avoid divide 0
    if (write_rate == 0) {
      write_rate = 1u;
    }
    max_delayed_write_rate_ = write_rate;
    // update delayed_write_rate_ as well
    delayed_write_rate_ = write_rate;
  }

  uint64_t delayed_write_rate() const { return delayed_write_rate_; }

  uint64_t max_delayed_write_rate() const { return max_delayed_write_rate_; }

  RateLimiter* low_pri_rate_limiter() { return low_pri_rate_limiter_.get(); }

 private:
  uint64_t NowMicrosMonotonic(Env* env);

  friend class WriteControllerToken;
  friend class StopWriteToken;
  friend class DelayWriteToken;
  friend class CompactionPressureToken;

  std::atomic<int> total_stopped_;
  std::atomic<int> total_delayed_;
  std::atomic<int> total_compaction_pressure_;
  uint64_t bytes_left_;
  uint64_t last_refill_time_;
  // write rate set when initialization or by `DBImpl::SetDBOptions`
  uint64_t max_delayed_write_rate_;
  // current write rate
  uint64_t delayed_write_rate_;

  std::unique_ptr<RateLimiter> low_pri_rate_limiter_;
};

class WriteControllerToken {
 public:
  explicit WriteControllerToken(WriteController* controller)
      : controller_(controller) {}
  virtual ~WriteControllerToken() {}

 protected:
  WriteController* controller_;

 private:
  // no copying allowed
  WriteControllerToken(const WriteControllerToken&) = delete;
  void operator=(const WriteControllerToken&) = delete;
};

class StopWriteToken : public WriteControllerToken {
 public:
  explicit StopWriteToken(WriteController* controller)
      : WriteControllerToken(controller) {}
  virtual ~StopWriteToken();
};

class DelayWriteToken : public WriteControllerToken {
 public:
  explicit DelayWriteToken(WriteController* controller)
      : WriteControllerToken(controller) {}
  virtual ~DelayWriteToken();
};

class CompactionPressureToken : public WriteControllerToken {
 public:
  explicit CompactionPressureToken(WriteController* controller)
      : WriteControllerToken(controller) {}
  virtual ~CompactionPressureToken();
};

}  // namespace rocksdb

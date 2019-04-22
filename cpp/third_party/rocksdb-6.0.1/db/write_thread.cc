//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include "db/write_thread.h"
#include <chrono>
#include <thread>
#include "db/column_family.h"
#include "monitoring/perf_context_imp.h"
#include "port/port.h"
#include "util/random.h"
#include "util/sync_point.h"

namespace rocksdb {

WriteThread::WriteThread(const ImmutableDBOptions& db_options)
    : max_yield_usec_(db_options.enable_write_thread_adaptive_yield
                          ? db_options.write_thread_max_yield_usec
                          : 0),
      slow_yield_usec_(db_options.write_thread_slow_yield_usec),
      allow_concurrent_memtable_write_(
          db_options.allow_concurrent_memtable_write),
      enable_pipelined_write_(db_options.enable_pipelined_write),
      newest_writer_(nullptr),
      newest_memtable_writer_(nullptr),
      last_sequence_(0),
      write_stall_dummy_(),
      stall_mu_(),
      stall_cv_(&stall_mu_) {}

uint8_t WriteThread::BlockingAwaitState(Writer* w, uint8_t goal_mask) {
  // We're going to block.  Lazily create the mutex.  We guarantee
  // propagation of this construction to the waker via the
  // STATE_LOCKED_WAITING state.  The waker won't try to touch the mutex
  // or the condvar unless they CAS away the STATE_LOCKED_WAITING that
  // we install below.
  w->CreateMutex();

  auto state = w->state.load(std::memory_order_acquire);
  assert(state != STATE_LOCKED_WAITING);
  if ((state & goal_mask) == 0 &&
      w->state.compare_exchange_strong(state, STATE_LOCKED_WAITING)) {
    // we have permission (and an obligation) to use StateMutex
    std::unique_lock<std::mutex> guard(w->StateMutex());
    w->StateCV().wait(guard, [w] {
      return w->state.load(std::memory_order_relaxed) != STATE_LOCKED_WAITING;
    });
    state = w->state.load(std::memory_order_relaxed);
  }
  // else tricky.  Goal is met or CAS failed.  In the latter case the waker
  // must have changed the state, and compare_exchange_strong has updated
  // our local variable with the new one.  At the moment WriteThread never
  // waits for a transition across intermediate states, so we know that
  // since a state change has occurred the goal must have been met.
  assert((state & goal_mask) != 0);
  return state;
}

uint8_t WriteThread::AwaitState(Writer* w, uint8_t goal_mask,
                                AdaptationContext* ctx) {
  uint8_t state;

  // 1. Busy loop using "pause" for 1 micro sec
  // 2. Else SOMETIMES busy loop using "yield" for 100 micro sec (default)
  // 3. Else blocking wait

  // On a modern Xeon each loop takes about 7 nanoseconds (most of which
  // is the effect of the pause instruction), so 200 iterations is a bit
  // more than a microsecond.  This is long enough that waits longer than
  // this can amortize the cost of accessing the clock and yielding.
  for (uint32_t tries = 0; tries < 200; ++tries) {
    state = w->state.load(std::memory_order_acquire);
    if ((state & goal_mask) != 0) {
      return state;
    }
    port::AsmVolatilePause();
  }

  // This is below the fast path, so that the stat is zero when all writes are
  // from the same thread.
  PERF_TIMER_GUARD(write_thread_wait_nanos);

  // If we're only going to end up waiting a short period of time,
  // it can be a lot more efficient to call std::this_thread::yield()
  // in a loop than to block in StateMutex().  For reference, on my 4.0
  // SELinux test server with support for syscall auditing enabled, the
  // minimum latency between FUTEX_WAKE to returning from FUTEX_WAIT is
  // 2.7 usec, and the average is more like 10 usec.  That can be a big
  // drag on RockDB's single-writer design.  Of course, spinning is a
  // bad idea if other threads are waiting to run or if we're going to
  // wait for a long time.  How do we decide?
  //
  // We break waiting into 3 categories: short-uncontended,
  // short-contended, and long.  If we had an oracle, then we would always
  // spin for short-uncontended, always block for long, and our choice for
  // short-contended might depend on whether we were trying to optimize
  // RocksDB throughput or avoid being greedy with system resources.
  //
  // Bucketing into short or long is easy by measuring elapsed time.
  // Differentiating short-uncontended from short-contended is a bit
  // trickier, but not too bad.  We could look for involuntary context
  // switches using getrusage(RUSAGE_THREAD, ..), but it's less work
  // (portability code and CPU) to just look for yield calls that take
  // longer than we expect.  sched_yield() doesn't actually result in any
  // context switch overhead if there are no other runnable processes
  // on the current core, in which case it usually takes less than
  // a microsecond.
  //
  // There are two primary tunables here: the threshold between "short"
  // and "long" waits, and the threshold at which we suspect that a yield
  // is slow enough to indicate we should probably block.  If these
  // thresholds are chosen well then CPU-bound workloads that don't
  // have more threads than cores will experience few context switches
  // (voluntary or involuntary), and the total number of context switches
  // (voluntary and involuntary) will not be dramatically larger (maybe
  // 2x) than the number of voluntary context switches that occur when
  // --max_yield_wait_micros=0.
  //
  // There's another constant, which is the number of slow yields we will
  // tolerate before reversing our previous decision.  Solitary slow
  // yields are pretty common (low-priority small jobs ready to run),
  // so this should be at least 2.  We set this conservatively to 3 so
  // that we can also immediately schedule a ctx adaptation, rather than
  // waiting for the next update_ctx.

  const size_t kMaxSlowYieldsWhileSpinning = 3;

  // Whether the yield approach has any credit in this context. The credit is
  // added by yield being succesfull before timing out, and decreased otherwise.
  auto& yield_credit = ctx->value;
  // Update the yield_credit based on sample runs or right after a hard failure
  bool update_ctx = false;
  // Should we reinforce the yield credit
  bool would_spin_again = false;
  // The samling base for updating the yeild credit. The sampling rate would be
  // 1/sampling_base.
  const int sampling_base = 256;

  if (max_yield_usec_ > 0) {
    update_ctx = Random::GetTLSInstance()->OneIn(sampling_base);

    if (update_ctx || yield_credit.load(std::memory_order_relaxed) >= 0) {
      // we're updating the adaptation statistics, or spinning has >
      // 50% chance of being shorter than max_yield_usec_ and causing no
      // involuntary context switches
      auto spin_begin = std::chrono::steady_clock::now();

      // this variable doesn't include the final yield (if any) that
      // causes the goal to be met
      size_t slow_yield_count = 0;

      auto iter_begin = spin_begin;
      while ((iter_begin - spin_begin) <=
             std::chrono::microseconds(max_yield_usec_)) {
        std::this_thread::yield();

        state = w->state.load(std::memory_order_acquire);
        if ((state & goal_mask) != 0) {
          // success
          would_spin_again = true;
          break;
        }

        auto now = std::chrono::steady_clock::now();
        if (now == iter_begin ||
            now - iter_begin >= std::chrono::microseconds(slow_yield_usec_)) {
          // conservatively count it as a slow yield if our clock isn't
          // accurate enough to measure the yield duration
          ++slow_yield_count;
          if (slow_yield_count >= kMaxSlowYieldsWhileSpinning) {
            // Not just one ivcsw, but several.  Immediately update yield_credit
            // and fall back to blocking
            update_ctx = true;
            break;
          }
        }
        iter_begin = now;
      }
    }
  }

  if ((state & goal_mask) == 0) {
    TEST_SYNC_POINT_CALLBACK("WriteThread::AwaitState:BlockingWaiting", w);
    state = BlockingAwaitState(w, goal_mask);
  }

  if (update_ctx) {
    // Since our update is sample based, it is ok if a thread overwrites the
    // updates by other threads. Thus the update does not have to be atomic.
    auto v = yield_credit.load(std::memory_order_relaxed);
    // fixed point exponential decay with decay constant 1/1024, with +1
    // and -1 scaled to avoid overflow for int32_t
    //
    // On each update the positive credit is decayed by a facor of 1/1024 (i.e.,
    // 0.1%). If the sampled yield was successful, the credit is also increased
    // by X. Setting X=2^17 ensures that the credit never exceeds
    // 2^17*2^10=2^27, which is lower than 2^31 the upperbound of int32_t. Same
    // logic applies to negative credits.
    v = v - (v / 1024) + (would_spin_again ? 1 : -1) * 131072;
    yield_credit.store(v, std::memory_order_relaxed);
  }

  assert((state & goal_mask) != 0);
  return state;
}

void WriteThread::SetState(Writer* w, uint8_t new_state) {
  auto state = w->state.load(std::memory_order_acquire);
  if (state == STATE_LOCKED_WAITING ||
      !w->state.compare_exchange_strong(state, new_state)) {
    assert(state == STATE_LOCKED_WAITING);

    std::lock_guard<std::mutex> guard(w->StateMutex());
    assert(w->state.load(std::memory_order_relaxed) != new_state);
    w->state.store(new_state, std::memory_order_relaxed);
    w->StateCV().notify_one();
  }
}

bool WriteThread::LinkOne(Writer* w, std::atomic<Writer*>* newest_writer) {
  assert(newest_writer != nullptr);
  assert(w->state == STATE_INIT);
  Writer* writers = newest_writer->load(std::memory_order_relaxed);
  while (true) {
    // If write stall in effect, and w->no_slowdown is not true,
    // block here until stall is cleared. If its true, then return
    // immediately
    if (writers == &write_stall_dummy_) {
      if (w->no_slowdown) {
        w->status = Status::Incomplete("Write stall");
        SetState(w, STATE_COMPLETED);
        return false;
      }
      // Since no_slowdown is false, wait here to be notified of the write
      // stall clearing
      {
        MutexLock lock(&stall_mu_);
        writers = newest_writer->load(std::memory_order_relaxed);
        if (writers == &write_stall_dummy_) {
          stall_cv_.Wait();
          // Load newest_writers_ again since it may have changed
          writers = newest_writer->load(std::memory_order_relaxed);
          continue;
        }
      }
    }
    w->link_older = writers;
    if (newest_writer->compare_exchange_weak(writers, w)) {
      return (writers == nullptr);
    }
  }
}

bool WriteThread::LinkGroup(WriteGroup& write_group,
                            std::atomic<Writer*>* newest_writer) {
  assert(newest_writer != nullptr);
  Writer* leader = write_group.leader;
  Writer* last_writer = write_group.last_writer;
  Writer* w = last_writer;
  while (true) {
    // Unset link_newer pointers to make sure when we call
    // CreateMissingNewerLinks later it create all missing links.
    w->link_newer = nullptr;
    w->write_group = nullptr;
    if (w == leader) {
      break;
    }
    w = w->link_older;
  }
  Writer* newest = newest_writer->load(std::memory_order_relaxed);
  while (true) {
    leader->link_older = newest;
    if (newest_writer->compare_exchange_weak(newest, last_writer)) {
      return (newest == nullptr);
    }
  }
}

void WriteThread::CreateMissingNewerLinks(Writer* head) {
  while (true) {
    Writer* next = head->link_older;
    if (next == nullptr || next->link_newer != nullptr) {
      assert(next == nullptr || next->link_newer == head);
      break;
    }
    next->link_newer = head;
    head = next;
  }
}

WriteThread::Writer* WriteThread::FindNextLeader(Writer* from,
                                                 Writer* boundary) {
  assert(from != nullptr && from != boundary);
  Writer* current = from;
  while (current->link_older != boundary) {
    current = current->link_older;
    assert(current != nullptr);
  }
  return current;
}

void WriteThread::CompleteLeader(WriteGroup& write_group) {
  assert(write_group.size > 0);
  Writer* leader = write_group.leader;
  if (write_group.size == 1) {
    write_group.leader = nullptr;
    write_group.last_writer = nullptr;
  } else {
    assert(leader->link_newer != nullptr);
    leader->link_newer->link_older = nullptr;
    write_group.leader = leader->link_newer;
  }
  write_group.size -= 1;
  SetState(leader, STATE_COMPLETED);
}

void WriteThread::CompleteFollower(Writer* w, WriteGroup& write_group) {
  assert(write_group.size > 1);
  assert(w != write_group.leader);
  if (w == write_group.last_writer) {
    w->link_older->link_newer = nullptr;
    write_group.last_writer = w->link_older;
  } else {
    w->link_older->link_newer = w->link_newer;
    w->link_newer->link_older = w->link_older;
  }
  write_group.size -= 1;
  SetState(w, STATE_COMPLETED);
}

void WriteThread::BeginWriteStall() {
  LinkOne(&write_stall_dummy_, &newest_writer_);

  // Walk writer list until w->write_group != nullptr. The current write group
  // will not have a mix of slowdown/no_slowdown, so its ok to stop at that
  // point
  Writer* w = write_stall_dummy_.link_older;
  Writer* prev = &write_stall_dummy_;
  while (w != nullptr && w->write_group == nullptr) {
    if (w->no_slowdown) {
      prev->link_older = w->link_older;
      w->status = Status::Incomplete("Write stall");
      SetState(w, STATE_COMPLETED);
      w = prev->link_older;
    } else {
      prev = w;
      w = w->link_older;
    }
  }
}

void WriteThread::EndWriteStall() {
  MutexLock lock(&stall_mu_);

  assert(newest_writer_.load(std::memory_order_relaxed) == &write_stall_dummy_);
  newest_writer_.exchange(write_stall_dummy_.link_older);

  // Wake up writers
  stall_cv_.SignalAll();
}

static WriteThread::AdaptationContext jbg_ctx("JoinBatchGroup");
void WriteThread::JoinBatchGroup(Writer* w) {
  TEST_SYNC_POINT_CALLBACK("WriteThread::JoinBatchGroup:Start", w);
  assert(w->batch != nullptr);

  bool linked_as_leader = LinkOne(w, &newest_writer_);

  if (linked_as_leader) {
    SetState(w, STATE_GROUP_LEADER);
  }

  TEST_SYNC_POINT_CALLBACK("WriteThread::JoinBatchGroup:Wait", w);

  if (!linked_as_leader) {
    /**
     * Wait util:
     * 1) An existing leader pick us as the new leader when it finishes
     * 2) An existing leader pick us as its follewer and
     * 2.1) finishes the memtable writes on our behalf
     * 2.2) Or tell us to finish the memtable writes in pralallel
     * 3) (pipelined write) An existing leader pick us as its follower and
     *    finish book-keeping and WAL write for us, enqueue us as pending
     *    memtable writer, and
     * 3.1) we become memtable writer group leader, or
     * 3.2) an existing memtable writer group leader tell us to finish memtable
     *      writes in parallel.
     */
    TEST_SYNC_POINT_CALLBACK("WriteThread::JoinBatchGroup:BeganWaiting", w);
    AwaitState(w, STATE_GROUP_LEADER | STATE_MEMTABLE_WRITER_LEADER |
                      STATE_PARALLEL_MEMTABLE_WRITER | STATE_COMPLETED,
               &jbg_ctx);
    TEST_SYNC_POINT_CALLBACK("WriteThread::JoinBatchGroup:DoneWaiting", w);
  }
}

size_t WriteThread::EnterAsBatchGroupLeader(Writer* leader,
                                            WriteGroup* write_group) {
  assert(leader->link_older == nullptr);
  assert(leader->batch != nullptr);
  assert(write_group != nullptr);

  size_t size = WriteBatchInternal::ByteSize(leader->batch);

  // Allow the group to grow up to a maximum size, but if the
  // original write is small, limit the growth so we do not slow
  // down the small write too much.
  size_t max_size = 1 << 20;
  if (size <= (128 << 10)) {
    max_size = size + (128 << 10);
  }

  leader->write_group = write_group;
  write_group->leader = leader;
  write_group->last_writer = leader;
  write_group->size = 1;
  Writer* newest_writer = newest_writer_.load(std::memory_order_acquire);

  // This is safe regardless of any db mutex status of the caller. Previous
  // calls to ExitAsGroupLeader either didn't call CreateMissingNewerLinks
  // (they emptied the list and then we added ourself as leader) or had to
  // explicitly wake us up (the list was non-empty when we added ourself,
  // so we have already received our MarkJoined).
  CreateMissingNewerLinks(newest_writer);

  // Tricky. Iteration start (leader) is exclusive and finish
  // (newest_writer) is inclusive. Iteration goes from old to new.
  Writer* w = leader;
  while (w != newest_writer) {
    w = w->link_newer;

    if (w->sync && !leader->sync) {
      // Do not include a sync write into a batch handled by a non-sync write.
      break;
    }

    if (w->no_slowdown != leader->no_slowdown) {
      // Do not mix writes that are ok with delays with the ones that
      // request fail on delays.
      break;
    }

    if (!w->disable_wal && leader->disable_wal) {
      // Do not include a write that needs WAL into a batch that has
      // WAL disabled.
      break;
    }

    if (w->batch == nullptr) {
      // Do not include those writes with nullptr batch. Those are not writes,
      // those are something else. They want to be alone
      break;
    }

    if (w->callback != nullptr && !w->callback->AllowWriteBatching()) {
      // dont batch writes that don't want to be batched
      break;
    }

    auto batch_size = WriteBatchInternal::ByteSize(w->batch);
    if (size + batch_size > max_size) {
      // Do not make batch too big
      break;
    }

    w->write_group = write_group;
    size += batch_size;
    write_group->last_writer = w;
    write_group->size++;
  }
  TEST_SYNC_POINT_CALLBACK("WriteThread::EnterAsBatchGroupLeader:End", w);
  return size;
}

void WriteThread::EnterAsMemTableWriter(Writer* leader,
                                        WriteGroup* write_group) {
  assert(leader != nullptr);
  assert(leader->link_older == nullptr);
  assert(leader->batch != nullptr);
  assert(write_group != nullptr);

  size_t size = WriteBatchInternal::ByteSize(leader->batch);

  // Allow the group to grow up to a maximum size, but if the
  // original write is small, limit the growth so we do not slow
  // down the small write too much.
  size_t max_size = 1 << 20;
  if (size <= (128 << 10)) {
    max_size = size + (128 << 10);
  }

  leader->write_group = write_group;
  write_group->leader = leader;
  write_group->size = 1;
  Writer* last_writer = leader;

  if (!allow_concurrent_memtable_write_ || !leader->batch->HasMerge()) {
    Writer* newest_writer = newest_memtable_writer_.load();
    CreateMissingNewerLinks(newest_writer);

    Writer* w = leader;
    while (w != newest_writer) {
      w = w->link_newer;

      if (w->batch == nullptr) {
        break;
      }

      if (w->batch->HasMerge()) {
        break;
      }

      if (!allow_concurrent_memtable_write_) {
        auto batch_size = WriteBatchInternal::ByteSize(w->batch);
        if (size + batch_size > max_size) {
          // Do not make batch too big
          break;
        }
        size += batch_size;
      }

      w->write_group = write_group;
      last_writer = w;
      write_group->size++;
    }
  }

  write_group->last_writer = last_writer;
  write_group->last_sequence =
      last_writer->sequence + WriteBatchInternal::Count(last_writer->batch) - 1;
}

void WriteThread::ExitAsMemTableWriter(Writer* /*self*/,
                                       WriteGroup& write_group) {
  Writer* leader = write_group.leader;
  Writer* last_writer = write_group.last_writer;

  Writer* newest_writer = last_writer;
  if (!newest_memtable_writer_.compare_exchange_strong(newest_writer,
                                                       nullptr)) {
    CreateMissingNewerLinks(newest_writer);
    Writer* next_leader = last_writer->link_newer;
    assert(next_leader != nullptr);
    next_leader->link_older = nullptr;
    SetState(next_leader, STATE_MEMTABLE_WRITER_LEADER);
  }
  Writer* w = leader;
  while (true) {
    if (!write_group.status.ok()) {
      w->status = write_group.status;
    }
    Writer* next = w->link_newer;
    if (w != leader) {
      SetState(w, STATE_COMPLETED);
    }
    if (w == last_writer) {
      break;
    }
    w = next;
  }
  // Note that leader has to exit last, since it owns the write group.
  SetState(leader, STATE_COMPLETED);
}

void WriteThread::LaunchParallelMemTableWriters(WriteGroup* write_group) {
  assert(write_group != nullptr);
  write_group->running.store(write_group->size);
  for (auto w : *write_group) {
    SetState(w, STATE_PARALLEL_MEMTABLE_WRITER);
  }
}

static WriteThread::AdaptationContext cpmtw_ctx("CompleteParallelMemTableWriter");
// This method is called by both the leader and parallel followers
bool WriteThread::CompleteParallelMemTableWriter(Writer* w) {

  auto* write_group = w->write_group;
  if (!w->status.ok()) {
    std::lock_guard<std::mutex> guard(write_group->leader->StateMutex());
    write_group->status = w->status;
  }

  if (write_group->running-- > 1) {
    // we're not the last one
    AwaitState(w, STATE_COMPLETED, &cpmtw_ctx);
    return false;
  }
  // else we're the last parallel worker and should perform exit duties.
  w->status = write_group->status;
  return true;
}

void WriteThread::ExitAsBatchGroupFollower(Writer* w) {
  auto* write_group = w->write_group;

  assert(w->state == STATE_PARALLEL_MEMTABLE_WRITER);
  assert(write_group->status.ok());
  ExitAsBatchGroupLeader(*write_group, write_group->status);
  assert(w->status.ok());
  assert(w->state == STATE_COMPLETED);
  SetState(write_group->leader, STATE_COMPLETED);
}

static WriteThread::AdaptationContext eabgl_ctx("ExitAsBatchGroupLeader");
void WriteThread::ExitAsBatchGroupLeader(WriteGroup& write_group,
                                         Status status) {
  Writer* leader = write_group.leader;
  Writer* last_writer = write_group.last_writer;
  assert(leader->link_older == nullptr);

  // Propagate memtable write error to the whole group.
  if (status.ok() && !write_group.status.ok()) {
    status = write_group.status;
  }

  if (enable_pipelined_write_) {
    // Notify writers don't write to memtable to exit.
    for (Writer* w = last_writer; w != leader;) {
      Writer* next = w->link_older;
      w->status = status;
      if (!w->ShouldWriteToMemtable()) {
        CompleteFollower(w, write_group);
      }
      w = next;
    }
    if (!leader->ShouldWriteToMemtable()) {
      CompleteLeader(write_group);
    }

    Writer* next_leader = nullptr;

    // Look for next leader before we call LinkGroup. If there isn't
    // pending writers, place a dummy writer at the tail of the queue
    // so we know the boundary of the current write group.
    Writer dummy;
    Writer* expected = last_writer;
    bool has_dummy = newest_writer_.compare_exchange_strong(expected, &dummy);
    if (!has_dummy) {
      // We find at least one pending writer when we insert dummy. We search
      // for next leader from there.
      next_leader = FindNextLeader(expected, last_writer);
      assert(next_leader != nullptr && next_leader != last_writer);
    }

    // Link the ramaining of the group to memtable writer list.
    //
    // We have to link our group to memtable writer queue before wake up the
    // next leader or set newest_writer_ to null, otherwise the next leader
    // can run ahead of us and link to memtable writer queue before we do.
    if (write_group.size > 0) {
      if (LinkGroup(write_group, &newest_memtable_writer_)) {
        // The leader can now be different from current writer.
        SetState(write_group.leader, STATE_MEMTABLE_WRITER_LEADER);
      }
    }

    // If we have inserted dummy in the queue, remove it now and check if there
    // are pending writer join the queue since we insert the dummy. If so,
    // look for next leader again.
    if (has_dummy) {
      assert(next_leader == nullptr);
      expected = &dummy;
      bool has_pending_writer =
          !newest_writer_.compare_exchange_strong(expected, nullptr);
      if (has_pending_writer) {
        next_leader = FindNextLeader(expected, &dummy);
        assert(next_leader != nullptr && next_leader != &dummy);
      }
    }

    if (next_leader != nullptr) {
      next_leader->link_older = nullptr;
      SetState(next_leader, STATE_GROUP_LEADER);
    }
    AwaitState(leader, STATE_MEMTABLE_WRITER_LEADER |
                           STATE_PARALLEL_MEMTABLE_WRITER | STATE_COMPLETED,
               &eabgl_ctx);
  } else {
    Writer* head = newest_writer_.load(std::memory_order_acquire);
    if (head != last_writer ||
        !newest_writer_.compare_exchange_strong(head, nullptr)) {
      // Either w wasn't the head during the load(), or it was the head
      // during the load() but somebody else pushed onto the list before
      // we did the compare_exchange_strong (causing it to fail).  In the
      // latter case compare_exchange_strong has the effect of re-reading
      // its first param (head).  No need to retry a failing CAS, because
      // only a departing leader (which we are at the moment) can remove
      // nodes from the list.
      assert(head != last_writer);

      // After walking link_older starting from head (if not already done)
      // we will be able to traverse w->link_newer below. This function
      // can only be called from an active leader, only a leader can
      // clear newest_writer_, we didn't, and only a clear newest_writer_
      // could cause the next leader to start their work without a call
      // to MarkJoined, so we can definitely conclude that no other leader
      // work is going on here (with or without db mutex).
      CreateMissingNewerLinks(head);
      assert(last_writer->link_newer->link_older == last_writer);
      last_writer->link_newer->link_older = nullptr;

      // Next leader didn't self-identify, because newest_writer_ wasn't
      // nullptr when they enqueued (we were definitely enqueued before them
      // and are still in the list).  That means leader handoff occurs when
      // we call MarkJoined
      SetState(last_writer->link_newer, STATE_GROUP_LEADER);
    }
    // else nobody else was waiting, although there might already be a new
    // leader now

    while (last_writer != leader) {
      last_writer->status = status;
      // we need to read link_older before calling SetState, because as soon
      // as it is marked committed the other thread's Await may return and
      // deallocate the Writer.
      auto next = last_writer->link_older;
      SetState(last_writer, STATE_COMPLETED);

      last_writer = next;
    }
  }
}

static WriteThread::AdaptationContext eu_ctx("EnterUnbatched");
void WriteThread::EnterUnbatched(Writer* w, InstrumentedMutex* mu) {
  assert(w != nullptr && w->batch == nullptr);
  mu->Unlock();
  bool linked_as_leader = LinkOne(w, &newest_writer_);
  if (!linked_as_leader) {
    TEST_SYNC_POINT("WriteThread::EnterUnbatched:Wait");
    // Last leader will not pick us as a follower since our batch is nullptr
    AwaitState(w, STATE_GROUP_LEADER, &eu_ctx);
  }
  if (enable_pipelined_write_) {
    WaitForMemTableWriters();
  }
  mu->Lock();
}

void WriteThread::ExitUnbatched(Writer* w) {
  assert(w != nullptr);
  Writer* newest_writer = w;
  if (!newest_writer_.compare_exchange_strong(newest_writer, nullptr)) {
    CreateMissingNewerLinks(newest_writer);
    Writer* next_leader = w->link_newer;
    assert(next_leader != nullptr);
    next_leader->link_older = nullptr;
    SetState(next_leader, STATE_GROUP_LEADER);
  }
}

static WriteThread::AdaptationContext wfmw_ctx("WaitForMemTableWriters");
void WriteThread::WaitForMemTableWriters() {
  assert(enable_pipelined_write_);
  if (newest_memtable_writer_.load() == nullptr) {
    return;
  }
  Writer w;
  if (!LinkOne(&w, &newest_memtable_writer_)) {
    AwaitState(&w, STATE_MEMTABLE_WRITER_LEADER, &wfmw_ctx);
  }
  newest_memtable_writer_.store(nullptr);
}

}  // namespace rocksdb

// Copyright (C) 2026 Zilliz. All rights reserved.
// Licensed under the Apache License, Version 2.0.
#pragma once

#include <exception>
#include <deque>
#include <functional>
#include <optional>
#include <utility>
#include <folly/executors/InlineExecutor.h>
#include <folly/futures/Future.h>

namespace milvus::grep {

// Sequential consumer with five batches of lookahead. The caller must
// release the returned batch before Next(). Native I/O runs on the existing
// storage runtime; submission and reference preparation run on the caller.
template <typename Batch>
class GrepBatchReader {
 public:
    static constexpr size_t kPrefetchBatches = 5;

    using Submit = std::function<std::optional<folly::SemiFuture<Batch>>()>;

    explicit GrepBatchReader(Submit submit) : submit_(std::move(submit)) {
    }
    GrepBatchReader(const GrepBatchReader&) = delete;
    GrepBatchReader&
    operator=(const GrepBatchReader&) = delete;
    ~GrepBatchReader() {
        Close();
    }

    std::optional<Batch>
    Next() {
        while (!stopped_ && pending_.size() < kPrefetchBatches + 1) {
            StartNext();
        }
        if (pending_.empty()) {
            if (error_)
                std::rethrow_exception(std::exchange(error_, nullptr));
            return std::nullopt;
        }
        auto current = std::move(*pending_.front());
        pending_.pop_front();
        return std::move(current).get();
    }

    // Early limit, cancellation and failure all stop submission and drain.
    // Ignore unconsumed speculative errors; preserve the consumer's outcome.
    void
    Close() noexcept {
        stopped_ = true;
        for (auto& batch : pending_) {
            if (batch) {
                try {
                    std::move(*batch).get();
                } catch (...) {
                }
            }
        }
        pending_.clear();
        error_ = nullptr;
    }

 private:
    void
    StartNext() {
        if (stopped_)
            return;
        // Reserve the queue slot before starting I/O so an allocation failure
        // cannot strand an untracked read.
        pending_.emplace_back();
        try {
            auto next = submit_();
            if (!next) {
                pending_.pop_back();
                stopped_ = true;
                return;
            }
            pending_.back().emplace(
                std::move(*next).via(&folly::InlineExecutor::instance()));
        } catch (...) {
            pending_.pop_back();
            // A later batch's preparation error matters only if consumed.
            error_ = std::current_exception();
            stopped_ = true;
        }
    }

    Submit submit_;
    std::deque<std::optional<folly::Future<Batch>>> pending_;
    std::exception_ptr error_;
    bool stopped_ = false;
};
}  // namespace milvus::grep

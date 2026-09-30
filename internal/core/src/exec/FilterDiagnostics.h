// Licensed to the LF AI & Data foundation under one or more contributor
// license agreements. Licensed under the Apache License, Version 2.0.
#pragma once

#include <array>
#include <chrono>
#include <cstdint>
#include <exception>

namespace milvus::exec {

// Request-opt-in diagnostics, not a cost model. One instance belongs to one
// operator/workspace; never share mutable counters between search workers.
// Durations are nested wall times, NOT additive CPU or physical IO times.
// batch_sizes records input lane counts (0..64), not unique visited IDs.
struct FilterDiagnostics {
    uint64_t calls{0}, input_rows{0}, active_rows{0}, accepted_rows{0};
    uint64_t errors{0};
    uint64_t prepare_ns{0}, execute_ns{0}, wait_ns{0}, filter_ns{0};
    uint64_t raw_path_rows{0}, index_path_rows{0};
    uint64_t raw_path_ns{0}, index_path_ns{0};
    // Only the instrumented shared readers contribute here. Includes pin,
    // gather/view/lookup and any page-fault wait there, NOT exclusively disk IO.
    // Borrowed string bytes can fault later inside the predicate: that wait
    // belongs to *_path_ns/callback time, not necessarily *_read_ns.
    uint64_t raw_read_ns{0}, index_read_ns{0};
    uint64_t raw_read_rows{0}, index_read_rows{0};
    std::array<uint64_t, 65> batch_sizes{};
};

// Active only around synchronous Expr evaluation on this thread. Scope restores
// the previous collector, including exceptions and nested evaluations. No
// global atomics, retained query pointers, clocks or allocations when disabled.
inline thread_local FilterDiagnostics* active_filter_diagnostics = nullptr;

class FilterDiagnosticScope {
 public:
    explicit FilterDiagnosticScope(FilterDiagnostics* current)
        : previous_(active_filter_diagnostics) {
        active_filter_diagnostics = current;
    }
    ~FilterDiagnosticScope() {
        active_filter_diagnostics = previous_;
    }
    FilterDiagnosticScope(const FilterDiagnosticScope&) = delete;
    FilterDiagnosticScope&
    operator=(const FilterDiagnosticScope&) = delete;

 private:
    FilterDiagnostics* previous_;
};

class FilterDiagnosticTimer {
 public:
    explicit FilterDiagnosticTimer(uint64_t* total) : total_(total) {
        if (total_) {
            start_ = Clock::now();
        }
    }
    ~FilterDiagnosticTimer() {
        Stop();
    }
    void
    Stop() noexcept {
        if (total_) {
            *total_ += std::chrono::duration_cast<std::chrono::nanoseconds>(
                           Clock::now() - start_)
                           .count();
            total_ = nullptr;
        }
    }
    FilterDiagnosticTimer(const FilterDiagnosticTimer&) = delete;
    FilterDiagnosticTimer&
    operator=(const FilterDiagnosticTimer&) = delete;

 private:
    using Clock = std::chrono::steady_clock;
    uint64_t* total_;
    Clock::time_point start_;
};

// Keep failures visible without intercepting or changing exception semantics.
class FilterDiagnosticFailure {
 public:
    explicit FilterDiagnosticFailure(FilterDiagnostics* stats) : stats_(stats) {
        if (stats_) {
            exceptions_ = std::uncaught_exceptions();
        }
    }
    ~FilterDiagnosticFailure() {
        if (stats_ && std::uncaught_exceptions() > exceptions_) {
            ++stats_->errors;
        }
    }

 private:
    FilterDiagnostics* stats_;
    int exceptions_{0};
};

}  // namespace milvus::exec

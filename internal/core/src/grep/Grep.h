// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
#pragma once

#include <array>
#include <cstddef>
#include <cstdint>
#include <exception>
#include <memory>
#include <string_view>
#include <vector>

struct MgGrepPlan;
struct MgGrepResult;

namespace milvus::grep {

// Private Grep statuses, NOT Milvus/Segcore error codes. Their values are
// checked against the generated ABI in Grep.cpp. Unknown values are preserved.
enum class Status : uint32_t {
    Ok = 0,
    InvalidArgument = 1,
    InvalidPattern = 2,
    InvalidUtf8 = 3,
    ResourceLimit = 4,
    Cancelled = 5,
    InternalError = 7,
    EngineBuildError = 8,
    AllocationError = 9,
};

enum class Operation { Compile, CreateSession, Execute, ReadResult };

// A typed module-local error retaining status and origin without text parsing.
// The future query/cgo adapter MUST translate this before a generic exception
// catch would flatten it. For example, InvalidUtf8 during Compile concerns a
// pattern; during Execute it concerns field data. No retry/blame is assigned here.
class Error : public std::exception {
 public:
    Error(Status status,
          Operation operation,
          std::string_view message) noexcept;
    Status
    status() const noexcept;
    Operation
    operation() const noexcept;
    const char*
    what() const noexcept override;

 private:
    Status status_;
    Operation operation_;
    std::array<char, 512> message_{};
};

enum class PatternKind { Regex, Literal };
enum class CaseMode { Sensitive, Insensitive };
enum class Boundary { None, Word, Line };

struct MatchOptions {
    PatternKind pattern_kind = PatternKind::Regex;
    CaseMode case_mode = CaseMode::Sensitive;
    Boundary boundary = Boundary::None;
};

// Compile limits have no production defaults; callers supply positive budgets.
struct CompileLimits {
    uint64_t max_regex_bytes = 0;
    uint64_t max_dfa_cache_bytes = 0;
};

struct Span {
    uint64_t start;
    uint64_t end;
};

namespace detail {
struct SessionState;
}

class Session;

class Plan {
 public:
    static Plan
    Compile(const std::vector<std::string_view>& patterns,
            const MatchOptions& options,
            const CompileLimits& limits);

    ~Plan();
    Plan(Plan&&) noexcept;
    Plan&
    operator=(Plan&&) noexcept;
    Plan(const Plan&) = delete;
    Plan&
    operator=(const Plan&) = delete;

    // Concurrent creation from an unchanged Plan is allowed. Do not move or
    // destroy this C++ object during a method call. Sessions retain Rust's Plan.
    Session
    CreateSession() const;

 private:
    explicit Plan(MgGrepPlan* handle) noexcept;
    struct Deleter {
        void
        operator()(MgGrepPlan* handle) const noexcept;
    };
    std::unique_ptr<MgGrepPlan, Deleter> handle_;
};

class CancellationToken {
 public:
    CancellationToken() = default;
    // Copyable, sticky, safe to call from an asynchronous callback. This token
    // pins the Rust Session even after the owning C++ Session is destroyed.
    // A default/moved-from token is inert. Cancel never revokes returned results.
    void
    Cancel() const noexcept;

 private:
    friend class Session;
    explicit CancellationToken(std::shared_ptr<detail::SessionState> state);
    std::shared_ptr<detail::SessionState> state_;
};

class Result {
 public:
    ~Result();
    Result(Result&&) noexcept;
    Result&
    operator=(Result&&) noexcept;
    Result(const Result&) = delete;
    Result&
    operator=(const Result&) = delete;

    size_t
    TextCount() const;
    bool
    Selected(size_t text) const;
    size_t
    LineCount(size_t text) const;
    size_t
    SpanCount(size_t text, size_t line) const;
    Span
    GetSpan(size_t text, size_t line, size_t span) const;

    // Borrowed, not NUL terminated. Retain the Result while consuming content.
    // Moving the Result transfers this lifetime without invalidating views.
    // Move assignment invalidates views into the old destination result.
    std::string_view
    Content(size_t text, size_t line) const&;
    std::string_view
    Content(size_t text, size_t line) const&& = delete;

 private:
    friend class Session;
    explicit Result(MgGrepResult* handle) noexcept;
    struct Deleter {
        void
        operator()(MgGrepResult* handle) const noexcept;
    };
    std::unique_ptr<MgGrepResult, Deleter> handle_;
};

class Session {
 public:
    ~Session();
    Session(Session&&) noexcept;
    Session&
    operator=(Session&&) noexcept;
    Session(const Session&) = delete;
    Session&
    operator=(const Session&) = delete;

    CancellationToken
    GetCancellationToken() const;

    // One batch at a time. Inputs are borrowed through return. The owned result
    // survives release of inputs, Session and Plan. No partial success is returned.
    // Discard the Session after an allocation or internal error.
    // Do not move/destroy the Session object while a method on it is running;
    // use a separately copied CancellationToken for concurrent cancellation.
    Result
    ExecuteBatch(const std::vector<std::string_view>& texts,
                 uint64_t max_result_bytes);

 private:
    friend class Plan;
    explicit Session(std::shared_ptr<detail::SessionState> state);
    std::shared_ptr<detail::SessionState> state_;
};

}  // namespace milvus::grep

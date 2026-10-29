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
#include "grep/Grep.h"

#include <algorithm>
#include <cstring>
#include <new>
#include <utility>

#include "milvus_grep.h"

namespace milvus::grep {
namespace {
#define CHECK_STATUS(name, value) \
    static_assert(static_cast<uint32_t>(Status::name) == value)
CHECK_STATUS(Ok, MG_GREP_OK);
CHECK_STATUS(InvalidArgument, MG_GREP_INVALID_ARGUMENT);
CHECK_STATUS(InvalidPattern, MG_GREP_INVALID_PATTERN);
CHECK_STATUS(InvalidUtf8, MG_GREP_INVALID_UTF8);
CHECK_STATUS(ResourceLimit, MG_GREP_RESOURCE_LIMIT);
CHECK_STATUS(Cancelled, MG_GREP_CANCELLED);
CHECK_STATUS(InternalError, MG_GREP_INTERNAL_ERROR);
CHECK_STATUS(EngineBuildError, MG_GREP_ENGINE_BUILD_ERROR);
CHECK_STATUS(AllocationError, MG_GREP_ALLOCATION_ERROR);
#undef CHECK_STATUS

ErrorCode
GrepErrorCode(Status status, Operation operation) {
    switch (status) {
        case Status::InvalidPattern:
            return InvalidParameter;
        case Status::InvalidUtf8:
            return operation == Operation::Compile ? InvalidParameter
                                                   : DataFormatBroken;
        case Status::ResourceLimit:
        case Status::EngineBuildError:
            // Fixed Grep budgets and engine build rejection are request errors.
            // Allocation failures remain distinct system errors below.
            return InvalidParameter;
        case Status::Cancelled:
            return FollyCancel;
        case Status::AllocationError:
            return MemAllocateFailed;
        default:
            // Invalid ABI/spec/state and unknown statuses are internal errors.
            return UnexpectedError;
    }
}

std::string
BoundedDiagnostic(std::string_view message) {
    size_t count = std::min(message.size(), size_t{511});
    while (count && count < message.size() &&
           (static_cast<unsigned char>(message[count]) & 0xc0) == 0x80) {
        --count;
    }
    return std::string(message.substr(0, count));
}

void
CheckStatus(MgGrepStatus status,
            Operation operation,
            const MgGrepError& error) {
    if (status != MG_GREP_OK) {
        const auto* end = static_cast<const char*>(
            std::memchr(error.message, 0, sizeof(error.message)));
        const auto size = end ? static_cast<size_t>(end - error.message)
                              : sizeof(error.message);
        throw GrepError(static_cast<Status>(status),
                        operation,
                        std::string_view(error.message, size));
    }
}

MgGrepMatchOptions
ToOptions(const MatchOptions& options) {
    MgGrepMatchOptions out{};
    switch (options.pattern_kind) {
        case PatternKind::Regex:
            out.pattern_kind = MG_GREP_PATTERN_REGEX;
            break;
        case PatternKind::Literal:
            out.pattern_kind = MG_GREP_PATTERN_LITERAL;
            break;
        default:
            throw GrepError(Status::InvalidArgument,
                            Operation::Compile,
                            "unknown pattern kind");
    }
    switch (options.case_mode) {
        case CaseMode::Sensitive:
            out.case_mode = MG_GREP_CASE_SENSITIVE;
            break;
        case CaseMode::Insensitive:
            out.case_mode = MG_GREP_CASE_INSENSITIVE;
            break;
        default:
            throw GrepError(Status::InvalidArgument,
                            Operation::Compile,
                            "unknown case mode");
    }
    switch (options.boundary) {
        case Boundary::None:
            out.boundary = MG_GREP_BOUNDARY_NONE;
            break;
        case Boundary::Word:
            out.boundary = MG_GREP_BOUNDARY_WORD;
            break;
        case Boundary::Line:
            out.boundary = MG_GREP_BOUNDARY_LINE;
            break;
        default:
            throw GrepError(Status::InvalidArgument,
                            Operation::Compile,
                            "unknown boundary mode");
    }
    return out;
}

std::vector<MgGrepBytes>
ToBytes(const std::vector<std::string_view>& values, Operation operation) {
    std::vector<MgGrepBytes> out;
    if (values.size() > out.max_size()) {
        throw GrepError(Status::ResourceLimit,
                        operation,
                        "descriptor count exceeds addressable capacity");
    }
    try {
        out.reserve(values.size());
        for (auto value : values) {
            out.push_back(
                {reinterpret_cast<const uint8_t*>(value.data()), value.size()});
        }
    } catch (const std::bad_alloc&) {
        throw GrepError(Status::AllocationError,
                        operation,
                        "C++ descriptor allocation failed");
    }
    return out;
}

struct SessionDeleter {
    void
    operator()(MgGrepSession* handle) const noexcept {
        mg_grep_session_free(handle);
    }
};
using SessionHandle = std::unique_ptr<MgGrepSession, SessionDeleter>;

const MgGrepBatchView&
View(const MgGrepResult* handle) {
    if (!handle) {
        throw GrepError(Status::InvalidArgument,
                        Operation::ReadResult,
                        "result has been moved from");
    }
    const auto* view = mg_grep_result_view(handle);
    if (!view || (view->text_count && !view->texts)) {
        throw GrepError(Status::InternalError,
                        Operation::ReadResult,
                        "invalid Grep batch view");
    }
    return *view;
}

const MgGrepTextResult&
Text(const MgGrepResult* handle, size_t text) {
    const auto& view = View(handle);
    if (text >= view.text_count) {
        throw GrepError(Status::InvalidArgument,
                        Operation::ReadResult,
                        "text index out of range");
    }
    const auto& result = view.texts[text];
    if (result.line_count && !result.lines) {
        throw GrepError(Status::InternalError,
                        Operation::ReadResult,
                        "invalid Grep line array");
    }
    return result;
}

const MgGrepLine&
GetLine(const MgGrepResult* handle, size_t text, size_t line) {
    const auto& result = Text(handle, text);
    if (line >= result.line_count) {
        throw GrepError(Status::InvalidArgument,
                        Operation::ReadResult,
                        "line index out of range");
    }
    const auto& value = result.lines[line];
    if ((value.content.len && !value.content.data) || !value.span_count ||
        !value.spans) {
        throw GrepError(Status::InternalError,
                        Operation::ReadResult,
                        "invalid Grep line view");
    }
    return value;
}
}  // namespace

namespace detail {
struct SessionState {
    explicit SessionState(SessionHandle handle) noexcept
        : handle(std::move(handle)) {
    }
    SessionHandle handle;

    void
    Cancel() noexcept {
        mg_grep_session_cancel(handle.get());
    }
};
}  // namespace detail

GrepError::GrepError(Status status,
                     Operation operation,
                     std::string_view message)
    : SegcoreError(GrepErrorCode(status, operation),
                   BoundedDiagnostic(message)),
      status_(status),
      operation_(operation) {
}
Status
GrepError::status() const noexcept {
    return status_;
}
Operation
GrepError::operation() const noexcept {
    return operation_;
}
Plan::Plan(MgGrepPlan* handle) noexcept : handle_(handle) {
}
Plan::~Plan() = default;
Plan::Plan(Plan&&) noexcept = default;
Plan&
Plan::operator=(Plan&&) noexcept = default;
void
Plan::Deleter::operator()(MgGrepPlan* handle) const noexcept {
    mg_grep_plan_free(handle);
}

Plan
Plan::Compile(const std::vector<std::string_view>& patterns,
              const MatchOptions& options,
              const CompileLimits& limits) {
    if (!limits.max_regex_bytes || !limits.max_dfa_cache_bytes ||
        patterns.empty()) {
        throw GrepError(
            Status::InvalidArgument,
            Operation::Compile,
            "positive compile limits and at least one pattern required");
    }
    const auto match_options = ToOptions(options);
    const auto bytes = ToBytes(patterns, Operation::Compile);
    const MgGrepSpecView spec{bytes.data(), bytes.size(), match_options};
    const MgGrepCompileLimits c_limits{limits.max_regex_bytes,
                                       limits.max_dfa_cache_bytes};
    MgGrepError error{};
    MgGrepPlan* handle = nullptr;
    auto status = mg_grep_compile(&spec, &c_limits, &handle, &error);
    Plan result(handle);  // Adopt before any subsequent operation can throw.
    CheckStatus(status, Operation::Compile, error);
    if (!handle) {
        throw GrepError(Status::InternalError,
                        Operation::Compile,
                        "successful compile returned NULL");
    }
    return result;
}

Session
Plan::CreateSession() const {
    if (!handle_) {
        throw GrepError(Status::InvalidArgument,
                        Operation::CreateSession,
                        "plan has been moved from");
    }
    MgGrepError error{};
    MgGrepSession* raw = nullptr;
    auto status = mg_grep_session_create(handle_.get(), &raw, &error);
    SessionHandle handle(raw);
    CheckStatus(status, Operation::CreateSession, error);
    if (!handle) {
        throw GrepError(Status::InternalError,
                        Operation::CreateSession,
                        "successful session creation returned NULL");
    }
    try {
        return Session(
            std::make_shared<detail::SessionState>(std::move(handle)));
    } catch (const std::bad_alloc&) {
        throw GrepError(Status::AllocationError,
                        Operation::CreateSession,
                        "C++ session state allocation failed");
    }
}

CancellationToken::CancellationToken(
    std::shared_ptr<detail::SessionState> state)
    : state_(std::move(state)) {
}
void
CancellationToken::Cancel() const noexcept {
    auto state = state_;
    if (state) {
        state->Cancel();
    }
}

Result::Result(MgGrepResult* handle) noexcept : handle_(handle) {
}
Result::~Result() = default;
Result::Result(Result&&) noexcept = default;
Result&
Result::operator=(Result&&) noexcept = default;
void
Result::Deleter::operator()(MgGrepResult* handle) const noexcept {
    mg_grep_result_free(handle);
}
bool
Result::Selected(size_t text) const {
    return LineCount(text) != 0;
}
size_t
Result::LineCount(size_t text) const {
    return Text(handle_.get(), text).line_count;
}
size_t
Result::SpanCount(size_t text, size_t line) const {
    return GetLine(handle_.get(), text, line).span_count;
}
std::string_view
Result::Content(size_t text, size_t line) const& {
    const auto& bytes = GetLine(handle_.get(), text, line).content;
    return bytes.len ? std::string_view(
                           reinterpret_cast<const char*>(bytes.data), bytes.len)
                     : std::string_view{};
}
Span
Result::GetSpan(size_t text, size_t line, size_t span) const {
    const auto& value = GetLine(handle_.get(), text, line);
    if (span >= value.span_count) {
        throw GrepError(Status::InvalidArgument,
                        Operation::ReadResult,
                        "span index out of range");
    }
    const auto& range = value.spans[span];
    if (range.start > range.end || range.end > value.content.len) {
        throw GrepError(Status::InternalError,
                        Operation::ReadResult,
                        "span outside line content");
    }
    return {range.start, range.end};
}

Session::Session(std::shared_ptr<detail::SessionState> state)
    : state_(std::move(state)) {
}
Session::~Session() = default;
Session::Session(Session&&) noexcept = default;
Session&
Session::operator=(Session&&) noexcept = default;
CancellationToken
Session::GetCancellationToken() const {
    if (!state_) {
        throw GrepError(Status::InvalidArgument,
                        Operation::Execute,
                        "session has been moved from");
    }
    return CancellationToken(state_);
}

Result
Session::ExecuteBatch(const std::vector<std::string_view>& texts,
                      uint64_t max_result_bytes) {
    auto state =
        state_;  // Pins Rust through conversion, execution and final checks.
    if (!state) {
        throw GrepError(Status::InvalidArgument,
                        Operation::Execute,
                        "session has been moved from");
    }
    if (!max_result_bytes) {
        throw GrepError(Status::InvalidArgument,
                        Operation::Execute,
                        "result byte budget must be positive");
    }
    const auto bytes = ToBytes(texts, Operation::Execute);
    MgGrepError error{};
    MgGrepResult* raw = nullptr;
    const auto status = mg_grep_execute_batch(state->handle.get(),
                                              bytes.data(),
                                              bytes.size(),
                                              max_result_bytes,
                                              &raw,
                                              &error);
    Result result(raw);
    CheckStatus(status, Operation::Execute, error);
    const auto* view = mg_grep_result_view(raw);
    if (!view || view->text_count != texts.size() ||
        (view->text_count && !view->texts)) {
        throw GrepError(Status::InternalError,
                        Operation::Execute,
                        "successful execution returned an invalid batch");
    }
    return result;
}

}  // namespace milvus::grep

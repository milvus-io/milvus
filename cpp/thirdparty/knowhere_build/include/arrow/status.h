// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.
//
// A Status encapsulates the result of an operation.  It may indicate success,
// or it may indicate an error with an associated error message.
//
// Multiple threads can invoke const methods on a Status without
// external synchronization, but if any of the threads may call a
// non-const method, all threads accessing the same Status must use
// external synchronization.

// Adapted from Apache Kudu, TensorFlow

#ifndef ARROW_STATUS_H_
#define ARROW_STATUS_H_

#include <cstring>
#include <iosfwd>
#include <string>
#include <utility>

#include "arrow/util/macros.h"
#include "arrow/util/string_builder.h"
#include "arrow/util/visibility.h"

#ifdef ARROW_EXTRA_ERROR_CONTEXT

/// \brief Return with given status if condition is met.
#define ARROW_RETURN_IF_(condition, status, expr)   \
  do {                                              \
    if (ARROW_PREDICT_FALSE(condition)) {           \
      ::arrow::Status _st = (status);               \
      _st.AddContextLine(__FILE__, __LINE__, expr); \
      return _st;                                   \
    }                                               \
  } while (0)

#else

#define ARROW_RETURN_IF_(condition, status, _) \
  do {                                         \
    if (ARROW_PREDICT_FALSE(condition)) {      \
      return (status);                         \
    }                                          \
  } while (0)

#endif  // ARROW_EXTRA_ERROR_CONTEXT

#define ARROW_RETURN_IF(condition, status) \
  ARROW_RETURN_IF_(condition, status, ARROW_STRINGIFY(status))

/// \brief Propagate any non-successful Status to the caller
#define ARROW_RETURN_NOT_OK(status)                            \
  do {                                                         \
    ::arrow::Status __s = (status);                            \
    ARROW_RETURN_IF_(!__s.ok(), __s, ARROW_STRINGIFY(status)); \
  } while (false)

#define RETURN_NOT_OK_ELSE(s, else_) \
  do {                               \
    ::arrow::Status _s = (s);        \
    if (!_s.ok()) {                  \
      else_;                         \
      return _s;                     \
    }                                \
  } while (false)

// This is an internal-use macro and should not be used in public headers.
#ifndef RETURN_NOT_OK
#define RETURN_NOT_OK(s) ARROW_RETURN_NOT_OK(s)
#endif

namespace arrow {

enum class StatusCode : char {
  OK = 0,
  OutOfMemory = 1,
  KeyError = 2,
  TypeError = 3,
  Invalid = 4,
  IOError = 5,
  CapacityError = 6,
  IndexError = 7,
  UnknownError = 9,
  NotImplemented = 10,
  SerializationError = 11,
  PythonError = 12,
  RError = 13,
  PlasmaObjectExists = 20,
  PlasmaObjectNonexistent = 21,
  PlasmaStoreFull = 22,
  PlasmaObjectAlreadySealed = 23,
  StillExecuting = 24,
  // Gandiva range of errors
  CodeGenError = 40,
  ExpressionValidationError = 41,
  ExecutionError = 42
};

#if defined(__clang__)
// Only clang supports warn_unused_result as a type annotation.
class ARROW_MUST_USE_RESULT ARROW_EXPORT Status;
#endif

/// \brief Status outcome object (success or error)
///
/// The Status object is an object holding the outcome of an operation.
/// The outcome is represented as a StatusCode, either success
/// (StatusCode::OK) or an error (any other of the StatusCode enumeration values).
///
/// Additionally, if an error occurred, a specific error message is generally
/// attached.
class ARROW_EXPORT Status {
 public:
  // Create a success status.
  Status() noexcept : state_(NULLPTR) {}
  ~Status() noexcept {
    // ARROW-2400: On certain compilers, splitting off the slow path improves
    // performance significantly.
    if (ARROW_PREDICT_FALSE(state_ != NULL)) {
      DeleteState();
    }
  }

  Status(StatusCode code, const std::string& msg);

  // Copy the specified status.
  inline Status(const Status& s);
  inline Status& operator=(const Status& s);

  // Move the specified status.
  inline Status(Status&& s) noexcept;
  inline Status& operator=(Status&& s) noexcept;

  // AND the statuses.
  inline Status operator&(const Status& s) const noexcept;
  inline Status operator&(Status&& s) const noexcept;
  inline Status& operator&=(const Status& s) noexcept;
  inline Status& operator&=(Status&& s) noexcept;

  /// Return a success status
  static Status OK() { return Status(); }

  /// Return a success status with a specific message
  template <typename... Args>
  static Status OK(Args&&... args) {
    return Status(StatusCode::OK, util::StringBuilder(std::forward<Args>(args)...));
  }

  /// Return an error status for out-of-memory conditions
  template <typename... Args>
  static Status OutOfMemory(Args&&... args) {
    return Status(StatusCode::OutOfMemory,
                  util::StringBuilder(std::forward<Args>(args)...));
  }

  /// Return an error status for failed key lookups (e.g. column name in a table)
  template <typename... Args>
  static Status KeyError(Args&&... args) {
    return Status(StatusCode::KeyError, util::StringBuilder(std::forward<Args>(args)...));
  }

  /// Return an error status for type errors (such as mismatching data types)
  template <typename... Args>
  static Status TypeError(Args&&... args) {
    return Status(StatusCode::TypeError,
                  util::StringBuilder(std::forward<Args>(args)...));
  }

  /// Return an error status for unknown errors
  template <typename... Args>
  static Status UnknownError(Args&&... args) {
    return Status(StatusCode::UnknownError,
                  util::StringBuilder(std::forward<Args>(args)...));
  }

  /// Return an error status when an operation or a combination of operation and
  /// data types is unimplemented
  template <typename... Args>
  static Status NotImplemented(Args&&... args) {
    return Status(StatusCode::NotImplemented,
                  util::StringBuilder(std::forward<Args>(args)...));
  }

  /// Return an error status for invalid data (for example a string that fails parsing)
  template <typename... Args>
  static Status Invalid(Args&&... args) {
    return Status(StatusCode::Invalid, util::StringBuilder(std::forward<Args>(args)...));
  }

  /// Return an error status when an index is out of bounds
  template <typename... Args>
  static Status IndexError(Args&&... args) {
    return Status(StatusCode::IndexError,
                  util::StringBuilder(std::forward<Args>(args)...));
  }

  /// Return an error status when a container's capacity would exceed its limits
  template <typename... Args>
  static Status CapacityError(Args&&... args) {
    return Status(StatusCode::CapacityError,
                  util::StringBuilder(std::forward<Args>(args)...));
  }

  /// Return an error status when some IO-related operation failed
  template <typename... Args>
  static Status IOError(Args&&... args) {
    return Status(StatusCode::IOError, util::StringBuilder(std::forward<Args>(args)...));
  }

  /// Return an error status when some (de)serialization operation failed
  template <typename... Args>
  static Status SerializationError(Args&&... args) {
    return Status(StatusCode::SerializationError,
                  util::StringBuilder(std::forward<Args>(args)...));
  }

  template <typename... Args>
  static Status RError(Args&&... args) {
    return Status(StatusCode::RError, util::StringBuilder(std::forward<Args>(args)...));
  }

  template <typename... Args>
  static Status PlasmaObjectExists(Args&&... args) {
    return Status(StatusCode::PlasmaObjectExists,
                  util::StringBuilder(std::forward<Args>(args)...));
  }

  template <typename... Args>
  static Status PlasmaObjectNonexistent(Args&&... args) {
    return Status(StatusCode::PlasmaObjectNonexistent,
                  util::StringBuilder(std::forward<Args>(args)...));
  }

  template <typename... Args>
  static Status PlasmaObjectAlreadySealed(Args&&... args) {
    return Status(StatusCode::PlasmaObjectAlreadySealed,
                  util::StringBuilder(std::forward<Args>(args)...));
  }

  template <typename... Args>
  static Status PlasmaStoreFull(Args&&... args) {
    return Status(StatusCode::PlasmaStoreFull,
                  util::StringBuilder(std::forward<Args>(args)...));
  }

  static Status StillExecuting() { return Status(StatusCode::StillExecuting, ""); }

  template <typename... Args>
  static Status CodeGenError(Args&&... args) {
    return Status(StatusCode::CodeGenError,
                  util::StringBuilder(std::forward<Args>(args)...));
  }

  template <typename... Args>
  static Status ExpressionValidationError(Args&&... args) {
    return Status(StatusCode::ExpressionValidationError,
                  util::StringBuilder(std::forward<Args>(args)...));
  }

  template <typename... Args>
  static Status ExecutionError(Args&&... args) {
    return Status(StatusCode::ExecutionError,
                  util::StringBuilder(std::forward<Args>(args)...));
  }

  /// Return true iff the status indicates success.
  bool ok() const { return (state_ == NULLPTR); }

  /// Return true iff the status indicates an out-of-memory error.
  bool IsOutOfMemory() const { return code() == StatusCode::OutOfMemory; }
  /// Return true iff the status indicates a key lookup error.
  bool IsKeyError() const { return code() == StatusCode::KeyError; }
  /// Return true iff the status indicates invalid data.
  bool IsInvalid() const { return code() == StatusCode::Invalid; }
  /// Return true iff the status indicates an IO-related failure.
  bool IsIOError() const { return code() == StatusCode::IOError; }
  /// Return true iff the status indicates a container reaching capacity limits.
  bool IsCapacityError() const { return code() == StatusCode::CapacityError; }
  /// Return true iff the status indicates an out of bounds index.
  bool IsIndexError() const { return code() == StatusCode::IndexError; }
  /// Return true iff the status indicates a type error.
  bool IsTypeError() const { return code() == StatusCode::TypeError; }
  /// Return true iff the status indicates an unknown error.
  bool IsUnknownError() const { return code() == StatusCode::UnknownError; }
  /// Return true iff the status indicates an unimplemented operation.
  bool IsNotImplemented() const { return code() == StatusCode::NotImplemented; }
  /// Return true iff the status indicates a (de)serialization failure
  bool IsSerializationError() const { return code() == StatusCode::SerializationError; }
  /// Return true iff the status indicates a R-originated error.
  bool IsRError() const { return code() == StatusCode::RError; }
  /// Return true iff the status indicates a Python-originated error.
  bool IsPythonError() const { return code() == StatusCode::PythonError; }
  /// Return true iff the status indicates an already existing Plasma object.
  bool IsPlasmaObjectExists() const { return code() == StatusCode::PlasmaObjectExists; }
  /// Return true iff the status indicates a non-existent Plasma object.
  bool IsPlasmaObjectNonexistent() const {
    return code() == StatusCode::PlasmaObjectNonexistent;
  }
  /// Return true iff the status indicates an already sealed Plasma object.
  bool IsPlasmaObjectAlreadySealed() const {
    return code() == StatusCode::PlasmaObjectAlreadySealed;
  }
  /// Return true iff the status indicates the Plasma store reached its capacity limit.
  bool IsPlasmaStoreFull() const { return code() == StatusCode::PlasmaStoreFull; }

  bool IsStillExecuting() const { return code() == StatusCode::StillExecuting; }

  bool IsCodeGenError() const { return code() == StatusCode::CodeGenError; }

  bool IsExpressionValidationError() const {
    return code() == StatusCode::ExpressionValidationError;
  }

  bool IsExecutionError() const { return code() == StatusCode::ExecutionError; }

  /// \brief Return a string representation of this status suitable for printing.
  ///
  /// The string "OK" is returned for success.
  std::string ToString() const;

  /// \brief Return a string representation of the status code, without the message
  /// text or POSIX code information.
  std::string CodeAsString() const;

  /// \brief Return the StatusCode value attached to this status.
  StatusCode code() const { return ok() ? StatusCode::OK : state_->code; }

  /// \brief Return the specific error message attached to this status.
  std::string message() const { return ok() ? "" : state_->msg; }

  [[noreturn]] void Abort() const;
  [[noreturn]] void Abort(const std::string& message) const;

#ifdef ARROW_EXTRA_ERROR_CONTEXT
  void AddContextLine(const char* filename, int line, const char* expr);
#endif

 private:
  struct State {
    StatusCode code;
    std::string msg;
  };
  // OK status has a `NULL` state_.  Otherwise, `state_` points to
  // a `State` structure containing the error code and message(s)
  State* state_;

  void DeleteState() {
    delete state_;
    state_ = NULLPTR;
  }
  void CopyFrom(const Status& s);
  inline void MoveFrom(Status& s);
};

static inline std::ostream& operator<<(std::ostream& os, const Status& x) {
  os << x.ToString();
  return os;
}

void Status::MoveFrom(Status& s) {
  delete state_;
  state_ = s.state_;
  s.state_ = NULLPTR;
}

Status::Status(const Status& s)
    : state_((s.state_ == NULLPTR) ? NULLPTR : new State(*s.state_)) {}

Status& Status::operator=(const Status& s) {
  // The following condition catches both aliasing (when this == &s),
  // and the common case where both s and *this are ok.
  if (state_ != s.state_) {
    CopyFrom(s);
  }
  return *this;
}

Status::Status(Status&& s) noexcept : state_(s.state_) { s.state_ = NULLPTR; }

Status& Status::operator=(Status&& s) noexcept {
  MoveFrom(s);
  return *this;
}

/// \cond FALSE
// (note: emits warnings on Doxygen < 1.8.15,
//  see https://github.com/doxygen/doxygen/issues/6295)
Status Status::operator&(const Status& s) const noexcept {
  if (ok()) {
    return s;
  } else {
    return *this;
  }
}

Status Status::operator&(Status&& s) const noexcept {
  if (ok()) {
    return std::move(s);
  } else {
    return *this;
  }
}

Status& Status::operator&=(const Status& s) noexcept {
  if (ok() && !s.ok()) {
    CopyFrom(s);
  }
  return *this;
}

Status& Status::operator&=(Status&& s) noexcept {
  if (ok() && !s.ok()) {
    MoveFrom(s);
  }
  return *this;
}
/// \endcond

}  // namespace arrow

#endif  // ARROW_STATUS_H_

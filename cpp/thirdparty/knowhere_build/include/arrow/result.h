//
// Copyright 2017 Asylo authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//

// Adapted from Asylo

#pragma once

#include <string>
#include <utility>

#include "arrow/status.h"
#include "arrow/util/macros.h"
#include "arrow/util/variant.h"

namespace arrow {

namespace internal {

ARROW_EXPORT void DieWithMessage(const std::string& msg);

}  // namespace internal

// A class for representing either a usable value, or an error.
///
/// A Result object either contains a value of type `T` or a Status object
/// explaining why such a value is not present. The type `T` must be
/// copy-constructible and/or move-constructible.
///
/// The state of a Result object may be determined by calling ok() or
/// status(). The ok() method returns true if the object contains a valid value.
/// The status() method returns the internal Status object. A Result object
/// that contains a valid value will return an OK Status for a call to status().
///
/// A value of type `T` may be extracted from a Result object through a call
/// to ValueOrDie(). This function should only be called if a call to ok()
/// returns true. Sample usage:
///
/// ```
///   arrow::Result<Foo> result = CalculateFoo();
///   if (result.ok()) {
///     Foo foo = result.ValueOrDie();
///     foo.DoSomethingCool();
///   } else {
///     ARROW_LOG(ERROR) << result.status();
///  }
/// ```
///
/// If `T` is a move-only type, like `std::unique_ptr<>`, then the value should
/// only be extracted after invoking `std::move()` on the Result object.
/// Sample usage:
///
/// ```
///   arrow::Result<std::unique_ptr<Foo>> result = CalculateFoo();
///   if (result.ok()) {
///     std::unique_ptr<Foo> foo = std::move(result).ValueOrDie();
///     foo->DoSomethingCool();
///   } else {
///     ARROW_LOG(ERROR) << result.status();
///   }
/// ```
///
/// Result is provided for the convenience of implementing functions that
/// return some value but may fail during execution. For instance, consider a
/// function with the following signature:
///
/// ```
///   arrow::Status CalculateFoo(int *output);
/// ```
///
/// This function may instead be written as:
///
/// ```
///   arrow::Result<int> CalculateFoo();
/// ```
template <class T>
class Result {
  template <typename U>
  friend class Result;
  using VariantType = arrow::util::variant<T, Status, const char*>;
  static_assert(!std::is_same<T, Status>::value,
                "this assert indicates you have probably made a metaprogramming error");

 public:
  /// Constructs a Result object that contains a non-OK status.
  ///
  /// This constructor is marked `explicit` to prevent attempts to `return {}`
  /// from a function with a return type of, for example,
  /// `Result<std::vector<int>>`. While `return {}` seems like it would return
  /// an empty vector, it will actually invoke the default constructor of
  /// Result.
  explicit Result()  // NOLINT(runtime/explicit)
      : variant_(Status::UnknownError("Unknown error")) {}

  ~Result() = default;

  /// Constructs a Result object with the given non-OK Status object. All
  /// calls to ValueOrDie() on this object will abort. The given `status` must
  /// not be an OK status, otherwise this constructor will abort.
  ///
  /// This constructor is not declared explicit so that a function with a return
  /// type of `Result<T>` can return a Status object, and the status will be
  /// implicitly converted to the appropriate return type as a matter of
  /// convenience.
  ///
  /// \param status The non-OK Status object to initalize to.
  Result(const Status& status)  // NOLINT(runtime/explicit)
      : variant_(status) {
    if (ARROW_PREDICT_FALSE(status.ok())) {
      internal::DieWithMessage(std::string("Constructed with a non-error status: ") +
                               status.ToString());
    }
  }

  /// Constructs a Result object that contains `value`. The resulting object
  /// is considered to have an OK status. The wrapped element can be accessed
  /// with ValueOrDie().
  ///
  /// This constructor is made implicit so that a function with a return type of
  /// `Result<T>` can return an object of type `U &&`, implicitly converting
  /// it to a `Result<T>` object.
  ///
  /// Note that `T` must be implicitly constructible from `U`, and `U` must not
  /// be a (cv-qualified) Status or Status-reference type. Due to C++
  /// reference-collapsing rules and perfect-forwarding semantics, this
  /// constructor matches invocations that pass `value` either as a const
  /// reference or as an rvalue reference. Since Result needs to work for both
  /// reference and rvalue-reference types, the constructor uses perfect
  /// forwarding to avoid invalidating arguments that were passed by reference.
  /// See http://thbecker.net/articles/rvalue_references/section_08.html for
  /// additional details.
  ///
  /// \param value The value to initialize to.
  template <typename U,
            typename E = typename std::enable_if<
                std::is_constructible<T, U>::value && std::is_convertible<U, T>::value &&
                !std::is_same<typename std::remove_reference<
                                  typename std::remove_cv<U>::type>::type,
                              Status>::value>::type>
  Result(U&& value)  // NOLINT(runtime/explicit)
      : variant_(std::forward<U>(value)) {}

  /// Copy constructor.
  ///
  /// This constructor needs to be explicitly defined because the presence of
  /// the move-assignment operator deletes the default copy constructor. In such
  /// a scenario, since the deleted copy constructor has stricter binding rules
  /// than the templated copy constructor, the templated constructor cannot act
  /// as a copy constructor, and any attempt to copy-construct a `Result`
  /// object results in a compilation error.
  ///
  /// \param other The value to copy from.
  Result(const Result& other) = default;

  /// Templatized constructor that constructs a `Result<T>` from a const
  /// reference to a `Result<U>`.
  ///
  /// `T` must be implicitly constructible from `const U &`.
  ///
  /// \param other The value to copy from.
  template <typename U,
            typename E = typename std::enable_if<std::is_constructible<T, U>::value &&
                                                 std::is_convertible<U, T>::value>::type>
  Result(const Result<U>& other) : variant_("unitialized") {
    AssignVariant(other.variant_);
  }

  /// Copy-assignment operator.
  ///
  /// \param other The Result object to copy.
  Result& operator=(const Result& other) = default;

  /// Templatized constructor which constructs a `Result<T>` by moving the
  /// contents of a `Result<U>`. `T` must be implicitly constructible from `U
  /// &&`.
  ///
  /// Sets `other` to contain a non-OK status with a`StatusError::Invalid`
  /// error code.
  ///
  /// \param other The Result object to move from and set to a non-OK status.
  template <typename U,
            typename E = typename std::enable_if<std::is_constructible<T, U>::value &&
                                                 std::is_convertible<U, T>::value>::type>
  Result(Result<U>&& other) : variant_("unitialized") {
    AssignVariant(std::move(other.variant_));
    other.variant_ = "Value was moved to another Result.";
  }

  /// Move-assignment operator.
  ///
  /// Sets `other` to an invalid state..
  ///
  /// \param other The Result object to assign from and set to a non-OK
  /// status.
  Result& operator=(Result&& other) {
    // Check for self-assignment.
    if (this == &other) {
      return *this;
    }
    AssignVariant(std::move(other.variant_));
    other.variant_ = "Value was moved to another Result.";

    return *this;
  }

  /// Indicates whether the object contains a `T` value.  Generally instead
  /// of accessing this directly you will want to use ASSIGN_OR_RAISE defined
  /// below.
  ///
  /// \return True if this Result object's status is OK (i.e. a call to ok()
  /// returns true). If this function returns true, then it is safe to access
  /// the wrapped element through a call to ValueOrDie().
  bool ok() const { return arrow::util::holds_alternative<T>(variant_); }

  /// \brief Equivelant to ok().
  // operator bool() const { return ok(); }

  /// Gets the stored status object, or an OK status if a `T` value is stored.
  ///
  /// \return The stored non-OK status object, or an OK status if this object
  ///         has a value.
  Status status() const {
    return ok() ? Status::OK() : arrow::util::get<Status>(variant_);
  }

  /// Gets the stored `T` value.
  ///
  /// This method should only be called if this Result object's status is OK
  /// (i.e. a call to ok() returns true), otherwise this call will abort.
  ///
  /// \return The stored `T` value.
  const T& ValueOrDie() const& {
    if (ARROW_PREDICT_FALSE(!ok())) {
      internal::DieWithMessage(std::string("ValueOrDie called on an error: ") +
                               status().ToString());
    }
    return arrow::util::get<T>(variant_);
  }
  const T& operator*() const& { return ValueOrDie(); }

  /// Gets a mutable reference to the stored `T` value.
  ///
  /// This method should only be called if this Result object's status is OK
  /// (i.e. a call to ok() returns true), otherwise this call will abort.
  ///
  /// \return The stored `T` value.
  T& ValueOrDie() & {
    if (ARROW_PREDICT_FALSE(!ok())) {
      internal::DieWithMessage(std::string("ValueOrDie called on an error: ") +
                               status().ToString());
    }
    return arrow::util::get<T>(variant_);
  }
  T& operator*() & { return ValueOrDie(); }

  /// Moves and returns the internally-stored `T` value.
  ///
  /// This method should only be called if this Result object's status is OK
  /// (i.e. a call to ok() returns true), otherwise this call will abort. The
  /// Result object is invalidated after this call and will be updated to
  /// contain a non-OK status.
  ///
  /// \return The stored `T` value.
  T ValueOrDie() && {
    if (ARROW_PREDICT_FALSE(!ok())) {
      internal::DieWithMessage(std::string("ValueOrDie called on an error: ") +
                               status().ToString());
    }
    T tmp(std::move(arrow::util::get<T>(variant_)));
    variant_ = "Object already returned with ValueOrDie";
    return tmp;
  }
  T operator*() && { return ValueOrDie(); }

 private:
  // Assignment is disabled by default so we need to destruct/reconstruct
  // the value.
  template <typename U>
  void AssignVariant(arrow::util::variant<U, Status, const char*>&& other) {
    variant_.~variant();
    if (arrow::util::holds_alternative<U>(other)) {
      // Reuse memory of variant_ for construction
      new (&variant_) VariantType(arrow::util::get<U>(std::move(other)));
    } else {
      new (&variant_) VariantType(arrow::util::get<Status>(std::move(other)));
    }
  }

  // Assignment is disabled by default so we need to destruct/reconstruct
  // the value.
  template <typename U>
  void AssignVariant(const arrow::util::variant<U, Status, const char*>& other) {
    variant_.~variant();
    if (arrow::util::holds_alternative<U>(other)) {
      // Reuse memory of variant_ for construction
      new (&variant_) VariantType(arrow::util::get<U>(other));
    } else {
      new (&variant_) VariantType(arrow::util::get<Status>(other));
    }
  }

  arrow::util::variant<T, Status, const char*> variant_;
};

#define ARROW_ASSIGN_OR_RAISE_IMPL(status_name, lhs, rexpr) \
  auto status_name = (rexpr);                               \
  ARROW_RETURN_NOT_OK(status_name.status());                \
  lhs = std::move(status_name).ValueOrDie();

#define ARROW_ASSIGN_OR_RAISE_NAME(x, y) ARROW_CONCAT(x, y)

// Executes an expression that returns a Result, extracting its value
// into the variable defined by lhs (or returning on error).
//
// Example: Assigning to an existing value
//   ValueType value;
//   ARROW_ASSIGN_OR_RAISE(value, MaybeGetValue(arg));
//
// WARNING: ASSIGN_OR_RAISE expands into multiple statements; it cannot be used
//  in a single statement (e.g. as the body of an if statement without {})!
#define ARROW_ASSIGN_OR_RAISE(lhs, rexpr)                                              \
  ARROW_ASSIGN_OR_RAISE_IMPL(ARROW_ASSIGN_OR_RAISE_NAME(_error_or_value, __COUNTER__), \
                             lhs, rexpr);

namespace internal {

// For Cython, because of https://github.com/cython/cython/issues/3018
template <class T>
using CResult = Result<T>;

}  // namespace internal
}  // namespace arrow

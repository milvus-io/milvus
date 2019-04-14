// Copyright 2007, Google Inc.
// All rights reserved.
//
// Redistribution and use in source and binary forms, with or without
// modification, are permitted provided that the following conditions are
// met:
//
//     * Redistributions of source code must retain the above copyright
// notice, this list of conditions and the following disclaimer.
//     * Redistributions in binary form must reproduce the above
// copyright notice, this list of conditions and the following disclaimer
// in the documentation and/or other materials provided with the
// distribution.
//     * Neither the name of Google Inc. nor the names of its
// contributors may be used to endorse or promote products derived from
// this software without specific prior written permission.
//
// THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
// "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
// LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
// A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT
// OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
// SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
// LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
// DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY
// THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
// (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
// OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
//
// Author: wan@google.com (Zhanyong Wan)

// Google Mock - a framework for writing C++ mock classes.
//
// This file tests the built-in actions.

#include "gmock/gmock-actions.h"
#include <algorithm>
#include <iterator>
#include <memory>
#include <string>
#include "gmock/gmock.h"
#include "gmock/internal/gmock-port.h"
#include "gtest/gtest.h"
#include "gtest/gtest-spi.h"

namespace {

// This list should be kept sorted.
using testing::Action;
using testing::ActionInterface;
using testing::Assign;
using testing::ByMove;
using testing::ByRef;
using testing::DefaultValue;
using testing::DoDefault;
using testing::IgnoreResult;
using testing::Invoke;
using testing::InvokeWithoutArgs;
using testing::MakePolymorphicAction;
using testing::Ne;
using testing::PolymorphicAction;
using testing::Return;
using testing::ReturnNull;
using testing::ReturnRef;
using testing::ReturnRefOfCopy;
using testing::SetArgPointee;
using testing::SetArgumentPointee;
using testing::_;
using testing::get;
using testing::internal::BuiltInDefaultValue;
using testing::internal::Int64;
using testing::internal::UInt64;
using testing::make_tuple;
using testing::tuple;
using testing::tuple_element;

#if !GTEST_OS_WINDOWS_MOBILE
using testing::SetErrnoAndReturn;
#endif

#if GTEST_HAS_PROTOBUF_
using testing::internal::TestMessage;
#endif  // GTEST_HAS_PROTOBUF_

// Tests that BuiltInDefaultValue<T*>::Get() returns NULL.
TEST(BuiltInDefaultValueTest, IsNullForPointerTypes) {
  EXPECT_TRUE(BuiltInDefaultValue<int*>::Get() == NULL);
  EXPECT_TRUE(BuiltInDefaultValue<const char*>::Get() == NULL);
  EXPECT_TRUE(BuiltInDefaultValue<void*>::Get() == NULL);
}

// Tests that BuiltInDefaultValue<T*>::Exists() return true.
TEST(BuiltInDefaultValueTest, ExistsForPointerTypes) {
  EXPECT_TRUE(BuiltInDefaultValue<int*>::Exists());
  EXPECT_TRUE(BuiltInDefaultValue<const char*>::Exists());
  EXPECT_TRUE(BuiltInDefaultValue<void*>::Exists());
}

// Tests that BuiltInDefaultValue<T>::Get() returns 0 when T is a
// built-in numeric type.
TEST(BuiltInDefaultValueTest, IsZeroForNumericTypes) {
  EXPECT_EQ(0U, BuiltInDefaultValue<unsigned char>::Get());
  EXPECT_EQ(0, BuiltInDefaultValue<signed char>::Get());
  EXPECT_EQ(0, BuiltInDefaultValue<char>::Get());
#if GMOCK_HAS_SIGNED_WCHAR_T_
  EXPECT_EQ(0U, BuiltInDefaultValue<unsigned wchar_t>::Get());
  EXPECT_EQ(0, BuiltInDefaultValue<signed wchar_t>::Get());
#endif
#if GMOCK_WCHAR_T_IS_NATIVE_
  EXPECT_EQ(0, BuiltInDefaultValue<wchar_t>::Get());
#endif
  EXPECT_EQ(0U, BuiltInDefaultValue<unsigned short>::Get());  // NOLINT
  EXPECT_EQ(0, BuiltInDefaultValue<signed short>::Get());  // NOLINT
  EXPECT_EQ(0, BuiltInDefaultValue<short>::Get());  // NOLINT
  EXPECT_EQ(0U, BuiltInDefaultValue<unsigned int>::Get());
  EXPECT_EQ(0, BuiltInDefaultValue<signed int>::Get());
  EXPECT_EQ(0, BuiltInDefaultValue<int>::Get());
  EXPECT_EQ(0U, BuiltInDefaultValue<unsigned long>::Get());  // NOLINT
  EXPECT_EQ(0, BuiltInDefaultValue<signed long>::Get());  // NOLINT
  EXPECT_EQ(0, BuiltInDefaultValue<long>::Get());  // NOLINT
  EXPECT_EQ(0U, BuiltInDefaultValue<UInt64>::Get());
  EXPECT_EQ(0, BuiltInDefaultValue<Int64>::Get());
  EXPECT_EQ(0, BuiltInDefaultValue<float>::Get());
  EXPECT_EQ(0, BuiltInDefaultValue<double>::Get());
}

// Tests that BuiltInDefaultValue<T>::Exists() returns true when T is a
// built-in numeric type.
TEST(BuiltInDefaultValueTest, ExistsForNumericTypes) {
  EXPECT_TRUE(BuiltInDefaultValue<unsigned char>::Exists());
  EXPECT_TRUE(BuiltInDefaultValue<signed char>::Exists());
  EXPECT_TRUE(BuiltInDefaultValue<char>::Exists());
#if GMOCK_HAS_SIGNED_WCHAR_T_
  EXPECT_TRUE(BuiltInDefaultValue<unsigned wchar_t>::Exists());
  EXPECT_TRUE(BuiltInDefaultValue<signed wchar_t>::Exists());
#endif
#if GMOCK_WCHAR_T_IS_NATIVE_
  EXPECT_TRUE(BuiltInDefaultValue<wchar_t>::Exists());
#endif
  EXPECT_TRUE(BuiltInDefaultValue<unsigned short>::Exists());  // NOLINT
  EXPECT_TRUE(BuiltInDefaultValue<signed short>::Exists());  // NOLINT
  EXPECT_TRUE(BuiltInDefaultValue<short>::Exists());  // NOLINT
  EXPECT_TRUE(BuiltInDefaultValue<unsigned int>::Exists());
  EXPECT_TRUE(BuiltInDefaultValue<signed int>::Exists());
  EXPECT_TRUE(BuiltInDefaultValue<int>::Exists());
  EXPECT_TRUE(BuiltInDefaultValue<unsigned long>::Exists());  // NOLINT
  EXPECT_TRUE(BuiltInDefaultValue<signed long>::Exists());  // NOLINT
  EXPECT_TRUE(BuiltInDefaultValue<long>::Exists());  // NOLINT
  EXPECT_TRUE(BuiltInDefaultValue<UInt64>::Exists());
  EXPECT_TRUE(BuiltInDefaultValue<Int64>::Exists());
  EXPECT_TRUE(BuiltInDefaultValue<float>::Exists());
  EXPECT_TRUE(BuiltInDefaultValue<double>::Exists());
}

// Tests that BuiltInDefaultValue<bool>::Get() returns false.
TEST(BuiltInDefaultValueTest, IsFalseForBool) {
  EXPECT_FALSE(BuiltInDefaultValue<bool>::Get());
}

// Tests that BuiltInDefaultValue<bool>::Exists() returns true.
TEST(BuiltInDefaultValueTest, BoolExists) {
  EXPECT_TRUE(BuiltInDefaultValue<bool>::Exists());
}

// Tests that BuiltInDefaultValue<T>::Get() returns "" when T is a
// string type.
TEST(BuiltInDefaultValueTest, IsEmptyStringForString) {
#if GTEST_HAS_GLOBAL_STRING
  EXPECT_EQ("", BuiltInDefaultValue< ::string>::Get());
#endif  // GTEST_HAS_GLOBAL_STRING

  EXPECT_EQ("", BuiltInDefaultValue< ::std::string>::Get());
}

// Tests that BuiltInDefaultValue<T>::Exists() returns true when T is a
// string type.
TEST(BuiltInDefaultValueTest, ExistsForString) {
#if GTEST_HAS_GLOBAL_STRING
  EXPECT_TRUE(BuiltInDefaultValue< ::string>::Exists());
#endif  // GTEST_HAS_GLOBAL_STRING

  EXPECT_TRUE(BuiltInDefaultValue< ::std::string>::Exists());
}

// Tests that BuiltInDefaultValue<const T>::Get() returns the same
// value as BuiltInDefaultValue<T>::Get() does.
TEST(BuiltInDefaultValueTest, WorksForConstTypes) {
  EXPECT_EQ("", BuiltInDefaultValue<const std::string>::Get());
  EXPECT_EQ(0, BuiltInDefaultValue<const int>::Get());
  EXPECT_TRUE(BuiltInDefaultValue<char* const>::Get() == NULL);
  EXPECT_FALSE(BuiltInDefaultValue<const bool>::Get());
}

// A type that's default constructible.
class MyDefaultConstructible {
 public:
  MyDefaultConstructible() : value_(42) {}

  int value() const { return value_; }

 private:
  int value_;
};

// A type that's not default constructible.
class MyNonDefaultConstructible {
 public:
  // Does not have a default ctor.
  explicit MyNonDefaultConstructible(int a_value) : value_(a_value) {}

  int value() const { return value_; }

 private:
  int value_;
};

#if GTEST_HAS_STD_TYPE_TRAITS_

TEST(BuiltInDefaultValueTest, ExistsForDefaultConstructibleType) {
  EXPECT_TRUE(BuiltInDefaultValue<MyDefaultConstructible>::Exists());
}

TEST(BuiltInDefaultValueTest, IsDefaultConstructedForDefaultConstructibleType) {
  EXPECT_EQ(42, BuiltInDefaultValue<MyDefaultConstructible>::Get().value());
}

#endif  // GTEST_HAS_STD_TYPE_TRAITS_

TEST(BuiltInDefaultValueTest, DoesNotExistForNonDefaultConstructibleType) {
  EXPECT_FALSE(BuiltInDefaultValue<MyNonDefaultConstructible>::Exists());
}

// Tests that BuiltInDefaultValue<T&>::Get() aborts the program.
TEST(BuiltInDefaultValueDeathTest, IsUndefinedForReferences) {
  EXPECT_DEATH_IF_SUPPORTED({
    BuiltInDefaultValue<int&>::Get();
  }, "");
  EXPECT_DEATH_IF_SUPPORTED({
    BuiltInDefaultValue<const char&>::Get();
  }, "");
}

TEST(BuiltInDefaultValueDeathTest, IsUndefinedForNonDefaultConstructibleType) {
  EXPECT_DEATH_IF_SUPPORTED({
    BuiltInDefaultValue<MyNonDefaultConstructible>::Get();
  }, "");
}

// Tests that DefaultValue<T>::IsSet() is false initially.
TEST(DefaultValueTest, IsInitiallyUnset) {
  EXPECT_FALSE(DefaultValue<int>::IsSet());
  EXPECT_FALSE(DefaultValue<MyDefaultConstructible>::IsSet());
  EXPECT_FALSE(DefaultValue<const MyNonDefaultConstructible>::IsSet());
}

// Tests that DefaultValue<T> can be set and then unset.
TEST(DefaultValueTest, CanBeSetAndUnset) {
  EXPECT_TRUE(DefaultValue<int>::Exists());
  EXPECT_FALSE(DefaultValue<const MyNonDefaultConstructible>::Exists());

  DefaultValue<int>::Set(1);
  DefaultValue<const MyNonDefaultConstructible>::Set(
      MyNonDefaultConstructible(42));

  EXPECT_EQ(1, DefaultValue<int>::Get());
  EXPECT_EQ(42, DefaultValue<const MyNonDefaultConstructible>::Get().value());

  EXPECT_TRUE(DefaultValue<int>::Exists());
  EXPECT_TRUE(DefaultValue<const MyNonDefaultConstructible>::Exists());

  DefaultValue<int>::Clear();
  DefaultValue<const MyNonDefaultConstructible>::Clear();

  EXPECT_FALSE(DefaultValue<int>::IsSet());
  EXPECT_FALSE(DefaultValue<const MyNonDefaultConstructible>::IsSet());

  EXPECT_TRUE(DefaultValue<int>::Exists());
  EXPECT_FALSE(DefaultValue<const MyNonDefaultConstructible>::Exists());
}

// Tests that DefaultValue<T>::Get() returns the
// BuiltInDefaultValue<T>::Get() when DefaultValue<T>::IsSet() is
// false.
TEST(DefaultValueDeathTest, GetReturnsBuiltInDefaultValueWhenUnset) {
  EXPECT_FALSE(DefaultValue<int>::IsSet());
  EXPECT_TRUE(DefaultValue<int>::Exists());
  EXPECT_FALSE(DefaultValue<MyNonDefaultConstructible>::IsSet());
  EXPECT_FALSE(DefaultValue<MyNonDefaultConstructible>::Exists());

  EXPECT_EQ(0, DefaultValue<int>::Get());

  EXPECT_DEATH_IF_SUPPORTED({
    DefaultValue<MyNonDefaultConstructible>::Get();
  }, "");
}

#if GTEST_HAS_STD_UNIQUE_PTR_
TEST(DefaultValueTest, GetWorksForMoveOnlyIfSet) {
  EXPECT_TRUE(DefaultValue<std::unique_ptr<int>>::Exists());
  EXPECT_TRUE(DefaultValue<std::unique_ptr<int>>::Get() == NULL);
  DefaultValue<std::unique_ptr<int>>::SetFactory([] {
    return std::unique_ptr<int>(new int(42));
  });
  EXPECT_TRUE(DefaultValue<std::unique_ptr<int>>::Exists());
  std::unique_ptr<int> i = DefaultValue<std::unique_ptr<int>>::Get();
  EXPECT_EQ(42, *i);
}
#endif  // GTEST_HAS_STD_UNIQUE_PTR_

// Tests that DefaultValue<void>::Get() returns void.
TEST(DefaultValueTest, GetWorksForVoid) {
  return DefaultValue<void>::Get();
}

// Tests using DefaultValue with a reference type.

// Tests that DefaultValue<T&>::IsSet() is false initially.
TEST(DefaultValueOfReferenceTest, IsInitiallyUnset) {
  EXPECT_FALSE(DefaultValue<int&>::IsSet());
  EXPECT_FALSE(DefaultValue<MyDefaultConstructible&>::IsSet());
  EXPECT_FALSE(DefaultValue<MyNonDefaultConstructible&>::IsSet());
}

// Tests that DefaultValue<T&>::Exists is false initiallly.
TEST(DefaultValueOfReferenceTest, IsInitiallyNotExisting) {
  EXPECT_FALSE(DefaultValue<int&>::Exists());
  EXPECT_FALSE(DefaultValue<MyDefaultConstructible&>::Exists());
  EXPECT_FALSE(DefaultValue<MyNonDefaultConstructible&>::Exists());
}

// Tests that DefaultValue<T&> can be set and then unset.
TEST(DefaultValueOfReferenceTest, CanBeSetAndUnset) {
  int n = 1;
  DefaultValue<const int&>::Set(n);
  MyNonDefaultConstructible x(42);
  DefaultValue<MyNonDefaultConstructible&>::Set(x);

  EXPECT_TRUE(DefaultValue<const int&>::Exists());
  EXPECT_TRUE(DefaultValue<MyNonDefaultConstructible&>::Exists());

  EXPECT_EQ(&n, &(DefaultValue<const int&>::Get()));
  EXPECT_EQ(&x, &(DefaultValue<MyNonDefaultConstructible&>::Get()));

  DefaultValue<const int&>::Clear();
  DefaultValue<MyNonDefaultConstructible&>::Clear();

  EXPECT_FALSE(DefaultValue<const int&>::Exists());
  EXPECT_FALSE(DefaultValue<MyNonDefaultConstructible&>::Exists());

  EXPECT_FALSE(DefaultValue<const int&>::IsSet());
  EXPECT_FALSE(DefaultValue<MyNonDefaultConstructible&>::IsSet());
}

// Tests that DefaultValue<T&>::Get() returns the
// BuiltInDefaultValue<T&>::Get() when DefaultValue<T&>::IsSet() is
// false.
TEST(DefaultValueOfReferenceDeathTest, GetReturnsBuiltInDefaultValueWhenUnset) {
  EXPECT_FALSE(DefaultValue<int&>::IsSet());
  EXPECT_FALSE(DefaultValue<MyNonDefaultConstructible&>::IsSet());

  EXPECT_DEATH_IF_SUPPORTED({
    DefaultValue<int&>::Get();
  }, "");
  EXPECT_DEATH_IF_SUPPORTED({
    DefaultValue<MyNonDefaultConstructible>::Get();
  }, "");
}

// Tests that ActionInterface can be implemented by defining the
// Perform method.

typedef int MyGlobalFunction(bool, int);

class MyActionImpl : public ActionInterface<MyGlobalFunction> {
 public:
  virtual int Perform(const tuple<bool, int>& args) {
    return get<0>(args) ? get<1>(args) : 0;
  }
};

TEST(ActionInterfaceTest, CanBeImplementedByDefiningPerform) {
  MyActionImpl my_action_impl;
  (void)my_action_impl;
}

TEST(ActionInterfaceTest, MakeAction) {
  Action<MyGlobalFunction> action = MakeAction(new MyActionImpl);

  // When exercising the Perform() method of Action<F>, we must pass
  // it a tuple whose size and type are compatible with F's argument
  // types.  For example, if F is int(), then Perform() takes a
  // 0-tuple; if F is void(bool, int), then Perform() takes a
  // tuple<bool, int>, and so on.
  EXPECT_EQ(5, action.Perform(make_tuple(true, 5)));
}

// Tests that Action<F> can be contructed from a pointer to
// ActionInterface<F>.
TEST(ActionTest, CanBeConstructedFromActionInterface) {
  Action<MyGlobalFunction> action(new MyActionImpl);
}

// Tests that Action<F> delegates actual work to ActionInterface<F>.
TEST(ActionTest, DelegatesWorkToActionInterface) {
  const Action<MyGlobalFunction> action(new MyActionImpl);

  EXPECT_EQ(5, action.Perform(make_tuple(true, 5)));
  EXPECT_EQ(0, action.Perform(make_tuple(false, 1)));
}

// Tests that Action<F> can be copied.
TEST(ActionTest, IsCopyable) {
  Action<MyGlobalFunction> a1(new MyActionImpl);
  Action<MyGlobalFunction> a2(a1);  // Tests the copy constructor.

  // a1 should continue to work after being copied from.
  EXPECT_EQ(5, a1.Perform(make_tuple(true, 5)));
  EXPECT_EQ(0, a1.Perform(make_tuple(false, 1)));

  // a2 should work like the action it was copied from.
  EXPECT_EQ(5, a2.Perform(make_tuple(true, 5)));
  EXPECT_EQ(0, a2.Perform(make_tuple(false, 1)));

  a2 = a1;  // Tests the assignment operator.

  // a1 should continue to work after being copied from.
  EXPECT_EQ(5, a1.Perform(make_tuple(true, 5)));
  EXPECT_EQ(0, a1.Perform(make_tuple(false, 1)));

  // a2 should work like the action it was copied from.
  EXPECT_EQ(5, a2.Perform(make_tuple(true, 5)));
  EXPECT_EQ(0, a2.Perform(make_tuple(false, 1)));
}

// Tests that an Action<From> object can be converted to a
// compatible Action<To> object.

class IsNotZero : public ActionInterface<bool(int)> {  // NOLINT
 public:
  virtual bool Perform(const tuple<int>& arg) {
    return get<0>(arg) != 0;
  }
};

#if !GTEST_OS_SYMBIAN
// Compiling this test on Nokia's Symbian compiler fails with:
//  'Result' is not a member of class 'testing::internal::Function<int>'
//  (point of instantiation: '@unnamed@gmock_actions_test_cc@::
//      ActionTest_CanBeConvertedToOtherActionType_Test::TestBody()')
// with no obvious fix.
TEST(ActionTest, CanBeConvertedToOtherActionType) {
  const Action<bool(int)> a1(new IsNotZero);  // NOLINT
  const Action<int(char)> a2 = Action<int(char)>(a1);  // NOLINT
  EXPECT_EQ(1, a2.Perform(make_tuple('a')));
  EXPECT_EQ(0, a2.Perform(make_tuple('\0')));
}
#endif  // !GTEST_OS_SYMBIAN

// The following two classes are for testing MakePolymorphicAction().

// Implements a polymorphic action that returns the second of the
// arguments it receives.
class ReturnSecondArgumentAction {
 public:
  // We want to verify that MakePolymorphicAction() can work with a
  // polymorphic action whose Perform() method template is either
  // const or not.  This lets us verify the non-const case.
  template <typename Result, typename ArgumentTuple>
  Result Perform(const ArgumentTuple& args) { return get<1>(args); }
};

// Implements a polymorphic action that can be used in a nullary
// function to return 0.
class ReturnZeroFromNullaryFunctionAction {
 public:
  // For testing that MakePolymorphicAction() works when the
  // implementation class' Perform() method template takes only one
  // template parameter.
  //
  // We want to verify that MakePolymorphicAction() can work with a
  // polymorphic action whose Perform() method template is either
  // const or not.  This lets us verify the const case.
  template <typename Result>
  Result Perform(const tuple<>&) const { return 0; }
};

// These functions verify that MakePolymorphicAction() returns a
// PolymorphicAction<T> where T is the argument's type.

PolymorphicAction<ReturnSecondArgumentAction> ReturnSecondArgument() {
  return MakePolymorphicAction(ReturnSecondArgumentAction());
}

PolymorphicAction<ReturnZeroFromNullaryFunctionAction>
ReturnZeroFromNullaryFunction() {
  return MakePolymorphicAction(ReturnZeroFromNullaryFunctionAction());
}

// Tests that MakePolymorphicAction() turns a polymorphic action
// implementation class into a polymorphic action.
TEST(MakePolymorphicActionTest, ConstructsActionFromImpl) {
  Action<int(bool, int, double)> a1 = ReturnSecondArgument();  // NOLINT
  EXPECT_EQ(5, a1.Perform(make_tuple(false, 5, 2.0)));
}

// Tests that MakePolymorphicAction() works when the implementation
// class' Perform() method template has only one template parameter.
TEST(MakePolymorphicActionTest, WorksWhenPerformHasOneTemplateParameter) {
  Action<int()> a1 = ReturnZeroFromNullaryFunction();
  EXPECT_EQ(0, a1.Perform(make_tuple()));

  Action<void*()> a2 = ReturnZeroFromNullaryFunction();
  EXPECT_TRUE(a2.Perform(make_tuple()) == NULL);
}

// Tests that Return() works as an action for void-returning
// functions.
TEST(ReturnTest, WorksForVoid) {
  const Action<void(int)> ret = Return();  // NOLINT
  return ret.Perform(make_tuple(1));
}

// Tests that Return(v) returns v.
TEST(ReturnTest, ReturnsGivenValue) {
  Action<int()> ret = Return(1);  // NOLINT
  EXPECT_EQ(1, ret.Perform(make_tuple()));

  ret = Return(-5);
  EXPECT_EQ(-5, ret.Perform(make_tuple()));
}

// Tests that Return("string literal") works.
TEST(ReturnTest, AcceptsStringLiteral) {
  Action<const char*()> a1 = Return("Hello");
  EXPECT_STREQ("Hello", a1.Perform(make_tuple()));

  Action<std::string()> a2 = Return("world");
  EXPECT_EQ("world", a2.Perform(make_tuple()));
}

// Test struct which wraps a vector of integers. Used in
// 'SupportsWrapperReturnType' test.
struct IntegerVectorWrapper {
  std::vector<int> * v;
  IntegerVectorWrapper(std::vector<int>& _v) : v(&_v) {}  // NOLINT
};

// Tests that Return() works when return type is a wrapper type.
TEST(ReturnTest, SupportsWrapperReturnType) {
  // Initialize vector of integers.
  std::vector<int> v;
  for (int i = 0; i < 5; ++i) v.push_back(i);

  // Return() called with 'v' as argument. The Action will return the same data
  // as 'v' (copy) but it will be wrapped in an IntegerVectorWrapper.
  Action<IntegerVectorWrapper()> a = Return(v);
  const std::vector<int>& result = *(a.Perform(make_tuple()).v);
  EXPECT_THAT(result, ::testing::ElementsAre(0, 1, 2, 3, 4));
}

// Tests that Return(v) is covaraint.

struct Base {
  bool operator==(const Base&) { return true; }
};

struct Derived : public Base {
  bool operator==(const Derived&) { return true; }
};

TEST(ReturnTest, IsCovariant) {
  Base base;
  Derived derived;
  Action<Base*()> ret = Return(&base);
  EXPECT_EQ(&base, ret.Perform(make_tuple()));

  ret = Return(&derived);
  EXPECT_EQ(&derived, ret.Perform(make_tuple()));
}

// Tests that the type of the value passed into Return is converted into T
// when the action is cast to Action<T(...)> rather than when the action is
// performed. See comments on testing::internal::ReturnAction in
// gmock-actions.h for more information.
class FromType {
 public:
  explicit FromType(bool* is_converted) : converted_(is_converted) {}
  bool* converted() const { return converted_; }

 private:
  bool* const converted_;

  GTEST_DISALLOW_ASSIGN_(FromType);
};

class ToType {
 public:
  // Must allow implicit conversion due to use in ImplicitCast_<T>.
  ToType(const FromType& x) { *x.converted() = true; }  // NOLINT
};

TEST(ReturnTest, ConvertsArgumentWhenConverted) {
  bool converted = false;
  FromType x(&converted);
  Action<ToType()> action(Return(x));
  EXPECT_TRUE(converted) << "Return must convert its argument in its own "
                         << "conversion operator.";
  converted = false;
  action.Perform(tuple<>());
  EXPECT_FALSE(converted) << "Action must NOT convert its argument "
                          << "when performed.";
}

class DestinationType {};

class SourceType {
 public:
  // Note: a non-const typecast operator.
  operator DestinationType() { return DestinationType(); }
};

TEST(ReturnTest, CanConvertArgumentUsingNonConstTypeCastOperator) {
  SourceType s;
  Action<DestinationType()> action(Return(s));
}

// Tests that ReturnNull() returns NULL in a pointer-returning function.
TEST(ReturnNullTest, WorksInPointerReturningFunction) {
  const Action<int*()> a1 = ReturnNull();
  EXPECT_TRUE(a1.Perform(make_tuple()) == NULL);

  const Action<const char*(bool)> a2 = ReturnNull();  // NOLINT
  EXPECT_TRUE(a2.Perform(make_tuple(true)) == NULL);
}

#if GTEST_HAS_STD_UNIQUE_PTR_
// Tests that ReturnNull() returns NULL for shared_ptr and unique_ptr returning
// functions.
TEST(ReturnNullTest, WorksInSmartPointerReturningFunction) {
  const Action<std::unique_ptr<const int>()> a1 = ReturnNull();
  EXPECT_TRUE(a1.Perform(make_tuple()) == nullptr);

  const Action<std::shared_ptr<int>(std::string)> a2 = ReturnNull();
  EXPECT_TRUE(a2.Perform(make_tuple("foo")) == nullptr);
}
#endif  // GTEST_HAS_STD_UNIQUE_PTR_

// Tests that ReturnRef(v) works for reference types.
TEST(ReturnRefTest, WorksForReference) {
  const int n = 0;
  const Action<const int&(bool)> ret = ReturnRef(n);  // NOLINT

  EXPECT_EQ(&n, &ret.Perform(make_tuple(true)));
}

// Tests that ReturnRef(v) is covariant.
TEST(ReturnRefTest, IsCovariant) {
  Base base;
  Derived derived;
  Action<Base&()> a = ReturnRef(base);
  EXPECT_EQ(&base, &a.Perform(make_tuple()));

  a = ReturnRef(derived);
  EXPECT_EQ(&derived, &a.Perform(make_tuple()));
}

// Tests that ReturnRefOfCopy(v) works for reference types.
TEST(ReturnRefOfCopyTest, WorksForReference) {
  int n = 42;
  const Action<const int&()> ret = ReturnRefOfCopy(n);

  EXPECT_NE(&n, &ret.Perform(make_tuple()));
  EXPECT_EQ(42, ret.Perform(make_tuple()));

  n = 43;
  EXPECT_NE(&n, &ret.Perform(make_tuple()));
  EXPECT_EQ(42, ret.Perform(make_tuple()));
}

// Tests that ReturnRefOfCopy(v) is covariant.
TEST(ReturnRefOfCopyTest, IsCovariant) {
  Base base;
  Derived derived;
  Action<Base&()> a = ReturnRefOfCopy(base);
  EXPECT_NE(&base, &a.Perform(make_tuple()));

  a = ReturnRefOfCopy(derived);
  EXPECT_NE(&derived, &a.Perform(make_tuple()));
}

// Tests that DoDefault() does the default action for the mock method.

class MockClass {
 public:
  MockClass() {}

  MOCK_METHOD1(IntFunc, int(bool flag));  // NOLINT
  MOCK_METHOD0(Foo, MyNonDefaultConstructible());
#if GTEST_HAS_STD_UNIQUE_PTR_
  MOCK_METHOD0(MakeUnique, std::unique_ptr<int>());
  MOCK_METHOD0(MakeUniqueBase, std::unique_ptr<Base>());
  MOCK_METHOD0(MakeVectorUnique, std::vector<std::unique_ptr<int>>());
#endif

 private:
  GTEST_DISALLOW_COPY_AND_ASSIGN_(MockClass);
};

// Tests that DoDefault() returns the built-in default value for the
// return type by default.
TEST(DoDefaultTest, ReturnsBuiltInDefaultValueByDefault) {
  MockClass mock;
  EXPECT_CALL(mock, IntFunc(_))
      .WillOnce(DoDefault());
  EXPECT_EQ(0, mock.IntFunc(true));
}

// Tests that DoDefault() throws (when exceptions are enabled) or aborts
// the process when there is no built-in default value for the return type.
TEST(DoDefaultDeathTest, DiesForUnknowType) {
  MockClass mock;
  EXPECT_CALL(mock, Foo())
      .WillRepeatedly(DoDefault());
#if GTEST_HAS_EXCEPTIONS
  EXPECT_ANY_THROW(mock.Foo());
#else
  EXPECT_DEATH_IF_SUPPORTED({
    mock.Foo();
  }, "");
#endif
}

// Tests that using DoDefault() inside a composite action leads to a
// run-time error.

void VoidFunc(bool /* flag */) {}

TEST(DoDefaultDeathTest, DiesIfUsedInCompositeAction) {
  MockClass mock;
  EXPECT_CALL(mock, IntFunc(_))
      .WillRepeatedly(DoAll(Invoke(VoidFunc),
                            DoDefault()));

  // Ideally we should verify the error message as well.  Sadly,
  // EXPECT_DEATH() can only capture stderr, while Google Mock's
  // errors are printed on stdout.  Therefore we have to settle for
  // not verifying the message.
  EXPECT_DEATH_IF_SUPPORTED({
    mock.IntFunc(true);
  }, "");
}

// Tests that DoDefault() returns the default value set by
// DefaultValue<T>::Set() when it's not overriden by an ON_CALL().
TEST(DoDefaultTest, ReturnsUserSpecifiedPerTypeDefaultValueWhenThereIsOne) {
  DefaultValue<int>::Set(1);
  MockClass mock;
  EXPECT_CALL(mock, IntFunc(_))
      .WillOnce(DoDefault());
  EXPECT_EQ(1, mock.IntFunc(false));
  DefaultValue<int>::Clear();
}

// Tests that DoDefault() does the action specified by ON_CALL().
TEST(DoDefaultTest, DoesWhatOnCallSpecifies) {
  MockClass mock;
  ON_CALL(mock, IntFunc(_))
      .WillByDefault(Return(2));
  EXPECT_CALL(mock, IntFunc(_))
      .WillOnce(DoDefault());
  EXPECT_EQ(2, mock.IntFunc(false));
}

// Tests that using DoDefault() in ON_CALL() leads to a run-time failure.
TEST(DoDefaultTest, CannotBeUsedInOnCall) {
  MockClass mock;
  EXPECT_NONFATAL_FAILURE({  // NOLINT
    ON_CALL(mock, IntFunc(_))
      .WillByDefault(DoDefault());
  }, "DoDefault() cannot be used in ON_CALL()");
}

// Tests that SetArgPointee<N>(v) sets the variable pointed to by
// the N-th (0-based) argument to v.
TEST(SetArgPointeeTest, SetsTheNthPointee) {
  typedef void MyFunction(bool, int*, char*);
  Action<MyFunction> a = SetArgPointee<1>(2);

  int n = 0;
  char ch = '\0';
  a.Perform(make_tuple(true, &n, &ch));
  EXPECT_EQ(2, n);
  EXPECT_EQ('\0', ch);

  a = SetArgPointee<2>('a');
  n = 0;
  ch = '\0';
  a.Perform(make_tuple(true, &n, &ch));
  EXPECT_EQ(0, n);
  EXPECT_EQ('a', ch);
}

#if !((GTEST_GCC_VER_ && GTEST_GCC_VER_ < 40000) || GTEST_OS_SYMBIAN)
// Tests that SetArgPointee<N>() accepts a string literal.
// GCC prior to v4.0 and the Symbian compiler do not support this.
TEST(SetArgPointeeTest, AcceptsStringLiteral) {
  typedef void MyFunction(std::string*, const char**);
  Action<MyFunction> a = SetArgPointee<0>("hi");
  std::string str;
  const char* ptr = NULL;
  a.Perform(make_tuple(&str, &ptr));
  EXPECT_EQ("hi", str);
  EXPECT_TRUE(ptr == NULL);

  a = SetArgPointee<1>("world");
  str = "";
  a.Perform(make_tuple(&str, &ptr));
  EXPECT_EQ("", str);
  EXPECT_STREQ("world", ptr);
}

TEST(SetArgPointeeTest, AcceptsWideStringLiteral) {
  typedef void MyFunction(const wchar_t**);
  Action<MyFunction> a = SetArgPointee<0>(L"world");
  const wchar_t* ptr = NULL;
  a.Perform(make_tuple(&ptr));
  EXPECT_STREQ(L"world", ptr);

# if GTEST_HAS_STD_WSTRING

  typedef void MyStringFunction(std::wstring*);
  Action<MyStringFunction> a2 = SetArgPointee<0>(L"world");
  std::wstring str = L"";
  a2.Perform(make_tuple(&str));
  EXPECT_EQ(L"world", str);

# endif
}
#endif

// Tests that SetArgPointee<N>() accepts a char pointer.
TEST(SetArgPointeeTest, AcceptsCharPointer) {
  typedef void MyFunction(bool, std::string*, const char**);
  const char* const hi = "hi";
  Action<MyFunction> a = SetArgPointee<1>(hi);
  std::string str;
  const char* ptr = NULL;
  a.Perform(make_tuple(true, &str, &ptr));
  EXPECT_EQ("hi", str);
  EXPECT_TRUE(ptr == NULL);

  char world_array[] = "world";
  char* const world = world_array;
  a = SetArgPointee<2>(world);
  str = "";
  a.Perform(make_tuple(true, &str, &ptr));
  EXPECT_EQ("", str);
  EXPECT_EQ(world, ptr);
}

TEST(SetArgPointeeTest, AcceptsWideCharPointer) {
  typedef void MyFunction(bool, const wchar_t**);
  const wchar_t* const hi = L"hi";
  Action<MyFunction> a = SetArgPointee<1>(hi);
  const wchar_t* ptr = NULL;
  a.Perform(make_tuple(true, &ptr));
  EXPECT_EQ(hi, ptr);

# if GTEST_HAS_STD_WSTRING

  typedef void MyStringFunction(bool, std::wstring*);
  wchar_t world_array[] = L"world";
  wchar_t* const world = world_array;
  Action<MyStringFunction> a2 = SetArgPointee<1>(world);
  std::wstring str;
  a2.Perform(make_tuple(true, &str));
  EXPECT_EQ(world_array, str);
# endif
}

#if GTEST_HAS_PROTOBUF_

// Tests that SetArgPointee<N>(proto_buffer) sets the v1 protobuf
// variable pointed to by the N-th (0-based) argument to proto_buffer.
TEST(SetArgPointeeTest, SetsTheNthPointeeOfProtoBufferType) {
  TestMessage* const msg = new TestMessage;
  msg->set_member("yes");
  TestMessage orig_msg;
  orig_msg.CopyFrom(*msg);

  Action<void(bool, TestMessage*)> a = SetArgPointee<1>(*msg);
  // SetArgPointee<N>(proto_buffer) makes a copy of proto_buffer
  // s.t. the action works even when the original proto_buffer has
  // died.  We ensure this behavior by deleting msg before using the
  // action.
  delete msg;

  TestMessage dest;
  EXPECT_FALSE(orig_msg.Equals(dest));
  a.Perform(make_tuple(true, &dest));
  EXPECT_TRUE(orig_msg.Equals(dest));
}

// Tests that SetArgPointee<N>(proto_buffer) sets the
// ::ProtocolMessage variable pointed to by the N-th (0-based)
// argument to proto_buffer.
TEST(SetArgPointeeTest, SetsTheNthPointeeOfProtoBufferBaseType) {
  TestMessage* const msg = new TestMessage;
  msg->set_member("yes");
  TestMessage orig_msg;
  orig_msg.CopyFrom(*msg);

  Action<void(bool, ::ProtocolMessage*)> a = SetArgPointee<1>(*msg);
  // SetArgPointee<N>(proto_buffer) makes a copy of proto_buffer
  // s.t. the action works even when the original proto_buffer has
  // died.  We ensure this behavior by deleting msg before using the
  // action.
  delete msg;

  TestMessage dest;
  ::ProtocolMessage* const dest_base = &dest;
  EXPECT_FALSE(orig_msg.Equals(dest));
  a.Perform(make_tuple(true, dest_base));
  EXPECT_TRUE(orig_msg.Equals(dest));
}

// Tests that SetArgPointee<N>(proto2_buffer) sets the v2
// protobuf variable pointed to by the N-th (0-based) argument to
// proto2_buffer.
TEST(SetArgPointeeTest, SetsTheNthPointeeOfProto2BufferType) {
  using testing::internal::FooMessage;
  FooMessage* const msg = new FooMessage;
  msg->set_int_field(2);
  msg->set_string_field("hi");
  FooMessage orig_msg;
  orig_msg.CopyFrom(*msg);

  Action<void(bool, FooMessage*)> a = SetArgPointee<1>(*msg);
  // SetArgPointee<N>(proto2_buffer) makes a copy of
  // proto2_buffer s.t. the action works even when the original
  // proto2_buffer has died.  We ensure this behavior by deleting msg
  // before using the action.
  delete msg;

  FooMessage dest;
  dest.set_int_field(0);
  a.Perform(make_tuple(true, &dest));
  EXPECT_EQ(2, dest.int_field());
  EXPECT_EQ("hi", dest.string_field());
}

// Tests that SetArgPointee<N>(proto2_buffer) sets the
// proto2::Message variable pointed to by the N-th (0-based) argument
// to proto2_buffer.
TEST(SetArgPointeeTest, SetsTheNthPointeeOfProto2BufferBaseType) {
  using testing::internal::FooMessage;
  FooMessage* const msg = new FooMessage;
  msg->set_int_field(2);
  msg->set_string_field("hi");
  FooMessage orig_msg;
  orig_msg.CopyFrom(*msg);

  Action<void(bool, ::proto2::Message*)> a = SetArgPointee<1>(*msg);
  // SetArgPointee<N>(proto2_buffer) makes a copy of
  // proto2_buffer s.t. the action works even when the original
  // proto2_buffer has died.  We ensure this behavior by deleting msg
  // before using the action.
  delete msg;

  FooMessage dest;
  dest.set_int_field(0);
  ::proto2::Message* const dest_base = &dest;
  a.Perform(make_tuple(true, dest_base));
  EXPECT_EQ(2, dest.int_field());
  EXPECT_EQ("hi", dest.string_field());
}

#endif  // GTEST_HAS_PROTOBUF_

// Tests that SetArgumentPointee<N>(v) sets the variable pointed to by
// the N-th (0-based) argument to v.
TEST(SetArgumentPointeeTest, SetsTheNthPointee) {
  typedef void MyFunction(bool, int*, char*);
  Action<MyFunction> a = SetArgumentPointee<1>(2);

  int n = 0;
  char ch = '\0';
  a.Perform(make_tuple(true, &n, &ch));
  EXPECT_EQ(2, n);
  EXPECT_EQ('\0', ch);

  a = SetArgumentPointee<2>('a');
  n = 0;
  ch = '\0';
  a.Perform(make_tuple(true, &n, &ch));
  EXPECT_EQ(0, n);
  EXPECT_EQ('a', ch);
}

#if GTEST_HAS_PROTOBUF_

// Tests that SetArgumentPointee<N>(proto_buffer) sets the v1 protobuf
// variable pointed to by the N-th (0-based) argument to proto_buffer.
TEST(SetArgumentPointeeTest, SetsTheNthPointeeOfProtoBufferType) {
  TestMessage* const msg = new TestMessage;
  msg->set_member("yes");
  TestMessage orig_msg;
  orig_msg.CopyFrom(*msg);

  Action<void(bool, TestMessage*)> a = SetArgumentPointee<1>(*msg);
  // SetArgumentPointee<N>(proto_buffer) makes a copy of proto_buffer
  // s.t. the action works even when the original proto_buffer has
  // died.  We ensure this behavior by deleting msg before using the
  // action.
  delete msg;

  TestMessage dest;
  EXPECT_FALSE(orig_msg.Equals(dest));
  a.Perform(make_tuple(true, &dest));
  EXPECT_TRUE(orig_msg.Equals(dest));
}

// Tests that SetArgumentPointee<N>(proto_buffer) sets the
// ::ProtocolMessage variable pointed to by the N-th (0-based)
// argument to proto_buffer.
TEST(SetArgumentPointeeTest, SetsTheNthPointeeOfProtoBufferBaseType) {
  TestMessage* const msg = new TestMessage;
  msg->set_member("yes");
  TestMessage orig_msg;
  orig_msg.CopyFrom(*msg);

  Action<void(bool, ::ProtocolMessage*)> a = SetArgumentPointee<1>(*msg);
  // SetArgumentPointee<N>(proto_buffer) makes a copy of proto_buffer
  // s.t. the action works even when the original proto_buffer has
  // died.  We ensure this behavior by deleting msg before using the
  // action.
  delete msg;

  TestMessage dest;
  ::ProtocolMessage* const dest_base = &dest;
  EXPECT_FALSE(orig_msg.Equals(dest));
  a.Perform(make_tuple(true, dest_base));
  EXPECT_TRUE(orig_msg.Equals(dest));
}

// Tests that SetArgumentPointee<N>(proto2_buffer) sets the v2
// protobuf variable pointed to by the N-th (0-based) argument to
// proto2_buffer.
TEST(SetArgumentPointeeTest, SetsTheNthPointeeOfProto2BufferType) {
  using testing::internal::FooMessage;
  FooMessage* const msg = new FooMessage;
  msg->set_int_field(2);
  msg->set_string_field("hi");
  FooMessage orig_msg;
  orig_msg.CopyFrom(*msg);

  Action<void(bool, FooMessage*)> a = SetArgumentPointee<1>(*msg);
  // SetArgumentPointee<N>(proto2_buffer) makes a copy of
  // proto2_buffer s.t. the action works even when the original
  // proto2_buffer has died.  We ensure this behavior by deleting msg
  // before using the action.
  delete msg;

  FooMessage dest;
  dest.set_int_field(0);
  a.Perform(make_tuple(true, &dest));
  EXPECT_EQ(2, dest.int_field());
  EXPECT_EQ("hi", dest.string_field());
}

// Tests that SetArgumentPointee<N>(proto2_buffer) sets the
// proto2::Message variable pointed to by the N-th (0-based) argument
// to proto2_buffer.
TEST(SetArgumentPointeeTest, SetsTheNthPointeeOfProto2BufferBaseType) {
  using testing::internal::FooMessage;
  FooMessage* const msg = new FooMessage;
  msg->set_int_field(2);
  msg->set_string_field("hi");
  FooMessage orig_msg;
  orig_msg.CopyFrom(*msg);

  Action<void(bool, ::proto2::Message*)> a = SetArgumentPointee<1>(*msg);
  // SetArgumentPointee<N>(proto2_buffer) makes a copy of
  // proto2_buffer s.t. the action works even when the original
  // proto2_buffer has died.  We ensure this behavior by deleting msg
  // before using the action.
  delete msg;

  FooMessage dest;
  dest.set_int_field(0);
  ::proto2::Message* const dest_base = &dest;
  a.Perform(make_tuple(true, dest_base));
  EXPECT_EQ(2, dest.int_field());
  EXPECT_EQ("hi", dest.string_field());
}

#endif  // GTEST_HAS_PROTOBUF_

// Sample functions and functors for testing Invoke() and etc.
int Nullary() { return 1; }

class NullaryFunctor {
 public:
  int operator()() { return 2; }
};

bool g_done = false;
void VoidNullary() { g_done = true; }

class VoidNullaryFunctor {
 public:
  void operator()() { g_done = true; }
};

class Foo {
 public:
  Foo() : value_(123) {}

  int Nullary() const { return value_; }

 private:
  int value_;
};

// Tests InvokeWithoutArgs(function).
TEST(InvokeWithoutArgsTest, Function) {
  // As an action that takes one argument.
  Action<int(int)> a = InvokeWithoutArgs(Nullary);  // NOLINT
  EXPECT_EQ(1, a.Perform(make_tuple(2)));

  // As an action that takes two arguments.
  Action<int(int, double)> a2 = InvokeWithoutArgs(Nullary);  // NOLINT
  EXPECT_EQ(1, a2.Perform(make_tuple(2, 3.5)));

  // As an action that returns void.
  Action<void(int)> a3 = InvokeWithoutArgs(VoidNullary);  // NOLINT
  g_done = false;
  a3.Perform(make_tuple(1));
  EXPECT_TRUE(g_done);
}

// Tests InvokeWithoutArgs(functor).
TEST(InvokeWithoutArgsTest, Functor) {
  // As an action that takes no argument.
  Action<int()> a = InvokeWithoutArgs(NullaryFunctor());  // NOLINT
  EXPECT_EQ(2, a.Perform(make_tuple()));

  // As an action that takes three arguments.
  Action<int(int, double, char)> a2 =  // NOLINT
      InvokeWithoutArgs(NullaryFunctor());
  EXPECT_EQ(2, a2.Perform(make_tuple(3, 3.5, 'a')));

  // As an action that returns void.
  Action<void()> a3 = InvokeWithoutArgs(VoidNullaryFunctor());
  g_done = false;
  a3.Perform(make_tuple());
  EXPECT_TRUE(g_done);
}

// Tests InvokeWithoutArgs(obj_ptr, method).
TEST(InvokeWithoutArgsTest, Method) {
  Foo foo;
  Action<int(bool, char)> a =  // NOLINT
      InvokeWithoutArgs(&foo, &Foo::Nullary);
  EXPECT_EQ(123, a.Perform(make_tuple(true, 'a')));
}

// Tests using IgnoreResult() on a polymorphic action.
TEST(IgnoreResultTest, PolymorphicAction) {
  Action<void(int)> a = IgnoreResult(Return(5));  // NOLINT
  a.Perform(make_tuple(1));
}

// Tests using IgnoreResult() on a monomorphic action.

int ReturnOne() {
  g_done = true;
  return 1;
}

TEST(IgnoreResultTest, MonomorphicAction) {
  g_done = false;
  Action<void()> a = IgnoreResult(Invoke(ReturnOne));
  a.Perform(make_tuple());
  EXPECT_TRUE(g_done);
}

// Tests using IgnoreResult() on an action that returns a class type.

MyNonDefaultConstructible ReturnMyNonDefaultConstructible(double /* x */) {
  g_done = true;
  return MyNonDefaultConstructible(42);
}

TEST(IgnoreResultTest, ActionReturningClass) {
  g_done = false;
  Action<void(int)> a =
      IgnoreResult(Invoke(ReturnMyNonDefaultConstructible));  // NOLINT
  a.Perform(make_tuple(2));
  EXPECT_TRUE(g_done);
}

TEST(AssignTest, Int) {
  int x = 0;
  Action<void(int)> a = Assign(&x, 5);
  a.Perform(make_tuple(0));
  EXPECT_EQ(5, x);
}

TEST(AssignTest, String) {
  ::std::string x;
  Action<void(void)> a = Assign(&x, "Hello, world");
  a.Perform(make_tuple());
  EXPECT_EQ("Hello, world", x);
}

TEST(AssignTest, CompatibleTypes) {
  double x = 0;
  Action<void(int)> a = Assign(&x, 5);
  a.Perform(make_tuple(0));
  EXPECT_DOUBLE_EQ(5, x);
}

#if !GTEST_OS_WINDOWS_MOBILE

class SetErrnoAndReturnTest : public testing::Test {
 protected:
  virtual void SetUp() { errno = 0; }
  virtual void TearDown() { errno = 0; }
};

TEST_F(SetErrnoAndReturnTest, Int) {
  Action<int(void)> a = SetErrnoAndReturn(ENOTTY, -5);
  EXPECT_EQ(-5, a.Perform(make_tuple()));
  EXPECT_EQ(ENOTTY, errno);
}

TEST_F(SetErrnoAndReturnTest, Ptr) {
  int x;
  Action<int*(void)> a = SetErrnoAndReturn(ENOTTY, &x);
  EXPECT_EQ(&x, a.Perform(make_tuple()));
  EXPECT_EQ(ENOTTY, errno);
}

TEST_F(SetErrnoAndReturnTest, CompatibleTypes) {
  Action<double()> a = SetErrnoAndReturn(EINVAL, 5);
  EXPECT_DOUBLE_EQ(5.0, a.Perform(make_tuple()));
  EXPECT_EQ(EINVAL, errno);
}

#endif  // !GTEST_OS_WINDOWS_MOBILE

// Tests ByRef().

// Tests that ReferenceWrapper<T> is copyable.
TEST(ByRefTest, IsCopyable) {
  const std::string s1 = "Hi";
  const std::string s2 = "Hello";

  ::testing::internal::ReferenceWrapper<const std::string> ref_wrapper =
      ByRef(s1);
  const std::string& r1 = ref_wrapper;
  EXPECT_EQ(&s1, &r1);

  // Assigns a new value to ref_wrapper.
  ref_wrapper = ByRef(s2);
  const std::string& r2 = ref_wrapper;
  EXPECT_EQ(&s2, &r2);

  ::testing::internal::ReferenceWrapper<const std::string> ref_wrapper1 =
      ByRef(s1);
  // Copies ref_wrapper1 to ref_wrapper.
  ref_wrapper = ref_wrapper1;
  const std::string& r3 = ref_wrapper;
  EXPECT_EQ(&s1, &r3);
}

// Tests using ByRef() on a const value.
TEST(ByRefTest, ConstValue) {
  const int n = 0;
  // int& ref = ByRef(n);  // This shouldn't compile - we have a
                           // negative compilation test to catch it.
  const int& const_ref = ByRef(n);
  EXPECT_EQ(&n, &const_ref);
}

// Tests using ByRef() on a non-const value.
TEST(ByRefTest, NonConstValue) {
  int n = 0;

  // ByRef(n) can be used as either an int&,
  int& ref = ByRef(n);
  EXPECT_EQ(&n, &ref);

  // or a const int&.
  const int& const_ref = ByRef(n);
  EXPECT_EQ(&n, &const_ref);
}

// Tests explicitly specifying the type when using ByRef().
TEST(ByRefTest, ExplicitType) {
  int n = 0;
  const int& r1 = ByRef<const int>(n);
  EXPECT_EQ(&n, &r1);

  // ByRef<char>(n);  // This shouldn't compile - we have a negative
                      // compilation test to catch it.

  Derived d;
  Derived& r2 = ByRef<Derived>(d);
  EXPECT_EQ(&d, &r2);

  const Derived& r3 = ByRef<const Derived>(d);
  EXPECT_EQ(&d, &r3);

  Base& r4 = ByRef<Base>(d);
  EXPECT_EQ(&d, &r4);

  const Base& r5 = ByRef<const Base>(d);
  EXPECT_EQ(&d, &r5);

  // The following shouldn't compile - we have a negative compilation
  // test for it.
  //
  // Base b;
  // ByRef<Derived>(b);
}

// Tests that Google Mock prints expression ByRef(x) as a reference to x.
TEST(ByRefTest, PrintsCorrectly) {
  int n = 42;
  ::std::stringstream expected, actual;
  testing::internal::UniversalPrinter<const int&>::Print(n, &expected);
  testing::internal::UniversalPrint(ByRef(n), &actual);
  EXPECT_EQ(expected.str(), actual.str());
}

#if GTEST_HAS_STD_UNIQUE_PTR_

std::unique_ptr<int> UniquePtrSource() {
  return std::unique_ptr<int>(new int(19));
}

std::vector<std::unique_ptr<int>> VectorUniquePtrSource() {
  std::vector<std::unique_ptr<int>> out;
  out.emplace_back(new int(7));
  return out;
}

TEST(MockMethodTest, CanReturnMoveOnlyValue_Return) {
  MockClass mock;
  std::unique_ptr<int> i(new int(19));
  EXPECT_CALL(mock, MakeUnique()).WillOnce(Return(ByMove(std::move(i))));
  EXPECT_CALL(mock, MakeVectorUnique())
      .WillOnce(Return(ByMove(VectorUniquePtrSource())));
  Derived* d = new Derived;
  EXPECT_CALL(mock, MakeUniqueBase())
      .WillOnce(Return(ByMove(std::unique_ptr<Derived>(d))));

  std::unique_ptr<int> result1 = mock.MakeUnique();
  EXPECT_EQ(19, *result1);

  std::vector<std::unique_ptr<int>> vresult = mock.MakeVectorUnique();
  EXPECT_EQ(1u, vresult.size());
  EXPECT_NE(nullptr, vresult[0]);
  EXPECT_EQ(7, *vresult[0]);

  std::unique_ptr<Base> result2 = mock.MakeUniqueBase();
  EXPECT_EQ(d, result2.get());
}

TEST(MockMethodTest, CanReturnMoveOnlyValue_DoAllReturn) {
  testing::MockFunction<void()> mock_function;
  MockClass mock;
  std::unique_ptr<int> i(new int(19));
  EXPECT_CALL(mock_function, Call());
  EXPECT_CALL(mock, MakeUnique()).WillOnce(DoAll(
      InvokeWithoutArgs(&mock_function, &testing::MockFunction<void()>::Call),
      Return(ByMove(std::move(i)))));

  std::unique_ptr<int> result1 = mock.MakeUnique();
  EXPECT_EQ(19, *result1);
}

TEST(MockMethodTest, CanReturnMoveOnlyValue_Invoke) {
  MockClass mock;

  // Check default value
  DefaultValue<std::unique_ptr<int>>::SetFactory([] {
    return std::unique_ptr<int>(new int(42));
  });
  EXPECT_EQ(42, *mock.MakeUnique());

  EXPECT_CALL(mock, MakeUnique()).WillRepeatedly(Invoke(UniquePtrSource));
  EXPECT_CALL(mock, MakeVectorUnique())
      .WillRepeatedly(Invoke(VectorUniquePtrSource));
  std::unique_ptr<int> result1 = mock.MakeUnique();
  EXPECT_EQ(19, *result1);
  std::unique_ptr<int> result2 = mock.MakeUnique();
  EXPECT_EQ(19, *result2);
  EXPECT_NE(result1, result2);

  std::vector<std::unique_ptr<int>> vresult = mock.MakeVectorUnique();
  EXPECT_EQ(1u, vresult.size());
  EXPECT_NE(nullptr, vresult[0]);
  EXPECT_EQ(7, *vresult[0]);
}

#endif  // GTEST_HAS_STD_UNIQUE_PTR_

}  // Unnamed namespace

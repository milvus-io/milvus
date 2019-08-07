// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#pragma once

#include <memory>

#include "arrow/compute/kernel.h"

namespace arrow {

class Array;
class Status;

namespace compute {

class FunctionContext;
struct Datum;

/// AggregateFunction is an interface for Aggregates
///
/// An aggregates transforms an array into single result called a state via the
/// Consume method.. State supports the merge operation via the Merge method.
/// State can be sealed into a final result via the Finalize method.
//
/// State ownership is handled by callers, thus the interface exposes 3 methods
/// for the caller to manage memory:
/// - Size
/// - New (placement new constructor invocation)
/// - Delete (state desctructor)
///
/// Design inspired by ClickHouse aggregate functions.
class AggregateFunction {
 public:
  /// \brief Consume an array into a state.
  virtual Status Consume(const Array& input, void* state) const = 0;

  /// \brief Merge states.
  virtual Status Merge(const void* src, void* dst) const = 0;

  /// \brief Convert state into a final result.
  virtual Status Finalize(const void* src, Datum* output) const = 0;

  virtual ~AggregateFunction() {}

  virtual std::shared_ptr<DataType> out_type() const = 0;

  /// State management methods.
  virtual int64_t Size() const = 0;
  virtual void New(void* ptr) const = 0;
  virtual void Delete(void* ptr) const = 0;
};

/// AggregateFunction partial implementation for static type state
template <typename State>
class AggregateFunctionStaticState : public AggregateFunction {
  virtual Status Consume(const Array& input, State* state) const = 0;
  virtual Status Merge(const State& src, State* dst) const = 0;
  virtual Status Finalize(const State& src, Datum* output) const = 0;

  Status Consume(const Array& input, void* state) const final {
    return Consume(input, static_cast<State*>(state));
  }

  Status Merge(const void* src, void* dst) const final {
    return Merge(*static_cast<const State*>(src), static_cast<State*>(dst));
  }

  /// \brief Convert state into a final result.
  Status Finalize(const void* src, Datum* output) const final {
    return Finalize(*static_cast<const State*>(src), output);
  }

  int64_t Size() const final { return sizeof(State); }

  void New(void* ptr) const final {
    // By using placement-new syntax, the constructor of the State is invoked
    // in the memory location defined by the caller. This only supports State
    // with a parameter-less constructor.
    new (ptr) State;
  }

  void Delete(void* ptr) const final { static_cast<State*>(ptr)->~State(); }
};

/// \brief UnaryKernel implemented by an AggregateState
class ARROW_EXPORT AggregateUnaryKernel : public UnaryKernel {
 public:
  explicit AggregateUnaryKernel(std::shared_ptr<AggregateFunction>& aggregate)
      : aggregate_function_(aggregate) {}

  Status Call(FunctionContext* ctx, const Datum& input, Datum* out) override;

  std::shared_ptr<DataType> out_type() const override;

 private:
  std::shared_ptr<AggregateFunction> aggregate_function_;
};

}  // namespace compute
}  // namespace arrow

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
#include <string>

#include "arrow/compute/type_fwd.h"
#include "arrow/status.h"
#include "arrow/util/macros.h"
#include "arrow/util/visibility.h"

namespace arrow {
namespace compute {

class LogicalType;
class ExprVisitor;
class Operation;

/// \brief Base class for all analytic expressions. Expressions may represent
/// data values (scalars, arrays, tables)
class ARROW_EXPORT Expr {
 public:
  /// \brief Instantiate expression from an abstract operation
  /// \param[in] op the operation that generates the expression
  explicit Expr(ConstOpPtr op);

  virtual ~Expr() = default;

  /// \brief A unique string identifier for the kind of expression
  virtual std::string kind() const = 0;

  /// \brief Accept expression visitor
  /// TODO(wesm)
  // virtual Status Accept(ExprVisitor* visitor) const = 0;

  /// \brief The underlying operation
  ConstOpPtr op() const { return op_; }

 protected:
  ConstOpPtr op_;
};

/// The value cardinality: one or many. These correspond to the arrow::Scalar
/// and arrow::Array types
enum class ValueRank { SCALAR, ARRAY };

/// \brief Base class for a data-generated expression with a fixed and known
/// type. This includes arrays and scalars
class ARROW_EXPORT ValueExpr : public Expr {
 public:
  /// \brief The name of the expression, if any. The default is unnamed
  // virtual const ExprName& name() const;
  LogicalTypePtr type() const;

  /// \brief The value cardinality (scalar or array) of the expression
  virtual ValueRank rank() const = 0;

 protected:
  ValueExpr(ConstOpPtr op, LogicalTypePtr type);

  /// \brief The semantic data type of the expression
  LogicalTypePtr type_;
};

class ARROW_EXPORT ArrayExpr : public ValueExpr {
 protected:
  using ValueExpr::ValueExpr;
  std::string kind() const override;
  ValueRank rank() const override;
};

class ARROW_EXPORT ScalarExpr : public ValueExpr {
 protected:
  using ValueExpr::ValueExpr;
  std::string kind() const override;
  ValueRank rank() const override;
};

namespace value {

// These are mixin classes to provide a type hierarchy for values identify
class ValueMixin {};
class Null : public ValueMixin {};
class Bool : public ValueMixin {};
class Number : public ValueMixin {};
class Integer : public Number {};
class SignedInteger : public Integer {};
class Int8 : public SignedInteger {};
class Int16 : public SignedInteger {};
class Int32 : public SignedInteger {};
class Int64 : public SignedInteger {};
class UnsignedInteger : public Integer {};
class UInt8 : public UnsignedInteger {};
class UInt16 : public UnsignedInteger {};
class UInt32 : public UnsignedInteger {};
class UInt64 : public UnsignedInteger {};
class Floating : public Number {};
class Float16 : public Floating {};
class Float32 : public Floating {};
class Float64 : public Floating {};
class Binary : public ValueMixin {};
class Utf8 : public Binary {};
class List : public ValueMixin {};
class Struct : public ValueMixin {};

}  // namespace value

#define SIMPLE_EXPR_FACTORY(NAME) ARROW_EXPORT ExprPtr NAME(ConstOpPtr op);

namespace scalar {

#define DECLARE_SCALAR_EXPR(TYPE)                                   \
  class ARROW_EXPORT TYPE : public ScalarExpr, public value::TYPE { \
   public:                                                          \
    explicit TYPE(ConstOpPtr op);                                   \
    using ScalarExpr::kind;                                         \
  };

DECLARE_SCALAR_EXPR(Null)
DECLARE_SCALAR_EXPR(Bool)
DECLARE_SCALAR_EXPR(Int8)
DECLARE_SCALAR_EXPR(Int16)
DECLARE_SCALAR_EXPR(Int32)
DECLARE_SCALAR_EXPR(Int64)
DECLARE_SCALAR_EXPR(UInt8)
DECLARE_SCALAR_EXPR(UInt16)
DECLARE_SCALAR_EXPR(UInt32)
DECLARE_SCALAR_EXPR(UInt64)
DECLARE_SCALAR_EXPR(Float16)
DECLARE_SCALAR_EXPR(Float32)
DECLARE_SCALAR_EXPR(Float64)
DECLARE_SCALAR_EXPR(Binary)
DECLARE_SCALAR_EXPR(Utf8)

#undef DECLARE_SCALAR_EXPR

SIMPLE_EXPR_FACTORY(null);
SIMPLE_EXPR_FACTORY(boolean);
SIMPLE_EXPR_FACTORY(int8);
SIMPLE_EXPR_FACTORY(int16);
SIMPLE_EXPR_FACTORY(int32);
SIMPLE_EXPR_FACTORY(int64);
SIMPLE_EXPR_FACTORY(uint8);
SIMPLE_EXPR_FACTORY(uint16);
SIMPLE_EXPR_FACTORY(uint32);
SIMPLE_EXPR_FACTORY(uint64);
SIMPLE_EXPR_FACTORY(float16);
SIMPLE_EXPR_FACTORY(float32);
SIMPLE_EXPR_FACTORY(float64);
SIMPLE_EXPR_FACTORY(binary);
SIMPLE_EXPR_FACTORY(utf8);

class ARROW_EXPORT List : public ScalarExpr, public value::List {
 public:
  List(ConstOpPtr op, LogicalTypePtr type);
  using ScalarExpr::kind;
};

class ARROW_EXPORT Struct : public ScalarExpr, public value::Struct {
 public:
  Struct(ConstOpPtr op, LogicalTypePtr type);
  using ScalarExpr::kind;
};

}  // namespace scalar

namespace array {

#define DECLARE_ARRAY_EXPR(TYPE)                                   \
  class ARROW_EXPORT TYPE : public ArrayExpr, public value::TYPE { \
   public:                                                         \
    explicit TYPE(ConstOpPtr op);                                  \
    using ArrayExpr::kind;                                         \
  };

DECLARE_ARRAY_EXPR(Null)
DECLARE_ARRAY_EXPR(Bool)
DECLARE_ARRAY_EXPR(Int8)
DECLARE_ARRAY_EXPR(Int16)
DECLARE_ARRAY_EXPR(Int32)
DECLARE_ARRAY_EXPR(Int64)
DECLARE_ARRAY_EXPR(UInt8)
DECLARE_ARRAY_EXPR(UInt16)
DECLARE_ARRAY_EXPR(UInt32)
DECLARE_ARRAY_EXPR(UInt64)
DECLARE_ARRAY_EXPR(Float16)
DECLARE_ARRAY_EXPR(Float32)
DECLARE_ARRAY_EXPR(Float64)
DECLARE_ARRAY_EXPR(Binary)
DECLARE_ARRAY_EXPR(Utf8)

#undef DECLARE_ARRAY_EXPR

SIMPLE_EXPR_FACTORY(null);
SIMPLE_EXPR_FACTORY(boolean);
SIMPLE_EXPR_FACTORY(int8);
SIMPLE_EXPR_FACTORY(int16);
SIMPLE_EXPR_FACTORY(int32);
SIMPLE_EXPR_FACTORY(int64);
SIMPLE_EXPR_FACTORY(uint8);
SIMPLE_EXPR_FACTORY(uint16);
SIMPLE_EXPR_FACTORY(uint32);
SIMPLE_EXPR_FACTORY(uint64);
SIMPLE_EXPR_FACTORY(float16);
SIMPLE_EXPR_FACTORY(float32);
SIMPLE_EXPR_FACTORY(float64);
SIMPLE_EXPR_FACTORY(binary);
SIMPLE_EXPR_FACTORY(utf8);

class ARROW_EXPORT List : public ArrayExpr, public value::List {
 public:
  List(ConstOpPtr op, LogicalTypePtr type);
  using ArrayExpr::kind;
};

class ARROW_EXPORT Struct : public ArrayExpr, public value::Struct {
 public:
  Struct(ConstOpPtr op, LogicalTypePtr type);
  using ArrayExpr::kind;
};

}  // namespace array

#undef SIMPLE_EXPR_FACTORY

template <typename T, typename ObjectType>
inline bool InheritsFrom(const ObjectType* obj) {
  return dynamic_cast<const T*>(obj) != NULLPTR;
}

template <typename T, typename ObjectType>
inline bool InheritsFrom(const ObjectType& obj) {
  return dynamic_cast<const T*>(&obj) != NULLPTR;
}

/// \brief Construct a ScalarExpr containing an Operation given a logical type
ARROW_EXPORT
Status GetScalarExpr(ConstOpPtr op, LogicalTypePtr ty, ExprPtr* out);

/// \brief Construct an ArrayExpr containing an Operation given a logical type
ARROW_EXPORT
Status GetArrayExpr(ConstOpPtr op, LogicalTypePtr ty, ExprPtr* out);

}  // namespace compute
}  // namespace arrow

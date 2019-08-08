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

// Metadata objects for creating well-typed expressions. These are distinct
// from (and higher level than) arrow::DataType as some type parameters (like
// decimal scale and precision) may not be known at expression build time, and
// these are resolved later on evaluation

#pragma once

#include <memory>
#include <string>

#include "arrow/compute/type_fwd.h"
#include "arrow/util/visibility.h"

namespace arrow {

class Status;

namespace compute {

class Expr;

/// \brief An object that represents either a single concrete value type or a
/// group of related types, to help with expression type validation and other
/// purposes
class ARROW_EXPORT LogicalType {
 public:
  enum Id {
    ANY,
    NUMBER,
    INTEGER,
    SIGNED_INTEGER,
    UNSIGNED_INTEGER,
    FLOATING,
    NULL_,
    BOOL,
    UINT8,
    INT8,
    UINT16,
    INT16,
    UINT32,
    INT32,
    UINT64,
    INT64,
    FLOAT16,
    FLOAT32,
    FLOAT64,
    BINARY,
    UTF8,
    DATE,
    TIME,
    TIMESTAMP,
    DECIMAL,
    LIST,
    STRUCT
  };

  Id id() const { return id_; }

  virtual ~LogicalType() = default;

  virtual std::string ToString() const = 0;

  /// \brief Check if expression is an instance of this type class
  virtual bool IsInstance(const Expr& expr) const = 0;

  /// \brief Get a logical expression type from a concrete Arrow in-memory
  /// array type
  static Status FromArrow(const ::arrow::DataType& type, LogicalTypePtr* out);

 protected:
  explicit LogicalType(Id id) : id_(id) {}
  Id id_;
};

namespace type {

/// \brief Logical type for any value type
class ARROW_EXPORT Any : public LogicalType {
 public:
  Any() : LogicalType(LogicalType::ANY) {}
  bool IsInstance(const Expr& expr) const override;
  std::string ToString() const override;
};

/// \brief Logical type for null
class ARROW_EXPORT Null : public LogicalType {
 public:
  Null() : LogicalType(LogicalType::NULL_) {}
  bool IsInstance(const Expr& expr) const override;
  std::string ToString() const override;
};

/// \brief Logical type for concrete boolean
class ARROW_EXPORT Bool : public LogicalType {
 public:
  Bool() : LogicalType(LogicalType::BOOL) {}
  bool IsInstance(const Expr& expr) const override;
  std::string ToString() const override;
};

/// \brief Logical type for any number (integer or floating point)
class ARROW_EXPORT Number : public LogicalType {
 public:
  Number() : Number(LogicalType::NUMBER) {}
  bool IsInstance(const Expr& expr) const override;
  std::string ToString() const override;

 protected:
  explicit Number(Id type_id) : LogicalType(type_id) {}
};

/// \brief Logical type for any integer
class ARROW_EXPORT Integer : public Number {
 public:
  Integer() : Integer(LogicalType::INTEGER) {}
  bool IsInstance(const Expr& expr) const override;
  std::string ToString() const override;

 protected:
  explicit Integer(Id type_id) : Number(type_id) {}
};

/// \brief Logical type for any floating point number
class ARROW_EXPORT Floating : public Number {
 public:
  Floating() : Floating(LogicalType::FLOATING) {}
  bool IsInstance(const Expr& expr) const override;
  std::string ToString() const override;

 protected:
  explicit Floating(Id type_id) : Number(type_id) {}
};

/// \brief Logical type for any signed integer
class ARROW_EXPORT SignedInteger : public Integer {
 public:
  SignedInteger() : SignedInteger(LogicalType::SIGNED_INTEGER) {}
  bool IsInstance(const Expr& expr) const override;
  std::string ToString() const override;

 protected:
  explicit SignedInteger(Id type_id) : Integer(type_id) {}
};

/// \brief Logical type for any unsigned integer
class ARROW_EXPORT UnsignedInteger : public Integer {
 public:
  UnsignedInteger() : UnsignedInteger(LogicalType::UNSIGNED_INTEGER) {}
  bool IsInstance(const Expr& expr) const override;
  std::string ToString() const override;

 protected:
  explicit UnsignedInteger(Id type_id) : Integer(type_id) {}
};

/// \brief Logical type for int8
class ARROW_EXPORT Int8 : public SignedInteger {
 public:
  Int8() : SignedInteger(LogicalType::INT8) {}
  bool IsInstance(const Expr& expr) const override;
  std::string ToString() const override;
};

/// \brief Logical type for int16
class ARROW_EXPORT Int16 : public SignedInteger {
 public:
  Int16() : SignedInteger(LogicalType::INT16) {}
  bool IsInstance(const Expr& expr) const override;
  std::string ToString() const override;
};

/// \brief Logical type for int32
class ARROW_EXPORT Int32 : public SignedInteger {
 public:
  Int32() : SignedInteger(LogicalType::INT32) {}
  bool IsInstance(const Expr& expr) const override;
  std::string ToString() const override;
};

/// \brief Logical type for int64
class ARROW_EXPORT Int64 : public SignedInteger {
 public:
  Int64() : SignedInteger(LogicalType::INT64) {}
  bool IsInstance(const Expr& expr) const override;
  std::string ToString() const override;
};

/// \brief Logical type for uint8
class ARROW_EXPORT UInt8 : public UnsignedInteger {
 public:
  UInt8() : UnsignedInteger(LogicalType::UINT8) {}
  bool IsInstance(const Expr& expr) const override;
  std::string ToString() const override;
};

/// \brief Logical type for uint16
class ARROW_EXPORT UInt16 : public UnsignedInteger {
 public:
  UInt16() : UnsignedInteger(LogicalType::UINT16) {}
  bool IsInstance(const Expr& expr) const override;
  std::string ToString() const override;
};

/// \brief Logical type for uint32
class ARROW_EXPORT UInt32 : public UnsignedInteger {
 public:
  UInt32() : UnsignedInteger(LogicalType::UINT32) {}
  bool IsInstance(const Expr& expr) const override;
  std::string ToString() const override;
};

/// \brief Logical type for uint64
class ARROW_EXPORT UInt64 : public UnsignedInteger {
 public:
  UInt64() : UnsignedInteger(LogicalType::UINT64) {}
  bool IsInstance(const Expr& expr) const override;
  std::string ToString() const override;
};

/// \brief Logical type for 16-bit floating point
class ARROW_EXPORT Float16 : public Floating {
 public:
  Float16() : Floating(LogicalType::FLOAT16) {}
  bool IsInstance(const Expr& expr) const override;
  std::string ToString() const override;
};

/// \brief Logical type for 32-bit floating point
class ARROW_EXPORT Float32 : public Floating {
 public:
  Float32() : Floating(LogicalType::FLOAT32) {}
  bool IsInstance(const Expr& expr) const override;
  std::string ToString() const override;
};

/// \brief Logical type for 64-bit floating point
class ARROW_EXPORT Float64 : public Floating {
 public:
  Float64() : Floating(LogicalType::FLOAT64) {}
  bool IsInstance(const Expr& expr) const override;
  std::string ToString() const override;
};

/// \brief Logical type for variable-size binary
class ARROW_EXPORT Binary : public LogicalType {
 public:
  Binary() : Binary(LogicalType::BINARY) {}
  bool IsInstance(const Expr& expr) const override;
  std::string ToString() const override;

 protected:
  explicit Binary(Id type_id) : LogicalType(type_id) {}
};

/// \brief Logical type for variable-size binary
class ARROW_EXPORT Utf8 : public Binary {
 public:
  Utf8() : Binary(LogicalType::UTF8) {}
  bool IsInstance(const Expr& expr) const override;
  std::string ToString() const override;
};

#define SIMPLE_TYPE_FACTORY(NAME) ARROW_EXPORT LogicalTypePtr NAME();

SIMPLE_TYPE_FACTORY(any);
SIMPLE_TYPE_FACTORY(null);
SIMPLE_TYPE_FACTORY(boolean);
SIMPLE_TYPE_FACTORY(number);
SIMPLE_TYPE_FACTORY(integer);
SIMPLE_TYPE_FACTORY(signed_integer);
SIMPLE_TYPE_FACTORY(unsigned_integer);
SIMPLE_TYPE_FACTORY(floating);
SIMPLE_TYPE_FACTORY(int8);
SIMPLE_TYPE_FACTORY(int16);
SIMPLE_TYPE_FACTORY(int32);
SIMPLE_TYPE_FACTORY(int64);
SIMPLE_TYPE_FACTORY(uint8);
SIMPLE_TYPE_FACTORY(uint16);
SIMPLE_TYPE_FACTORY(uint32);
SIMPLE_TYPE_FACTORY(uint64);
SIMPLE_TYPE_FACTORY(float16);
SIMPLE_TYPE_FACTORY(float32);
SIMPLE_TYPE_FACTORY(float64);
SIMPLE_TYPE_FACTORY(binary);
SIMPLE_TYPE_FACTORY(utf8);

#undef SIMPLE_TYPE_FACTORY

}  // namespace type
}  // namespace compute
}  // namespace arrow

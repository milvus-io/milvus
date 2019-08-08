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

#ifndef ARROW_TYPE_H
#define ARROW_TYPE_H

#include <climits>
#include <cstdint>
#include <iosfwd>
#include <memory>
#include <string>
#include <vector>

#include "arrow/status.h"
#include "arrow/type_fwd.h"  // IWYU pragma: export
#include "arrow/util/checked_cast.h"
#include "arrow/util/macros.h"
#include "arrow/util/visibility.h"
#include "arrow/visitor.h"  // IWYU pragma: keep

namespace arrow {

class Array;
class Field;
class MemoryPool;

struct Type {
  /// \brief Main data type enumeration
  ///
  /// This enumeration provides a quick way to interrogate the category
  /// of a DataType instance.
  enum type {
    /// A NULL type having no physical storage
    NA,

    /// Boolean as 1 bit, LSB bit-packed ordering
    BOOL,

    /// Unsigned 8-bit little-endian integer
    UINT8,

    /// Signed 8-bit little-endian integer
    INT8,

    /// Unsigned 16-bit little-endian integer
    UINT16,

    /// Signed 16-bit little-endian integer
    INT16,

    /// Unsigned 32-bit little-endian integer
    UINT32,

    /// Signed 32-bit little-endian integer
    INT32,

    /// Unsigned 64-bit little-endian integer
    UINT64,

    /// Signed 64-bit little-endian integer
    INT64,

    /// 2-byte floating point value
    HALF_FLOAT,

    /// 4-byte floating point value
    FLOAT,

    /// 8-byte floating point value
    DOUBLE,

    /// UTF8 variable-length string as List<Char>
    STRING,

    /// Variable-length bytes (no guarantee of UTF8-ness)
    BINARY,

    /// Fixed-size binary. Each value occupies the same number of bytes
    FIXED_SIZE_BINARY,

    /// int32_t days since the UNIX epoch
    DATE32,

    /// int64_t milliseconds since the UNIX epoch
    DATE64,

    /// Exact timestamp encoded with int64 since UNIX epoch
    /// Default unit millisecond
    TIMESTAMP,

    /// Time as signed 32-bit integer, representing either seconds or
    /// milliseconds since midnight
    TIME32,

    /// Time as signed 64-bit integer, representing either microseconds or
    /// nanoseconds since midnight
    TIME64,

    /// YEAR_MONTH or DAY_TIME interval in SQL style
    INTERVAL,

    /// Precision- and scale-based decimal type. Storage type depends on the
    /// parameters.
    DECIMAL,

    /// A list of some logical data type
    LIST,

    /// Struct of logical types
    STRUCT,

    /// Unions of logical types
    UNION,

    /// Dictionary-encoded type, also called "categorical" or "factor"
    /// in other programming languages. Holds the dictionary value
    /// type but not the dictionary itself, which is part of the
    /// ArrayData struct
    DICTIONARY,

    /// Map, a repeated struct logical type
    MAP,

    /// Custom data type, implemented by user
    EXTENSION,

    /// Fixed size list of some logical type
    FIXED_SIZE_LIST,

    /// Measure of elapsed time in either seconds, milliseconds, microseconds
    /// or nanoseconds.
    DURATION
  };
};

struct ARROW_EXPORT DataTypeLayout {
  // The bit width for each buffer in this DataType's representation
  // (kVariableSizeBuffer if the item size for a given buffer is unknown or variable,
  //  kAlwaysNullBuffer if the buffer is always null).
  // Child types are not included, they should be inspected separately.
  std::vector<int64_t> bit_widths;
  bool has_dictionary;

  static constexpr int64_t kAlwaysNullBuffer = 0;
  static constexpr int64_t kVariableSizeBuffer = -1;
};

/// \brief Base class for all data types
///
/// Data types in this library are all *logical*. They can be expressed as
/// either a primitive physical type (bytes or bits of some fixed size), a
/// nested type consisting of other data types, or another data type (e.g. a
/// timestamp encoded as an int64).
///
/// Simple datatypes may be entirely described by their Type::type id, but
/// complex datatypes are usually parametric.
class ARROW_EXPORT DataType {
 public:
  explicit DataType(Type::type id) : id_(id) {}
  virtual ~DataType();

  /// \brief Return whether the types are equal
  ///
  /// Types that are logically convertible from one to another (e.g. List<UInt8>
  /// and Binary) are NOT equal.
  bool Equals(const DataType& other, bool check_metadata = true) const;

  /// \brief Return whether the types are equal
  bool Equals(const std::shared_ptr<DataType>& other) const;

  std::shared_ptr<Field> child(int i) const { return children_[i]; }

  const std::vector<std::shared_ptr<Field>>& children() const { return children_; }

  int num_children() const { return static_cast<int>(children_.size()); }

  Status Accept(TypeVisitor* visitor) const;

  /// \brief A string representation of the type, including any children
  virtual std::string ToString() const = 0;

  /// \brief A string name of the type, omitting any child fields
  ///
  /// \note Experimental API
  /// \since 0.7.0
  virtual std::string name() const = 0;

  /// \brief Return the data type layout.  Children are not included.
  ///
  /// \note Experimental API
  virtual DataTypeLayout layout() const = 0;

  /// \brief Return the type category
  Type::type id() const { return id_; }

 protected:
  Type::type id_;
  std::vector<std::shared_ptr<Field>> children_;

 private:
  ARROW_DISALLOW_COPY_AND_ASSIGN(DataType);
};

std::ostream& operator<<(std::ostream& os, const DataType& type);

/// \brief Base class for all fixed-width data types
class ARROW_EXPORT FixedWidthType : public DataType {
 public:
  using DataType::DataType;

  virtual int bit_width() const = 0;
};

/// \brief Base class for all data types representing primitive values
class ARROW_EXPORT PrimitiveCType : public FixedWidthType {
 public:
  using FixedWidthType::FixedWidthType;
};

/// \brief Base class for all numeric data types
class ARROW_EXPORT NumberType : public PrimitiveCType {
 public:
  using PrimitiveCType::PrimitiveCType;
};

/// \brief Base class for all integral data types
class ARROW_EXPORT IntegerType : public NumberType {
 public:
  using NumberType::NumberType;
  virtual bool is_signed() const = 0;
};

/// \brief Base class for all floating-point data types
class ARROW_EXPORT FloatingPointType : public NumberType {
 public:
  using NumberType::NumberType;
  enum Precision { HALF, SINGLE, DOUBLE };
  virtual Precision precision() const = 0;
};

/// \brief Base class for all parametric data types
class ParametricType {};

class ARROW_EXPORT NestedType : public DataType, public ParametricType {
 public:
  using DataType::DataType;
};

class NoExtraMeta {};

/// \brief The combination of a field name and data type, with optional metadata
///
/// Fields are used to describe the individual constituents of a
/// nested DataType or a Schema.
///
/// A field's metadata is represented by a KeyValueMetadata instance,
/// which holds arbitrary key-value pairs.
class ARROW_EXPORT Field {
 public:
  Field(const std::string& name, const std::shared_ptr<DataType>& type,
        bool nullable = true,
        const std::shared_ptr<const KeyValueMetadata>& metadata = NULLPTR)
      : name_(name), type_(type), nullable_(nullable), metadata_(metadata) {}

  /// \brief Return the field's attached metadata
  std::shared_ptr<const KeyValueMetadata> metadata() const { return metadata_; }

  /// \brief Return whether the field has non-empty metadata
  bool HasMetadata() const;

  /// \brief Return a copy of this field with the given metadata attached to it
  std::shared_ptr<Field> AddMetadata(
      const std::shared_ptr<const KeyValueMetadata>& metadata) const;
  /// \brief Return a copy of this field without any metadata attached to it
  std::shared_ptr<Field> RemoveMetadata() const;

  /// \brief Return a copy of this field with the replaced type.
  std::shared_ptr<Field> WithType(const std::shared_ptr<DataType>& type) const;

  /// \brief Return a copy of this field with the replaced name.
  std::shared_ptr<Field> WithName(const std::string& name) const;

  std::vector<std::shared_ptr<Field>> Flatten() const;

  bool Equals(const Field& other, bool check_metadata = true) const;
  bool Equals(const std::shared_ptr<Field>& other, bool check_metadata = true) const;

  /// \brief Return a string representation ot the field
  std::string ToString() const;

  /// \brief Return the field name
  const std::string& name() const { return name_; }
  /// \brief Return the field data type
  std::shared_ptr<DataType> type() const { return type_; }
  /// \brief Return whether the field is nullable
  bool nullable() const { return nullable_; }

  std::shared_ptr<Field> Copy() const;

 private:
  // Field name
  std::string name_;

  // The field's data type
  std::shared_ptr<DataType> type_;

  // Fields can be nullable
  bool nullable_;

  // The field's metadata, if any
  std::shared_ptr<const KeyValueMetadata> metadata_;

  ARROW_DISALLOW_COPY_AND_ASSIGN(Field);
};

namespace detail {

template <typename DERIVED, typename BASE, Type::type TYPE_ID, typename C_TYPE>
class ARROW_EXPORT CTypeImpl : public BASE {
 public:
  using c_type = C_TYPE;
  static constexpr Type::type type_id = TYPE_ID;

  CTypeImpl() : BASE(TYPE_ID) {}

  int bit_width() const override { return static_cast<int>(sizeof(C_TYPE) * CHAR_BIT); }

  DataTypeLayout layout() const override { return {{1, bit_width()}, false}; }

  std::string ToString() const override { return this->name(); }
};

template <typename DERIVED, Type::type TYPE_ID, typename C_TYPE>
class IntegerTypeImpl : public detail::CTypeImpl<DERIVED, IntegerType, TYPE_ID, C_TYPE> {
  bool is_signed() const override { return std::is_signed<C_TYPE>::value; }
};

}  // namespace detail

/// Concrete type class for always-null data
class ARROW_EXPORT NullType : public DataType, public NoExtraMeta {
 public:
  static constexpr Type::type type_id = Type::NA;

  NullType() : DataType(Type::NA) {}

  std::string ToString() const override;

  DataTypeLayout layout() const override {
    return {{DataTypeLayout::kAlwaysNullBuffer}, false};
  }

  std::string name() const override { return "null"; }
};

/// Concrete type class for boolean data
class ARROW_EXPORT BooleanType : public FixedWidthType, public NoExtraMeta {
 public:
  static constexpr Type::type type_id = Type::BOOL;

  BooleanType() : FixedWidthType(Type::BOOL) {}

  std::string ToString() const override;

  DataTypeLayout layout() const override { return {{1, 1}, false}; }

  int bit_width() const override { return 1; }
  std::string name() const override { return "bool"; }
};

/// Concrete type class for unsigned 8-bit integer data
class ARROW_EXPORT UInt8Type
    : public detail::IntegerTypeImpl<UInt8Type, Type::UINT8, uint8_t> {
 public:
  std::string name() const override { return "uint8"; }
};

/// Concrete type class for signed 8-bit integer data
class ARROW_EXPORT Int8Type
    : public detail::IntegerTypeImpl<Int8Type, Type::INT8, int8_t> {
 public:
  std::string name() const override { return "int8"; }
};

/// Concrete type class for unsigned 16-bit integer data
class ARROW_EXPORT UInt16Type
    : public detail::IntegerTypeImpl<UInt16Type, Type::UINT16, uint16_t> {
 public:
  std::string name() const override { return "uint16"; }
};

/// Concrete type class for signed 16-bit integer data
class ARROW_EXPORT Int16Type
    : public detail::IntegerTypeImpl<Int16Type, Type::INT16, int16_t> {
 public:
  std::string name() const override { return "int16"; }
};

/// Concrete type class for unsigned 32-bit integer data
class ARROW_EXPORT UInt32Type
    : public detail::IntegerTypeImpl<UInt32Type, Type::UINT32, uint32_t> {
 public:
  std::string name() const override { return "uint32"; }
};

/// Concrete type class for signed 32-bit integer data
class ARROW_EXPORT Int32Type
    : public detail::IntegerTypeImpl<Int32Type, Type::INT32, int32_t> {
 public:
  std::string name() const override { return "int32"; }
};

/// Concrete type class for unsigned 64-bit integer data
class ARROW_EXPORT UInt64Type
    : public detail::IntegerTypeImpl<UInt64Type, Type::UINT64, uint64_t> {
 public:
  std::string name() const override { return "uint64"; }
};

/// Concrete type class for signed 64-bit integer data
class ARROW_EXPORT Int64Type
    : public detail::IntegerTypeImpl<Int64Type, Type::INT64, int64_t> {
 public:
  std::string name() const override { return "int64"; }
};

/// Concrete type class for 16-bit floating-point data
class ARROW_EXPORT HalfFloatType
    : public detail::CTypeImpl<HalfFloatType, FloatingPointType, Type::HALF_FLOAT,
                               uint16_t> {
 public:
  Precision precision() const override;
  std::string name() const override { return "halffloat"; }
};

/// Concrete type class for 32-bit floating-point data (C "float")
class ARROW_EXPORT FloatType
    : public detail::CTypeImpl<FloatType, FloatingPointType, Type::FLOAT, float> {
 public:
  Precision precision() const override;
  std::string name() const override { return "float"; }
};

/// Concrete type class for 64-bit floating-point data (C "double")
class ARROW_EXPORT DoubleType
    : public detail::CTypeImpl<DoubleType, FloatingPointType, Type::DOUBLE, double> {
 public:
  Precision precision() const override;
  std::string name() const override { return "double"; }
};

/// \brief Concrete type class for list data
///
/// List data is nested data where each value is a variable number of
/// child items.  Lists can be recursively nested, for example
/// list(list(int32)).
class ARROW_EXPORT ListType : public NestedType {
 public:
  static constexpr Type::type type_id = Type::LIST;

  // List can contain any other logical value type
  explicit ListType(const std::shared_ptr<DataType>& value_type)
      : ListType(std::make_shared<Field>("item", value_type)) {}

  explicit ListType(const std::shared_ptr<Field>& value_field) : NestedType(Type::LIST) {
    children_ = {value_field};
  }

  std::shared_ptr<Field> value_field() const { return children_[0]; }

  std::shared_ptr<DataType> value_type() const { return children_[0]->type(); }

  DataTypeLayout layout() const override {
    return {{1, CHAR_BIT * sizeof(int32_t)}, false};
  }

  std::string ToString() const override;

  std::string name() const override { return "list"; }
};

/// \brief Concrete type class for map data
///
/// Map data is nested data where each value is a variable number of
/// key-item pairs.  Maps can be recursively nested, for example
/// map(utf8, map(utf8, int32)).
class ARROW_EXPORT MapType : public ListType {
 public:
  static constexpr Type::type type_id = Type::MAP;

  MapType(const std::shared_ptr<DataType>& key_type,
          const std::shared_ptr<DataType>& item_type, bool keys_sorted = false);

  std::shared_ptr<DataType> key_type() const { return value_type()->child(0)->type(); }

  std::shared_ptr<DataType> item_type() const { return value_type()->child(1)->type(); }

  std::string ToString() const override;

  std::string name() const override { return "map"; }

  bool keys_sorted() const { return keys_sorted_; }

 private:
  bool keys_sorted_;
};

/// \brief Concrete type class for fixed size list data
class ARROW_EXPORT FixedSizeListType : public NestedType {
 public:
  static constexpr Type::type type_id = Type::FIXED_SIZE_LIST;

  // List can contain any other logical value type
  FixedSizeListType(const std::shared_ptr<DataType>& value_type, int32_t list_size)
      : FixedSizeListType(std::make_shared<Field>("item", value_type), list_size) {}

  FixedSizeListType(const std::shared_ptr<Field>& value_field, int32_t list_size)
      : NestedType(type_id), list_size_(list_size) {
    children_ = {value_field};
  }

  std::shared_ptr<Field> value_field() const { return children_[0]; }

  std::shared_ptr<DataType> value_type() const { return children_[0]->type(); }

  DataTypeLayout layout() const override { return {{1}, false}; }

  std::string ToString() const override;

  std::string name() const override { return "fixed_size_list"; }

  int32_t list_size() const { return list_size_; }

 protected:
  int32_t list_size_;
};

/// \brief Concrete type class for variable-size binary data
class ARROW_EXPORT BinaryType : public DataType, public NoExtraMeta {
 public:
  static constexpr Type::type type_id = Type::BINARY;

  BinaryType() : BinaryType(Type::BINARY) {}

  DataTypeLayout layout() const override {
    return {{1, CHAR_BIT * sizeof(int32_t), DataTypeLayout::kVariableSizeBuffer}, false};
  }

  std::string ToString() const override;
  std::string name() const override { return "binary"; }

 protected:
  // Allow subclasses to change the logical type.
  explicit BinaryType(Type::type logical_type) : DataType(logical_type) {}
};

/// \brief Concrete type class for fixed-size binary data
class ARROW_EXPORT FixedSizeBinaryType : public FixedWidthType, public ParametricType {
 public:
  static constexpr Type::type type_id = Type::FIXED_SIZE_BINARY;

  explicit FixedSizeBinaryType(int32_t byte_width)
      : FixedWidthType(Type::FIXED_SIZE_BINARY), byte_width_(byte_width) {}
  explicit FixedSizeBinaryType(int32_t byte_width, Type::type override_type_id)
      : FixedWidthType(override_type_id), byte_width_(byte_width) {}

  std::string ToString() const override;
  std::string name() const override { return "fixed_size_binary"; }

  DataTypeLayout layout() const override { return {{1, bit_width()}, false}; }

  int32_t byte_width() const { return byte_width_; }
  int bit_width() const override;

 protected:
  int32_t byte_width_;
};

/// \brief Concrete type class for variable-size string data, utf8-encoded
class ARROW_EXPORT StringType : public BinaryType {
 public:
  static constexpr Type::type type_id = Type::STRING;

  StringType() : BinaryType(Type::STRING) {}

  std::string ToString() const override;
  std::string name() const override { return "utf8"; }
};

/// \brief Concrete type class for struct data
class ARROW_EXPORT StructType : public NestedType {
 public:
  static constexpr Type::type type_id = Type::STRUCT;

  explicit StructType(const std::vector<std::shared_ptr<Field>>& fields);

  ~StructType() override;

  DataTypeLayout layout() const override { return {{1}, false}; }

  std::string ToString() const override;
  std::string name() const override { return "struct"; }

  /// Returns null if name not found
  std::shared_ptr<Field> GetFieldByName(const std::string& name) const;

  /// Return all fields having this name
  std::vector<std::shared_ptr<Field>> GetAllFieldsByName(const std::string& name) const;

  /// Returns -1 if name not found or if there are multiple fields having the
  /// same name
  int GetFieldIndex(const std::string& name) const;

  /// Return the indices of all fields having this name
  std::vector<int> GetAllFieldIndices(const std::string& name) const;

  ARROW_DEPRECATED("Use GetFieldByName")
  std::shared_ptr<Field> GetChildByName(const std::string& name) const;

  ARROW_DEPRECATED("Use GetFieldIndex")
  int GetChildIndex(const std::string& name) const;

 private:
  class Impl;
  std::unique_ptr<Impl> impl_;
};

/// \brief Base type class for (fixed-size) decimal data
class ARROW_EXPORT DecimalType : public FixedSizeBinaryType {
 public:
  explicit DecimalType(int32_t byte_width, int32_t precision, int32_t scale)
      : FixedSizeBinaryType(byte_width, Type::DECIMAL),
        precision_(precision),
        scale_(scale) {}

  int32_t precision() const { return precision_; }
  int32_t scale() const { return scale_; }

 protected:
  int32_t precision_;
  int32_t scale_;
};

/// \brief Concrete type class for 128-bit decimal data
class ARROW_EXPORT Decimal128Type : public DecimalType {
 public:
  static constexpr Type::type type_id = Type::DECIMAL;

  explicit Decimal128Type(int32_t precision, int32_t scale);

  std::string ToString() const override;
  std::string name() const override { return "decimal"; }
};

struct UnionMode {
  enum type { SPARSE, DENSE };
};

/// \brief Concrete type class for union data
class ARROW_EXPORT UnionType : public NestedType {
 public:
  static constexpr Type::type type_id = Type::UNION;

  UnionType(const std::vector<std::shared_ptr<Field>>& fields,
            const std::vector<uint8_t>& type_codes,
            UnionMode::type mode = UnionMode::SPARSE);

  DataTypeLayout layout() const override;

  std::string ToString() const override;
  std::string name() const override { return "union"; }

  const std::vector<uint8_t>& type_codes() const { return type_codes_; }

  UnionMode::type mode() const { return mode_; }

 private:
  UnionMode::type mode_;

  // The type id used in the data to indicate each data type in the union. For
  // example, the first type in the union might be denoted by the id 5 (instead
  // of 0).
  std::vector<uint8_t> type_codes_;
};

// ----------------------------------------------------------------------
// Date and time types

enum class DateUnit : char { DAY = 0, MILLI = 1 };

/// \brief Base type for all date and time types
class ARROW_EXPORT TemporalType : public FixedWidthType {
 public:
  using FixedWidthType::FixedWidthType;

  DataTypeLayout layout() const override { return {{1, bit_width()}, false}; }
};

/// \brief Base type class for date data
class ARROW_EXPORT DateType : public TemporalType {
 public:
  virtual DateUnit unit() const = 0;

 protected:
  explicit DateType(Type::type type_id);
};

/// Concrete type class for 32-bit date data (as number of days since UNIX epoch)
class ARROW_EXPORT Date32Type : public DateType {
 public:
  static constexpr Type::type type_id = Type::DATE32;
  static constexpr DateUnit UNIT = DateUnit::DAY;

  using c_type = int32_t;

  Date32Type();

  int bit_width() const override { return static_cast<int>(sizeof(c_type) * CHAR_BIT); }

  std::string ToString() const override;

  std::string name() const override { return "date32"; }
  DateUnit unit() const override { return UNIT; }
};

/// Concrete type class for 64-bit date data (as number of milliseconds since UNIX epoch)
class ARROW_EXPORT Date64Type : public DateType {
 public:
  static constexpr Type::type type_id = Type::DATE64;
  static constexpr DateUnit UNIT = DateUnit::MILLI;

  using c_type = int64_t;

  Date64Type();

  int bit_width() const override { return static_cast<int>(sizeof(c_type) * CHAR_BIT); }

  std::string ToString() const override;

  std::string name() const override { return "date64"; }
  DateUnit unit() const override { return UNIT; }
};

struct TimeUnit {
  /// The unit for a time or timestamp DataType
  enum type { SECOND = 0, MILLI = 1, MICRO = 2, NANO = 3 };
};

std::ostream& operator<<(std::ostream& os, TimeUnit::type unit);

/// Base type class for time data
class ARROW_EXPORT TimeType : public TemporalType, public ParametricType {
 public:
  TimeUnit::type unit() const { return unit_; }

 protected:
  TimeType(Type::type type_id, TimeUnit::type unit);
  TimeUnit::type unit_;
};

/// Concrete type class for 32-bit time data (as number of seconds or milliseconds
/// since midnight)
class ARROW_EXPORT Time32Type : public TimeType {
 public:
  static constexpr Type::type type_id = Type::TIME32;
  using c_type = int32_t;

  int bit_width() const override { return static_cast<int>(sizeof(c_type) * CHAR_BIT); }

  explicit Time32Type(TimeUnit::type unit = TimeUnit::MILLI);

  std::string ToString() const override;

  std::string name() const override { return "time32"; }
};

/// Concrete type class for 64-bit time data (as number of microseconds or nanoseconds
/// since midnight)
class ARROW_EXPORT Time64Type : public TimeType {
 public:
  static constexpr Type::type type_id = Type::TIME64;
  using c_type = int64_t;

  int bit_width() const override { return static_cast<int>(sizeof(c_type) * CHAR_BIT); }

  explicit Time64Type(TimeUnit::type unit = TimeUnit::NANO);

  std::string ToString() const override;

  std::string name() const override { return "time64"; }
};

/// \brief Concrete type class for datetime data (as number of seconds, milliseconds,
/// microseconds or nanoseconds since UNIX epoch)
///
/// If supplied, the timezone string should take either the form (i) "Area/Location",
/// with values drawn from the names in the IANA Time Zone Database (such as
/// "Europe/Zurich"); or (ii) "(+|-)HH:MM" indicating an absolute offset from GMT
/// (such as "-08:00").  To indicate a native UTC timestamp, one of the strings "UTC",
/// "Etc/UTC" or "+00:00" should be used.
///
/// If any non-empty string is supplied as the timezone for a TimestampType, then the
/// Arrow field containing that timestamp type (and by extension the column associated
/// with such a field) is considered "timezone-aware".  The integer arrays that comprise
/// a timezone-aware column must contain UTC normalized datetime values, regardless of
/// the contents of their timezone string.  More precisely, (i) the producer of a
/// timezone-aware column must populate its constituent arrays with valid UTC values
/// (performing offset conversions from non-UTC values if necessary); and (ii) the
/// consumer of a timezone-aware column may assume that the column's values are directly
/// comparable (that is, with no offset adjustment required) to the values of any other
/// timezone-aware column or to any other valid UTC datetime value (provided all values
/// are expressed in the same units).
///
/// If a TimestampType is constructed without a timezone (or, equivalently, if the
/// timezone supplied is an empty string) then the resulting Arrow field (column) is
/// considered "timezone-naive".  The producer of a timezone-naive column may populate
/// its constituent integer arrays with datetime values from any timezone; the consumer
/// of a timezone-naive column should make no assumptions about the interoperability or
/// comparability of the values of such a column with those of any other timestamp
/// column or datetime value.
///
/// If a timezone-aware field contains a recognized timezone, its values may be
/// localized to that locale upon display; the values of timezone-naive fields must
/// always be displayed "as is", with no localization performed on them.
class ARROW_EXPORT TimestampType : public TemporalType, public ParametricType {
 public:
  using Unit = TimeUnit;

  typedef int64_t c_type;
  static constexpr Type::type type_id = Type::TIMESTAMP;

  int bit_width() const override { return static_cast<int>(sizeof(int64_t) * CHAR_BIT); }

  explicit TimestampType(TimeUnit::type unit = TimeUnit::MILLI)
      : TemporalType(Type::TIMESTAMP), unit_(unit) {}

  explicit TimestampType(TimeUnit::type unit, const std::string& timezone)
      : TemporalType(Type::TIMESTAMP), unit_(unit), timezone_(timezone) {}

  std::string ToString() const override;
  std::string name() const override { return "timestamp"; }

  TimeUnit::type unit() const { return unit_; }
  const std::string& timezone() const { return timezone_; }

 private:
  TimeUnit::type unit_;
  std::string timezone_;
};

// Base class for the different kinds of intervals.
class ARROW_EXPORT IntervalType : public TemporalType, public ParametricType {
 public:
  enum type { MONTHS, DAY_TIME };
  IntervalType() : TemporalType(Type::INTERVAL) {}

  virtual type interval_type() const = 0;
  virtual ~IntervalType() = default;
};

/// \brief Represents a some number of months.
///
/// Type representing a number of months.  Corresponeds to YearMonth type
/// in Schema.fbs (Years are defined as 12 months).
class ARROW_EXPORT MonthIntervalType : public IntervalType {
 public:
  using c_type = int32_t;
  static constexpr Type::type type_id = Type::INTERVAL;

  IntervalType::type interval_type() const override { return IntervalType::MONTHS; }

  int bit_width() const override { return static_cast<int>(sizeof(c_type) * CHAR_BIT); }

  MonthIntervalType() : IntervalType() {}

  std::string ToString() const override { return name(); }
  std::string name() const override { return "month_interval"; }
};

/// \brief Represents a number of days and milliseconds (fraction of day).
class ARROW_EXPORT DayTimeIntervalType : public IntervalType {
 public:
  struct DayMilliseconds {
    int32_t days;
    int32_t milliseconds;
    bool operator==(DayMilliseconds other) const {
      return this->days == other.days && this->milliseconds == other.milliseconds;
    }
    bool operator!=(DayMilliseconds other) const { return !(*this == other); }
  };
  using c_type = DayMilliseconds;
  static_assert(sizeof(DayMilliseconds) == 8,
                "DayMilliseconds struct assumed to be of size 8 bytes");
  static constexpr Type::type type_id = Type::INTERVAL;
  IntervalType::type interval_type() const override { return IntervalType::DAY_TIME; }

  DayTimeIntervalType() : IntervalType() {}

  int bit_width() const override { return static_cast<int>(sizeof(c_type) * CHAR_BIT); }

  std::string ToString() const override { return name(); }
  std::string name() const override { return "day_time_interval"; }
};

// \brief Represents an amount of elapsed time without any relation to a calendar
// artifact.
class ARROW_EXPORT DurationType : public TemporalType, public ParametricType {
 public:
  using Unit = TimeUnit;

  static constexpr Type::type type_id = Type::DURATION;
  using c_type = int64_t;

  int bit_width() const override { return static_cast<int>(sizeof(int64_t) * CHAR_BIT); }

  explicit DurationType(TimeUnit::type unit = TimeUnit::MILLI)
      : TemporalType(Type::DURATION), unit_(unit) {}

  std::string ToString() const override;
  std::string name() const override { return "duration"; }

  TimeUnit::type unit() const { return unit_; }

 private:
  TimeUnit::type unit_;
};

// ----------------------------------------------------------------------
// Dictionary type (for representing categorical or dictionary-encoded
// in memory)

/// \brief Dictionary-encoded value type with data-dependent
/// dictionary
class ARROW_EXPORT DictionaryType : public FixedWidthType {
 public:
  static constexpr Type::type type_id = Type::DICTIONARY;

  DictionaryType(const std::shared_ptr<DataType>& index_type,
                 const std::shared_ptr<DataType>& value_type, bool ordered = false);

  std::string ToString() const override;
  std::string name() const override { return "dictionary"; }

  int bit_width() const override;

  DataTypeLayout layout() const override;

  std::shared_ptr<DataType> index_type() const { return index_type_; }
  std::shared_ptr<DataType> value_type() const { return value_type_; }

  bool ordered() const { return ordered_; }

  /// \brief Unify dictionaries types
  ///
  /// Compute a resulting dictionary that will allow the union of values
  /// of all input dictionary types.  The input types must all have the
  /// same value type.
  /// \param[in] pool Memory pool to allocate dictionary values from
  /// \param[in] types A sequence of input dictionary types
  /// \param[in] dictionaries A sequence of input dictionaries
  /// corresponding to each type
  /// \param[out] out_type The unified dictionary type
  /// \param[out] out_dictionary The unified dictionary
  /// \param[out] out_transpose_maps (optionally) A sequence of integer vectors,
  ///     one per input type.  Each integer vector represents the transposition
  ///     of input type indices into unified type indices.
  // XXX Should we return something special (an empty transpose map?) when
  // the transposition is the identity function?
  static Status Unify(MemoryPool* pool, const std::vector<const DataType*>& types,
                      const std::vector<const Array*>& dictionaries,
                      std::shared_ptr<DataType>* out_type,
                      std::shared_ptr<Array>* out_dictionary,
                      std::vector<std::vector<int32_t>>* out_transpose_maps = NULLPTR);

 protected:
  // Must be an integer type (not currently checked)
  std::shared_ptr<DataType> index_type_;
  std::shared_ptr<DataType> value_type_;
  bool ordered_;
};

// ----------------------------------------------------------------------
// Schema

/// \class Schema
/// \brief Sequence of arrow::Field objects describing the columns of a record
/// batch or table data structure
class ARROW_EXPORT Schema {
 public:
  explicit Schema(const std::vector<std::shared_ptr<Field>>& fields,
                  const std::shared_ptr<const KeyValueMetadata>& metadata = NULLPTR);

  explicit Schema(std::vector<std::shared_ptr<Field>>&& fields,
                  const std::shared_ptr<const KeyValueMetadata>& metadata = NULLPTR);

  Schema(const Schema&);

  virtual ~Schema();

  /// Returns true if all of the schema fields are equal
  bool Equals(const Schema& other, bool check_metadata = true) const;

  /// \brief Return the number of fields (columns) in the schema
  int num_fields() const;

  /// Return the ith schema element. Does not boundscheck
  std::shared_ptr<Field> field(int i) const;

  const std::vector<std::shared_ptr<Field>>& fields() const;

  std::vector<std::string> field_names() const;

  /// Returns null if name not found
  std::shared_ptr<Field> GetFieldByName(const std::string& name) const;

  /// Return all fields having this name
  std::vector<std::shared_ptr<Field>> GetAllFieldsByName(const std::string& name) const;

  /// Returns -1 if name not found
  int GetFieldIndex(const std::string& name) const;

  /// Return the indices of all fields having this name
  std::vector<int> GetAllFieldIndices(const std::string& name) const;

  /// \brief The custom key-value metadata, if any
  ///
  /// \return metadata may be null
  std::shared_ptr<const KeyValueMetadata> metadata() const;

  /// \brief Render a string representation of the schema suitable for debugging
  std::string ToString() const;

  Status AddField(int i, const std::shared_ptr<Field>& field,
                  std::shared_ptr<Schema>* out) const;
  Status RemoveField(int i, std::shared_ptr<Schema>* out) const;
  Status SetField(int i, const std::shared_ptr<Field>& field,
                  std::shared_ptr<Schema>* out) const;

  /// \brief Replace key-value metadata with new metadata
  ///
  /// \param[in] metadata new KeyValueMetadata
  /// \return new Schema
  std::shared_ptr<Schema> AddMetadata(
      const std::shared_ptr<const KeyValueMetadata>& metadata) const;

  /// \brief Return copy of Schema without the KeyValueMetadata
  std::shared_ptr<Schema> RemoveMetadata() const;

  /// \brief Indicates that Schema has non-empty KevValueMetadata
  bool HasMetadata() const;

 private:
  class Impl;
  std::unique_ptr<Impl> impl_;
};

// ----------------------------------------------------------------------
// Parametric factory functions
// Other factory functions are in type_fwd.h

/// \addtogroup type-factories
/// @{

/// \brief Create a FixedSizeBinaryType instance.
ARROW_EXPORT
std::shared_ptr<DataType> fixed_size_binary(int32_t byte_width);

/// \brief Create a Decimal128Type instance
ARROW_EXPORT
std::shared_ptr<DataType> decimal(int32_t precision, int32_t scale);

/// \brief Create a ListType instance from its child Field type
ARROW_EXPORT
std::shared_ptr<DataType> list(const std::shared_ptr<Field>& value_type);

/// \brief Create a ListType instance from its child DataType
ARROW_EXPORT
std::shared_ptr<DataType> list(const std::shared_ptr<DataType>& value_type);

/// \brief Create a MapType instance from its key and value DataTypes
ARROW_EXPORT
std::shared_ptr<DataType> map(const std::shared_ptr<DataType>& key_type,
                              const std::shared_ptr<DataType>& value_type,
                              bool keys_sorted = false);

/// \brief Create a FixedSizeListType instance from its child Field type
ARROW_EXPORT
std::shared_ptr<DataType> fixed_size_list(const std::shared_ptr<Field>& value_type,
                                          int32_t list_size);

/// \brief Create a FixedSizeListType instance from its child DataType
ARROW_EXPORT
std::shared_ptr<DataType> fixed_size_list(const std::shared_ptr<DataType>& value_type,
                                          int32_t list_size);
/// \brief Return an Duration instance (naming use _type to avoid namespace conflict with
/// built in time clases).
std::shared_ptr<DataType> ARROW_EXPORT duration(TimeUnit::type unit);

/// \brief Return an DayTimeIntervalType instance
std::shared_ptr<DataType> ARROW_EXPORT day_time_interval();

/// \brief Return an MonthIntervalType instance
std::shared_ptr<DataType> ARROW_EXPORT month_interval();

/// \brief Create a TimestampType instance from its unit
ARROW_EXPORT
std::shared_ptr<DataType> timestamp(TimeUnit::type unit);

/// \brief Create a TimestampType instance from its unit and timezone
ARROW_EXPORT
std::shared_ptr<DataType> timestamp(TimeUnit::type unit, const std::string& timezone);

/// \brief Create a 32-bit time type instance
///
/// Unit can be either SECOND or MILLI
std::shared_ptr<DataType> ARROW_EXPORT time32(TimeUnit::type unit);

/// \brief Create a 64-bit time type instance
///
/// Unit can be either MICRO or NANO
std::shared_ptr<DataType> ARROW_EXPORT time64(TimeUnit::type unit);

/// \brief Create a StructType instance
std::shared_ptr<DataType> ARROW_EXPORT
struct_(const std::vector<std::shared_ptr<Field>>& fields);

/// \brief Create a UnionType instance
std::shared_ptr<DataType> ARROW_EXPORT
union_(const std::vector<std::shared_ptr<Field>>& child_fields,
       const std::vector<uint8_t>& type_codes, UnionMode::type mode = UnionMode::SPARSE);

/// \brief Create a UnionType instance
std::shared_ptr<DataType> ARROW_EXPORT
union_(const std::vector<std::shared_ptr<Array>>& children,
       const std::vector<std::string>& field_names,
       const std::vector<uint8_t>& type_codes, UnionMode::type mode = UnionMode::SPARSE);

/// \brief Create a UnionType instance
inline std::shared_ptr<DataType> ARROW_EXPORT
union_(const std::vector<std::shared_ptr<Array>>& children,
       const std::vector<std::string>& field_names,
       UnionMode::type mode = UnionMode::SPARSE) {
  return union_(children, field_names, {}, mode);
}

/// \brief Create a UnionType instance
inline std::shared_ptr<DataType> ARROW_EXPORT
union_(const std::vector<std::shared_ptr<Array>>& children,
       UnionMode::type mode = UnionMode::SPARSE) {
  return union_(children, {}, {}, mode);
}

/// \brief Create a DictionaryType instance
/// \param[in] index_type the type of the dictionary indices (must be
/// a signed integer)
/// \param[in] dict_type the type of the values in the variable dictionary
/// \param[in] ordered true if the order of the dictionary values has
/// semantic meaning and should be preserved where possible
ARROW_EXPORT
std::shared_ptr<DataType> dictionary(const std::shared_ptr<DataType>& index_type,
                                     const std::shared_ptr<DataType>& dict_type,
                                     bool ordered = false);

/// @}

/// \defgroup schema-factories Factory functions for fields and schemas
///
/// Factory functions for fields and schemas
/// @{

/// \brief Create a Field instance
///
/// \param name the field name
/// \param type the field value type
/// \param nullable whether the values are nullable, default true
/// \param metadata any custom key-value metadata, default null
std::shared_ptr<Field> ARROW_EXPORT field(
    const std::string& name, const std::shared_ptr<DataType>& type, bool nullable = true,
    const std::shared_ptr<const KeyValueMetadata>& metadata = NULLPTR);

/// \brief Create a Schema instance
///
/// \param fields the schema's fields
/// \param metadata any custom key-value metadata, default null
/// \return schema shared_ptr to Schema
ARROW_EXPORT
std::shared_ptr<Schema> schema(
    const std::vector<std::shared_ptr<Field>>& fields,
    const std::shared_ptr<const KeyValueMetadata>& metadata = NULLPTR);

/// \brief Create a Schema instance
///
/// \param fields the schema's fields (rvalue reference)
/// \param metadata any custom key-value metadata, default null
/// \return schema shared_ptr to Schema
ARROW_EXPORT
std::shared_ptr<Schema> schema(
    std::vector<std::shared_ptr<Field>>&& fields,
    const std::shared_ptr<const KeyValueMetadata>& metadata = NULLPTR);

/// @}

}  // namespace arrow

#endif  // ARROW_TYPE_H

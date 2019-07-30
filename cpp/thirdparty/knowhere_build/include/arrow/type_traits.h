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

#ifndef ARROW_TYPE_TRAITS_H
#define ARROW_TYPE_TRAITS_H

#include <memory>
#include <string>
#include <type_traits>
#include <vector>

#include "arrow/type_fwd.h"
#include "arrow/util/bit-util.h"

namespace arrow {

//
// Per-type type traits
//

template <typename T>
struct TypeTraits {};

template <typename T>
struct CTypeTraits {};

template <>
struct TypeTraits<NullType> {
  using ArrayType = NullArray;
  using BuilderType = NullBuilder;
  using ScalarType = NullScalar;

  static constexpr int64_t bytes_required(int64_t) { return 0; }
  constexpr static bool is_parameter_free = true;
  static inline std::shared_ptr<DataType> type_singleton() { return null(); }
};

template <>
struct TypeTraits<BooleanType> {
  using ArrayType = BooleanArray;
  using BuilderType = BooleanBuilder;
  using ScalarType = BooleanScalar;
  using CType = bool;

  static constexpr int64_t bytes_required(int64_t elements) {
    return BitUtil::BytesForBits(elements);
  }
  constexpr static bool is_parameter_free = true;
  static inline std::shared_ptr<DataType> type_singleton() { return boolean(); }
};

template <>
struct CTypeTraits<bool> : public TypeTraits<BooleanType> {
  using ArrowType = BooleanType;
};

#define PRIMITIVE_TYPE_TRAITS_DEF_(CType_, ArrowType_, ArrowArrayType, ArrowBuilderType, \
                                   ArrowScalarType, ArrowTensorType, SingletonFn)        \
  template <>                                                                            \
  struct TypeTraits<ArrowType_> {                                                        \
    using ArrayType = ArrowArrayType;                                                    \
    using BuilderType = ArrowBuilderType;                                                \
    using ScalarType = ArrowScalarType;                                                  \
    using TensorType = ArrowTensorType;                                                  \
    using CType = ArrowType_::c_type;                                                    \
    static constexpr int64_t bytes_required(int64_t elements) {                          \
      return elements * static_cast<int64_t>(sizeof(CType));                             \
    }                                                                                    \
    constexpr static bool is_parameter_free = true;                                      \
    static inline std::shared_ptr<DataType> type_singleton() { return SingletonFn(); }   \
  };                                                                                     \
                                                                                         \
  template <>                                                                            \
  struct CTypeTraits<CType_> : public TypeTraits<ArrowType_> {                           \
    using ArrowType = ArrowType_;                                                        \
  };

#define PRIMITIVE_TYPE_TRAITS_DEF(CType, ArrowShort, SingletonFn)             \
  PRIMITIVE_TYPE_TRAITS_DEF_(                                                 \
      CType, ARROW_CONCAT(ArrowShort, Type), ARROW_CONCAT(ArrowShort, Array), \
      ARROW_CONCAT(ArrowShort, Builder), ARROW_CONCAT(ArrowShort, Scalar),    \
      ARROW_CONCAT(ArrowShort, Tensor), SingletonFn)

PRIMITIVE_TYPE_TRAITS_DEF(uint8_t, UInt8, uint8)
PRIMITIVE_TYPE_TRAITS_DEF(int8_t, Int8, int8)
PRIMITIVE_TYPE_TRAITS_DEF(uint16_t, UInt16, uint16)
PRIMITIVE_TYPE_TRAITS_DEF(int16_t, Int16, int16)
PRIMITIVE_TYPE_TRAITS_DEF(uint32_t, UInt32, uint32)
PRIMITIVE_TYPE_TRAITS_DEF(int32_t, Int32, int32)
PRIMITIVE_TYPE_TRAITS_DEF(uint64_t, UInt64, uint64)
PRIMITIVE_TYPE_TRAITS_DEF(int64_t, Int64, int64)
PRIMITIVE_TYPE_TRAITS_DEF(float, Float, float32)
PRIMITIVE_TYPE_TRAITS_DEF(double, Double, float64)

#undef PRIMITIVE_TYPE_TRAITS_DEF
#undef PRIMITIVE_TYPE_TRAITS_DEF_

template <>
struct TypeTraits<Date64Type> {
  using ArrayType = Date64Array;
  using BuilderType = Date64Builder;
  using ScalarType = Date64Scalar;
  using CType = Date64Type::c_type;

  static constexpr int64_t bytes_required(int64_t elements) {
    return elements * static_cast<int64_t>(sizeof(int64_t));
  }
  constexpr static bool is_parameter_free = true;
  static inline std::shared_ptr<DataType> type_singleton() { return date64(); }
};

template <>
struct TypeTraits<Date32Type> {
  using ArrayType = Date32Array;
  using BuilderType = Date32Builder;
  using ScalarType = Date32Scalar;
  using CType = Date32Type::c_type;

  static constexpr int64_t bytes_required(int64_t elements) {
    return elements * static_cast<int64_t>(sizeof(int32_t));
  }
  constexpr static bool is_parameter_free = true;
  static inline std::shared_ptr<DataType> type_singleton() { return date32(); }
};

template <>
struct TypeTraits<TimestampType> {
  using ArrayType = TimestampArray;
  using BuilderType = TimestampBuilder;
  using ScalarType = TimestampScalar;
  using CType = TimestampType::c_type;

  static constexpr int64_t bytes_required(int64_t elements) {
    return elements * static_cast<int64_t>(sizeof(int64_t));
  }
  constexpr static bool is_parameter_free = false;
};

template <>
struct TypeTraits<DurationType> {
  using ArrayType = DurationArray;
  using BuilderType = DurationBuilder;
  using ScalarType = DurationScalar;

  static constexpr int64_t bytes_required(int64_t elements) {
    return elements * static_cast<int64_t>(sizeof(int64_t));
  }
  constexpr static bool is_parameter_free = false;
};

template <>
struct TypeTraits<DayTimeIntervalType> {
  using ArrayType = DayTimeIntervalArray;
  using BuilderType = DayTimeIntervalBuilder;
  using ScalarType = DayTimeIntervalScalar;

  static constexpr int64_t bytes_required(int64_t elements) {
    return elements * static_cast<int64_t>(sizeof(DayTimeIntervalType::DayMilliseconds));
  }
  constexpr static bool is_parameter_free = true;
};

template <>
struct TypeTraits<MonthIntervalType> {
  using ArrayType = MonthIntervalArray;
  using BuilderType = MonthIntervalBuilder;
  using ScalarType = MonthIntervalScalar;

  static constexpr int64_t bytes_required(int64_t elements) {
    return elements * static_cast<int64_t>(sizeof(int32_t));
  }
  constexpr static bool is_parameter_free = true;
};

template <>
struct TypeTraits<Time32Type> {
  using ArrayType = Time32Array;
  using BuilderType = Time32Builder;
  using ScalarType = Time32Scalar;
  using CType = Time32Type::c_type;

  static constexpr int64_t bytes_required(int64_t elements) {
    return elements * static_cast<int64_t>(sizeof(int32_t));
  }
  constexpr static bool is_parameter_free = false;
};

template <>
struct TypeTraits<Time64Type> {
  using ArrayType = Time64Array;
  using BuilderType = Time64Builder;
  using ScalarType = Time64Scalar;
  using CType = Time64Type::c_type;

  static constexpr int64_t bytes_required(int64_t elements) {
    return elements * static_cast<int64_t>(sizeof(int64_t));
  }
  constexpr static bool is_parameter_free = false;
};

template <>
struct TypeTraits<HalfFloatType> {
  using ArrayType = HalfFloatArray;
  using BuilderType = HalfFloatBuilder;
  using ScalarType = HalfFloatScalar;
  using TensorType = HalfFloatTensor;

  static constexpr int64_t bytes_required(int64_t elements) {
    return elements * static_cast<int64_t>(sizeof(uint16_t));
  }
  constexpr static bool is_parameter_free = true;
  static inline std::shared_ptr<DataType> type_singleton() { return float16(); }
};

template <>
struct TypeTraits<Decimal128Type> {
  using ArrayType = Decimal128Array;
  using BuilderType = Decimal128Builder;
  using ScalarType = Decimal128Scalar;
  constexpr static bool is_parameter_free = false;
};

template <>
struct TypeTraits<BinaryType> {
  using ArrayType = BinaryArray;
  using BuilderType = BinaryBuilder;
  using ScalarType = BinaryScalar;
  constexpr static bool is_parameter_free = true;
  static inline std::shared_ptr<DataType> type_singleton() { return binary(); }
};

template <>
struct TypeTraits<FixedSizeBinaryType> {
  using ArrayType = FixedSizeBinaryArray;
  using BuilderType = FixedSizeBinaryBuilder;
  using ScalarType = FixedSizeBinaryScalar;
  constexpr static bool is_parameter_free = false;
};

template <>
struct TypeTraits<StringType> {
  using ArrayType = StringArray;
  using BuilderType = StringBuilder;
  using ScalarType = StringScalar;
  constexpr static bool is_parameter_free = true;
  static inline std::shared_ptr<DataType> type_singleton() { return utf8(); }
};

template <>
struct CTypeTraits<std::string> : public TypeTraits<StringType> {
  using ArrowType = StringType;
};

template <>
struct CTypeTraits<char*> : public TypeTraits<StringType> {
  using ArrowType = StringType;
};

template <>
struct TypeTraits<ListType> {
  using ArrayType = ListArray;
  using BuilderType = ListBuilder;
  using ScalarType = ListScalar;
  constexpr static bool is_parameter_free = false;
};

template <>
struct TypeTraits<MapType> {
  using ArrayType = MapArray;
  using BuilderType = MapBuilder;
  using ScalarType = MapScalar;
  constexpr static bool is_parameter_free = false;
};

template <>
struct TypeTraits<FixedSizeListType> {
  using ArrayType = FixedSizeListArray;
  using BuilderType = FixedSizeListBuilder;
  using ScalarType = FixedSizeListScalar;
  constexpr static bool is_parameter_free = false;
};

template <typename CType>
struct CTypeTraits<std::vector<CType>> : public TypeTraits<ListType> {
  using ArrowType = ListType;

  static inline std::shared_ptr<DataType> type_singleton() {
    return list(CTypeTraits<CType>::type_singleton());
  }
};

template <>
struct TypeTraits<StructType> {
  using ArrayType = StructArray;
  using BuilderType = StructBuilder;
  using ScalarType = StructScalar;
  constexpr static bool is_parameter_free = false;
};

template <>
struct TypeTraits<UnionType> {
  using ArrayType = UnionArray;
  constexpr static bool is_parameter_free = false;
};

template <>
struct TypeTraits<DictionaryType> {
  using ArrayType = DictionaryArray;
  using ScalarType = DictionaryScalar;
  constexpr static bool is_parameter_free = false;
};

template <>
struct TypeTraits<ExtensionType> {
  using ArrayType = ExtensionArray;
  constexpr static bool is_parameter_free = false;
};

//
// Useful type predicates
//

template <typename T>
using is_number_type = std::is_base_of<NumberType, T>;

template <typename T>
using is_integer_type = std::is_base_of<IntegerType, T>;

template <typename T>
using is_floating_type = std::is_base_of<FloatingPointType, T>;

template <typename T>
using is_temporal_type = std::is_base_of<TemporalType, T>;

template <typename T>
struct has_c_type {
  static constexpr bool value =
      (std::is_base_of<PrimitiveCType, T>::value || std::is_base_of<DateType, T>::value ||
       std::is_base_of<TimeType, T>::value || std::is_base_of<TimestampType, T>::value ||
       std::is_base_of<IntervalType, T>::value ||
       std::is_base_of<DurationType, T>::value);
};

template <typename T>
struct is_8bit_int {
  static constexpr bool value =
      (std::is_same<UInt8Type, T>::value || std::is_same<Int8Type, T>::value);
};

template <typename T, typename R = void>
using enable_if_8bit_int = typename std::enable_if<is_8bit_int<T>::value, R>::type;

template <typename T, typename R = void>
using enable_if_primitive_ctype =
    typename std::enable_if<std::is_base_of<PrimitiveCType, T>::value, R>::type;

template <typename T, typename R = void>
using enable_if_integer = typename std::enable_if<is_integer_type<T>::value, R>::type;

template <typename T>
using is_signed_integer =
    std::integral_constant<bool, is_integer_type<T>::value &&
                                     std::is_signed<typename T::c_type>::value>;

template <typename T, typename R = void>
using enable_if_signed_integer =
    typename std::enable_if<is_signed_integer<T>::value, R>::type;

template <typename T, typename R = void>
using enable_if_unsigned_integer = typename std::enable_if<
    is_integer_type<T>::value && std::is_unsigned<typename T::c_type>::value, R>::type;

template <typename T, typename R = void>
using enable_if_floating_point =
    typename std::enable_if<is_floating_type<T>::value, R>::type;

template <typename T>
using is_date = std::is_base_of<DateType, T>;

template <typename T, typename R = void>
using enable_if_date = typename std::enable_if<is_date<T>::value, R>::type;

template <typename T>
using is_time = std::is_base_of<TimeType, T>;

template <typename T, typename R = void>
using enable_if_time = typename std::enable_if<is_time<T>::value, R>::type;

template <typename T>
using is_timestamp = std::is_base_of<TimestampType, T>;

template <typename T, typename R = void>
using enable_if_timestamp = typename std::enable_if<is_timestamp<T>::value, R>::type;

template <typename T, typename R = void>
using enable_if_has_c_type = typename std::enable_if<has_c_type<T>::value, R>::type;

template <typename T, typename R = void>
using enable_if_null = typename std::enable_if<std::is_same<NullType, T>::value, R>::type;

template <typename T, typename R = void>
using enable_if_binary =
    typename std::enable_if<std::is_base_of<BinaryType, T>::value, R>::type;

template <typename T, typename R = void>
using enable_if_boolean =
    typename std::enable_if<std::is_same<BooleanType, T>::value, R>::type;

template <typename T, typename R = void>
using enable_if_binary_like =
    typename std::enable_if<std::is_base_of<BinaryType, T>::value ||
                                std::is_base_of<FixedSizeBinaryType, T>::value,
                            R>::type;

template <typename T, typename R = void>
using enable_if_fixed_size_binary =
    typename std::enable_if<std::is_base_of<FixedSizeBinaryType, T>::value, R>::type;

template <typename T, typename R = void>
using enable_if_list =
    typename std::enable_if<std::is_base_of<ListType, T>::value, R>::type;

template <typename T, typename R = void>
using enable_if_fixed_size_list =
    typename std::enable_if<std::is_base_of<FixedSizeListType, T>::value, R>::type;

template <typename T, typename R = void>
using enable_if_number = typename std::enable_if<is_number_type<T>::value, R>::type;

namespace detail {

// Not all type classes have a c_type
template <typename T>
struct as_void {
  using type = void;
};

// The partial specialization will match if T has the ATTR_NAME member
#define GET_ATTR(ATTR_NAME, DEFAULT)                                             \
  template <typename T, typename Enable = void>                                  \
  struct GetAttr_##ATTR_NAME {                                                   \
    using type = DEFAULT;                                                        \
  };                                                                             \
                                                                                 \
  template <typename T>                                                          \
  struct GetAttr_##ATTR_NAME<T, typename as_void<typename T::ATTR_NAME>::type> { \
    using type = typename T::ATTR_NAME;                                          \
  };

GET_ATTR(c_type, void)
GET_ATTR(TypeClass, void)

#undef GET_ATTR

}  // namespace detail

#define PRIMITIVE_TRAITS(T)                                                         \
  using TypeClass =                                                                 \
      typename std::conditional<std::is_base_of<DataType, T>::value, T,             \
                                typename detail::GetAttr_TypeClass<T>::type>::type; \
  using c_type = typename detail::GetAttr_c_type<TypeClass>::type

template <typename T>
struct IsUnsignedInt {
  PRIMITIVE_TRAITS(T);
  static constexpr bool value =
      std::is_integral<c_type>::value && std::is_unsigned<c_type>::value;
};

template <typename T>
struct IsSignedInt {
  PRIMITIVE_TRAITS(T);
  static constexpr bool value =
      std::is_integral<c_type>::value && std::is_signed<c_type>::value;
};

template <typename T>
struct IsInteger {
  PRIMITIVE_TRAITS(T);
  static constexpr bool value = std::is_integral<c_type>::value;
};

template <typename T>
struct IsFloatingPoint {
  PRIMITIVE_TRAITS(T);
  static constexpr bool value = std::is_floating_point<c_type>::value;
};

template <typename T>
struct IsNumeric {
  PRIMITIVE_TRAITS(T);
  static constexpr bool value = std::is_arithmetic<c_type>::value;
};

static inline bool is_integer(Type::type type_id) {
  switch (type_id) {
    case Type::UINT8:
    case Type::INT8:
    case Type::UINT16:
    case Type::INT16:
    case Type::UINT32:
    case Type::INT32:
    case Type::UINT64:
    case Type::INT64:
      return true;
    default:
      break;
  }
  return false;
}

static inline bool is_floating(Type::type type_id) {
  switch (type_id) {
    case Type::HALF_FLOAT:
    case Type::FLOAT:
    case Type::DOUBLE:
      return true;
    default:
      break;
  }
  return false;
}

static inline bool is_primitive(Type::type type_id) {
  switch (type_id) {
    case Type::NA:
    case Type::BOOL:
    case Type::UINT8:
    case Type::INT8:
    case Type::UINT16:
    case Type::INT16:
    case Type::UINT32:
    case Type::INT32:
    case Type::UINT64:
    case Type::INT64:
    case Type::HALF_FLOAT:
    case Type::FLOAT:
    case Type::DOUBLE:
    case Type::DATE32:
    case Type::DATE64:
    case Type::TIME32:
    case Type::TIME64:
    case Type::TIMESTAMP:
    case Type::INTERVAL:
      return true;
    default:
      break;
  }
  return false;
}

static inline bool is_binary_like(Type::type type_id) {
  switch (type_id) {
    case Type::BINARY:
    case Type::STRING:
      return true;
    default:
      break;
  }
  return false;
}

static inline bool is_dictionary(Type::type type_id) {
  return type_id == Type::DICTIONARY;
}

static inline bool is_fixed_size_binary(Type::type type_id) {
  switch (type_id) {
    case Type::DECIMAL:
    case Type::FIXED_SIZE_BINARY:
      return true;
    default:
      break;
  }
  return false;
}

static inline bool is_fixed_width(Type::type type_id) {
  return is_primitive(type_id) || is_dictionary(type_id) || is_fixed_size_binary(type_id);
}

}  // namespace arrow

#endif  // ARROW_TYPE_TRAITS_H

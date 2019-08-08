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

// This is a private header for string-to-number parsing utilitiers

#ifndef ARROW_UTIL_PARSING_H
#define ARROW_UTIL_PARSING_H

#include <cassert>
#include <chrono>
#include <limits>
#include <memory>
#include <string>
#include <type_traits>

#include <double-conversion/double-conversion.h>

#include "arrow/type.h"
#include "arrow/type_traits.h"
#include "arrow/util/checked_cast.h"
#include "arrow/util/config.h"
#include "arrow/vendored/datetime.h"

namespace arrow {
namespace internal {

/// \brief A class providing conversion from strings to some Arrow data types
///
/// Conversion is triggered by calling operator().  It returns true on
/// success, false on failure.
///
/// The class may have a non-trivial construction cost in some cases,
/// so it's recommended to use a single instance many times, if doing bulk
/// conversion. Instances of this class are not guaranteed to be thread-safe.
///
template <typename ARROW_TYPE, typename Enable = void>
class StringConverter;

template <>
class StringConverter<BooleanType> {
 public:
  explicit StringConverter(const std::shared_ptr<DataType>& = NULLPTR) {}

  using value_type = bool;

  bool operator()(const char* s, size_t length, value_type* out) {
    if (length == 1) {
      // "0" or "1"?
      if (s[0] == '0') {
        *out = false;
        return true;
      }
      if (s[0] == '1') {
        *out = true;
        return true;
      }
      return false;
    }
    if (length == 4) {
      // "true"?
      *out = true;
      return ((s[0] == 't' || s[0] == 'T') && (s[1] == 'r' || s[1] == 'R') &&
              (s[2] == 'u' || s[2] == 'U') && (s[3] == 'e' || s[3] == 'E'));
    }
    if (length == 5) {
      // "false"?
      *out = false;
      return ((s[0] == 'f' || s[0] == 'F') && (s[1] == 'a' || s[1] == 'A') &&
              (s[2] == 'l' || s[2] == 'L') && (s[3] == 's' || s[3] == 'S') &&
              (s[4] == 'e' || s[4] == 'E'));
    }
    return false;
  }
};

// Ideas for faster float parsing:
// - http://rapidjson.org/md_doc_internals.html#ParsingDouble
// - https://github.com/google/double-conversion [used here]
// - https://github.com/achan001/dtoa-fast

template <class ARROW_TYPE>
class StringToFloatConverterMixin {
 public:
  using value_type = typename ARROW_TYPE::c_type;

  explicit StringToFloatConverterMixin(const std::shared_ptr<DataType>& = NULLPTR)
      : main_converter_(flags_, main_junk_value_, main_junk_value_, "inf", "nan"),
        fallback_converter_(flags_, fallback_junk_value_, fallback_junk_value_, "inf",
                            "nan") {}

  bool operator()(const char* s, size_t length, value_type* out) {
    value_type v;
    // double-conversion doesn't give us an error flag but signals parse
    // errors with sentinel values.  Since a sentinel value can appear as
    // legitimate input, we fallback on a second converter with a different
    // sentinel to eliminate false errors.
    TryConvert(main_converter_, s, length, &v);
    if (ARROW_PREDICT_FALSE(v == static_cast<value_type>(main_junk_value_))) {
      TryConvert(fallback_converter_, s, length, &v);
      if (ARROW_PREDICT_FALSE(v == static_cast<value_type>(fallback_junk_value_))) {
        return false;
      }
    }
    *out = v;
    return true;
  }

 protected:
// This is only support in double-conversion 3.1+
#ifdef DOUBLE_CONVERSION_HAS_CASE_INSENSIBILITY
  static const int flags_ =
      double_conversion::StringToDoubleConverter::ALLOW_CASE_INSENSIBILITY;
#else
  static const int flags_ = double_conversion::StringToDoubleConverter::NO_FLAGS;
#endif
  // Two unlikely values to signal a parsing error
  static constexpr double main_junk_value_ = 0.7066424364107089;
  static constexpr double fallback_junk_value_ = 0.40088499148279166;

  double_conversion::StringToDoubleConverter main_converter_;
  double_conversion::StringToDoubleConverter fallback_converter_;

  inline void TryConvert(double_conversion::StringToDoubleConverter& converter,
                         const char* s, size_t length, float* out) {
    int processed_length;
    *out = converter.StringToFloat(s, static_cast<int>(length), &processed_length);
  }

  inline void TryConvert(double_conversion::StringToDoubleConverter& converter,
                         const char* s, size_t length, double* out) {
    int processed_length;
    *out = converter.StringToDouble(s, static_cast<int>(length), &processed_length);
  }
};

template <>
class StringConverter<FloatType> : public StringToFloatConverterMixin<FloatType> {
  using StringToFloatConverterMixin<FloatType>::StringToFloatConverterMixin;
};

template <>
class StringConverter<DoubleType> : public StringToFloatConverterMixin<DoubleType> {
  using StringToFloatConverterMixin<DoubleType>::StringToFloatConverterMixin;
};

// NOTE: HalfFloatType would require a half<->float conversion library

namespace detail {

inline uint8_t ParseDecimalDigit(char c) { return static_cast<uint8_t>(c - '0'); }

#define PARSE_UNSIGNED_ITERATION(C_TYPE)          \
  if (length > 0) {                               \
    uint8_t digit = ParseDecimalDigit(*s++);      \
    result = static_cast<C_TYPE>(result * 10U);   \
    length--;                                     \
    if (ARROW_PREDICT_FALSE(digit > 9U)) {        \
      /* Non-digit */                             \
      return false;                               \
    }                                             \
    result = static_cast<C_TYPE>(result + digit); \
  }

#define PARSE_UNSIGNED_ITERATION_LAST(C_TYPE)                                     \
  if (length > 0) {                                                               \
    if (ARROW_PREDICT_FALSE(result > std::numeric_limits<C_TYPE>::max() / 10U)) { \
      /* Overflow */                                                              \
      return false;                                                               \
    }                                                                             \
    uint8_t digit = ParseDecimalDigit(*s++);                                      \
    result = static_cast<C_TYPE>(result * 10U);                                   \
    C_TYPE new_result = static_cast<C_TYPE>(result + digit);                      \
    if (ARROW_PREDICT_FALSE(--length > 0)) {                                      \
      /* Too many digits */                                                       \
      return false;                                                               \
    }                                                                             \
    if (ARROW_PREDICT_FALSE(digit > 9U)) {                                        \
      /* Non-digit */                                                             \
      return false;                                                               \
    }                                                                             \
    if (ARROW_PREDICT_FALSE(new_result < result)) {                               \
      /* Overflow */                                                              \
      return false;                                                               \
    }                                                                             \
    result = new_result;                                                          \
  }

inline bool ParseUnsigned(const char* s, size_t length, uint8_t* out) {
  uint8_t result = 0;

  PARSE_UNSIGNED_ITERATION(uint8_t);
  PARSE_UNSIGNED_ITERATION(uint8_t);
  PARSE_UNSIGNED_ITERATION_LAST(uint8_t);
  *out = result;
  return true;
}

inline bool ParseUnsigned(const char* s, size_t length, uint16_t* out) {
  uint16_t result = 0;

  PARSE_UNSIGNED_ITERATION(uint16_t);
  PARSE_UNSIGNED_ITERATION(uint16_t);
  PARSE_UNSIGNED_ITERATION(uint16_t);
  PARSE_UNSIGNED_ITERATION(uint16_t);
  PARSE_UNSIGNED_ITERATION_LAST(uint16_t);
  *out = result;
  return true;
}

inline bool ParseUnsigned(const char* s, size_t length, uint32_t* out) {
  uint32_t result = 0;

  PARSE_UNSIGNED_ITERATION(uint32_t);
  PARSE_UNSIGNED_ITERATION(uint32_t);
  PARSE_UNSIGNED_ITERATION(uint32_t);
  PARSE_UNSIGNED_ITERATION(uint32_t);
  PARSE_UNSIGNED_ITERATION(uint32_t);

  PARSE_UNSIGNED_ITERATION(uint32_t);
  PARSE_UNSIGNED_ITERATION(uint32_t);
  PARSE_UNSIGNED_ITERATION(uint32_t);
  PARSE_UNSIGNED_ITERATION(uint32_t);

  PARSE_UNSIGNED_ITERATION_LAST(uint32_t);
  *out = result;
  return true;
}

inline bool ParseUnsigned(const char* s, size_t length, uint64_t* out) {
  uint64_t result = 0;

  PARSE_UNSIGNED_ITERATION(uint64_t);
  PARSE_UNSIGNED_ITERATION(uint64_t);
  PARSE_UNSIGNED_ITERATION(uint64_t);
  PARSE_UNSIGNED_ITERATION(uint64_t);
  PARSE_UNSIGNED_ITERATION(uint64_t);

  PARSE_UNSIGNED_ITERATION(uint64_t);
  PARSE_UNSIGNED_ITERATION(uint64_t);
  PARSE_UNSIGNED_ITERATION(uint64_t);
  PARSE_UNSIGNED_ITERATION(uint64_t);
  PARSE_UNSIGNED_ITERATION(uint64_t);

  PARSE_UNSIGNED_ITERATION(uint64_t);
  PARSE_UNSIGNED_ITERATION(uint64_t);
  PARSE_UNSIGNED_ITERATION(uint64_t);
  PARSE_UNSIGNED_ITERATION(uint64_t);
  PARSE_UNSIGNED_ITERATION(uint64_t);

  PARSE_UNSIGNED_ITERATION(uint64_t);
  PARSE_UNSIGNED_ITERATION(uint64_t);
  PARSE_UNSIGNED_ITERATION(uint64_t);
  PARSE_UNSIGNED_ITERATION(uint64_t);

  PARSE_UNSIGNED_ITERATION_LAST(uint64_t);
  *out = result;
  return true;
}

#undef PARSE_UNSIGNED_ITERATION
#undef PARSE_UNSIGNED_ITERATION_LAST

}  // namespace detail

template <class ARROW_TYPE>
class StringToUnsignedIntConverterMixin {
 public:
  using value_type = typename ARROW_TYPE::c_type;

  explicit StringToUnsignedIntConverterMixin(const std::shared_ptr<DataType>& = NULLPTR) {
  }

  bool operator()(const char* s, size_t length, value_type* out) {
    if (ARROW_PREDICT_FALSE(length == 0)) {
      return false;
    }
    // Skip leading zeros
    while (length > 0 && *s == '0') {
      length--;
      s++;
    }
    return detail::ParseUnsigned(s, length, out);
  }
};

template <>
class StringConverter<UInt8Type> : public StringToUnsignedIntConverterMixin<UInt8Type> {
  using StringToUnsignedIntConverterMixin<UInt8Type>::StringToUnsignedIntConverterMixin;
};

template <>
class StringConverter<UInt16Type> : public StringToUnsignedIntConverterMixin<UInt16Type> {
  using StringToUnsignedIntConverterMixin<UInt16Type>::StringToUnsignedIntConverterMixin;
};

template <>
class StringConverter<UInt32Type> : public StringToUnsignedIntConverterMixin<UInt32Type> {
  using StringToUnsignedIntConverterMixin<UInt32Type>::StringToUnsignedIntConverterMixin;
};

template <>
class StringConverter<UInt64Type> : public StringToUnsignedIntConverterMixin<UInt64Type> {
  using StringToUnsignedIntConverterMixin<UInt64Type>::StringToUnsignedIntConverterMixin;
};

template <class ARROW_TYPE>
class StringToSignedIntConverterMixin {
 public:
  using value_type = typename ARROW_TYPE::c_type;
  using unsigned_type = typename std::make_unsigned<value_type>::type;

  explicit StringToSignedIntConverterMixin(const std::shared_ptr<DataType>& = NULLPTR) {}

  bool operator()(const char* s, size_t length, value_type* out) {
    static constexpr unsigned_type max_positive =
        static_cast<unsigned_type>(std::numeric_limits<value_type>::max());
    // Assuming two's complement
    static constexpr unsigned_type max_negative = max_positive + 1;
    bool negative = false;
    unsigned_type unsigned_value = 0;

    if (ARROW_PREDICT_FALSE(length == 0)) {
      return false;
    }
    if (*s == '-') {
      negative = true;
      s++;
      if (--length == 0) {
        return false;
      }
    }
    // Skip leading zeros
    while (length > 0 && *s == '0') {
      length--;
      s++;
    }
    if (!ARROW_PREDICT_TRUE(detail::ParseUnsigned(s, length, &unsigned_value))) {
      return false;
    }
    if (negative) {
      if (ARROW_PREDICT_FALSE(unsigned_value > max_negative)) {
        return false;
      }
      // To avoid both compiler warnings (with unsigned negation)
      // and undefined behaviour (with signed negation overflow),
      // use the expanded formula for 2's complement negation.
      *out = static_cast<value_type>(~unsigned_value + 1);
    } else {
      if (ARROW_PREDICT_FALSE(unsigned_value > max_positive)) {
        return false;
      }
      *out = static_cast<value_type>(unsigned_value);
    }
    return true;
  }
};

template <>
class StringConverter<Int8Type> : public StringToSignedIntConverterMixin<Int8Type> {
  using StringToSignedIntConverterMixin<Int8Type>::StringToSignedIntConverterMixin;
};

template <>
class StringConverter<Int16Type> : public StringToSignedIntConverterMixin<Int16Type> {
  using StringToSignedIntConverterMixin<Int16Type>::StringToSignedIntConverterMixin;
};

template <>
class StringConverter<Int32Type> : public StringToSignedIntConverterMixin<Int32Type> {
  using StringToSignedIntConverterMixin<Int32Type>::StringToSignedIntConverterMixin;
};

template <>
class StringConverter<Int64Type> : public StringToSignedIntConverterMixin<Int64Type> {
  using StringToSignedIntConverterMixin<Int64Type>::StringToSignedIntConverterMixin;
};

template <>
class StringConverter<TimestampType> {
 public:
  using value_type = TimestampType::c_type;

  explicit StringConverter(const std::shared_ptr<DataType>& type)
      : unit_(checked_cast<TimestampType*>(type.get())->unit()) {}

  bool operator()(const char* s, size_t length, value_type* out) {
    // We allow the following formats:
    // - "YYYY-MM-DD"
    // - "YYYY-MM-DD[ T]hh:mm:ss"
    // - "YYYY-MM-DD[ T]hh:mm:ssZ"
    // UTC is always assumed, and the DataType's timezone is ignored.
    arrow_vendored::date::year_month_day ymd;
    if (ARROW_PREDICT_FALSE(length < 10)) {
      return false;
    }
    if (length == 10) {
      if (ARROW_PREDICT_FALSE(!ParseYYYY_MM_DD(s, &ymd))) {
        return false;
      }
      return ConvertTimePoint(arrow_vendored::date::sys_days(ymd), out);
    }
    if (ARROW_PREDICT_FALSE(s[10] != ' ') && ARROW_PREDICT_FALSE(s[10] != 'T')) {
      return false;
    }
    if (s[length - 1] == 'Z') {
      --length;
    }
    if (length == 19) {
      if (ARROW_PREDICT_FALSE(!ParseYYYY_MM_DD(s, &ymd))) {
        return false;
      }
      std::chrono::duration<value_type> seconds;
      if (ARROW_PREDICT_FALSE(!ParseHH_MM_SS(s + 11, &seconds))) {
        return false;
      }
      return ConvertTimePoint(arrow_vendored::date::sys_days(ymd) + seconds, out);
    }
    return false;
  }

 protected:
  template <class TimePoint>
  bool ConvertTimePoint(TimePoint tp, value_type* out) {
    auto duration = tp.time_since_epoch();
    switch (unit_) {
      case TimeUnit::SECOND:
        *out = std::chrono::duration_cast<std::chrono::seconds>(duration).count();
        return true;
      case TimeUnit::MILLI:
        *out = std::chrono::duration_cast<std::chrono::milliseconds>(duration).count();
        return true;
      case TimeUnit::MICRO:
        *out = std::chrono::duration_cast<std::chrono::microseconds>(duration).count();
        return true;
      case TimeUnit::NANO:
        *out = std::chrono::duration_cast<std::chrono::nanoseconds>(duration).count();
        return true;
    }
    // Unreachable, but suppress compiler warning
    assert(0);
    *out = 0;
    return true;
  }

  bool ParseYYYY_MM_DD(const char* s, arrow_vendored::date::year_month_day* out) {
    uint16_t year;
    uint8_t month, day;
    if (ARROW_PREDICT_FALSE(s[4] != '-') || ARROW_PREDICT_FALSE(s[7] != '-')) {
      return false;
    }
    if (ARROW_PREDICT_FALSE(!detail::ParseUnsigned(s + 0, 4, &year))) {
      return false;
    }
    if (ARROW_PREDICT_FALSE(!detail::ParseUnsigned(s + 5, 2, &month))) {
      return false;
    }
    if (ARROW_PREDICT_FALSE(!detail::ParseUnsigned(s + 8, 2, &day))) {
      return false;
    }
    *out = {arrow_vendored::date::year{year}, arrow_vendored::date::month{month},
            arrow_vendored::date::day{day}};
    return out->ok();
  }

  bool ParseHH_MM_SS(const char* s, std::chrono::duration<value_type>* out) {
    uint8_t hours, minutes, seconds;
    if (ARROW_PREDICT_FALSE(s[2] != ':') || ARROW_PREDICT_FALSE(s[5] != ':')) {
      return false;
    }
    if (ARROW_PREDICT_FALSE(!detail::ParseUnsigned(s + 0, 2, &hours))) {
      return false;
    }
    if (ARROW_PREDICT_FALSE(!detail::ParseUnsigned(s + 3, 2, &minutes))) {
      return false;
    }
    if (ARROW_PREDICT_FALSE(!detail::ParseUnsigned(s + 6, 2, &seconds))) {
      return false;
    }
    if (ARROW_PREDICT_FALSE(hours >= 24)) {
      return false;
    }
    if (ARROW_PREDICT_FALSE(minutes >= 60)) {
      return false;
    }
    if (ARROW_PREDICT_FALSE(seconds >= 60)) {
      return false;
    }
    *out = std::chrono::duration<value_type>(3600U * hours + 60U * minutes + seconds);
    return true;
  }

  const TimeUnit::type unit_;
};

}  // namespace internal
}  // namespace arrow

#endif  // ARROW_UTIL_PARSING_H

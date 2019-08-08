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

#include <algorithm>
#include <cassert>
#include <cstdint>
#include <limits>
#include <memory>
#include <random>
#include <vector>

#include "arrow/type.h"
#include "arrow/util/visibility.h"

namespace arrow {

class Array;

namespace random {

using SeedType = std::random_device::result_type;
constexpr SeedType kSeedMax = std::numeric_limits<SeedType>::max();

class ARROW_EXPORT RandomArrayGenerator {
 public:
  explicit RandomArrayGenerator(SeedType seed)
      : seed_distribution_(static_cast<SeedType>(1), kSeedMax), seed_rng_(seed) {}

  /// \brief Generates a random BooleanArray
  ///
  /// \param[in] size the size of the array to generate
  /// \param[in] probability the estimated number of active bits
  /// \param[in] null_probability the probability of a row being null
  ///
  /// \return a generated Array
  std::shared_ptr<arrow::Array> Boolean(int64_t size, double probability,
                                        double null_probability);

  /// \brief Generates a random UInt8Array
  ///
  /// \param[in] size the size of the array to generate
  /// \param[in] min the lower bound of the uniform distribution
  /// \param[in] max the upper bound of the uniform distribution
  /// \param[in] null_probability the probability of a row being null
  ///
  /// \return a generated Array
  std::shared_ptr<arrow::Array> UInt8(int64_t size, uint8_t min, uint8_t max,
                                      double null_probability);

  /// \brief Generates a random Int8Array
  ///
  /// \param[in] size the size of the array to generate
  /// \param[in] min the lower bound of the uniform distribution
  /// \param[in] max the upper bound of the uniform distribution
  /// \param[in] null_probability the probability of a row being null
  ///
  /// \return a generated Array
  std::shared_ptr<arrow::Array> Int8(int64_t size, int8_t min, int8_t max,
                                     double null_probability);

  /// \brief Generates a random UInt16Array
  ///
  /// \param[in] size the size of the array to generate
  /// \param[in] min the lower bound of the uniform distribution
  /// \param[in] max the upper bound of the uniform distribution
  /// \param[in] null_probability the probability of a row being null
  ///
  /// \return a generated Array
  std::shared_ptr<arrow::Array> UInt16(int64_t size, uint16_t min, uint16_t max,
                                       double null_probability);

  /// \brief Generates a random Int16Array
  ///
  /// \param[in] size the size of the array to generate
  /// \param[in] min the lower bound of the uniform distribution
  /// \param[in] max the upper bound of the uniform distribution
  /// \param[in] null_probability the probability of a row being null
  ///
  /// \return a generated Array
  std::shared_ptr<arrow::Array> Int16(int64_t size, int16_t min, int16_t max,
                                      double null_probability);

  /// \brief Generates a random UInt32Array
  ///
  /// \param[in] size the size of the array to generate
  /// \param[in] min the lower bound of the uniform distribution
  /// \param[in] max the upper bound of the uniform distribution
  /// \param[in] null_probability the probability of a row being null
  ///
  /// \return a generated Array
  std::shared_ptr<arrow::Array> UInt32(int64_t size, uint32_t min, uint32_t max,
                                       double null_probability);

  /// \brief Generates a random Int32Array
  ///
  /// \param[in] size the size of the array to generate
  /// \param[in] min the lower bound of the uniform distribution
  /// \param[in] max the upper bound of the uniform distribution
  /// \param[in] null_probability the probability of a row being null
  ///
  /// \return a generated Array
  std::shared_ptr<arrow::Array> Int32(int64_t size, int32_t min, int32_t max,
                                      double null_probability);

  /// \brief Generates a random UInt64Array
  ///
  /// \param[in] size the size of the array to generate
  /// \param[in] min the lower bound of the uniform distribution
  /// \param[in] max the upper bound of the uniform distribution
  /// \param[in] null_probability the probability of a row being null
  ///
  /// \return a generated Array
  std::shared_ptr<arrow::Array> UInt64(int64_t size, uint64_t min, uint64_t max,
                                       double null_probability);

  /// \brief Generates a random Int64Array
  ///
  /// \param[in] size the size of the array to generate
  /// \param[in] min the lower bound of the uniform distribution
  /// \param[in] max the upper bound of the uniform distribution
  /// \param[in] null_probability the probability of a row being null
  ///
  /// \return a generated Array
  std::shared_ptr<arrow::Array> Int64(int64_t size, int64_t min, int64_t max,
                                      double null_probability);

  /// \brief Generates a random FloatArray
  ///
  /// \param[in] size the size of the array to generate
  /// \param[in] min the lower bound of the uniform distribution
  /// \param[in] max the upper bound of the uniform distribution
  /// \param[in] null_probability the probability of a row being null
  ///
  /// \return a generated Array
  std::shared_ptr<arrow::Array> Float32(int64_t size, float min, float max,
                                        double null_probability);

  /// \brief Generates a random DoubleArray
  ///
  /// \param[in] size the size of the array to generate
  /// \param[in] min the lower bound of the uniform distribution
  /// \param[in] max the upper bound of the uniform distribution
  /// \param[in] null_probability the probability of a row being null
  ///
  /// \return a generated Array
  std::shared_ptr<arrow::Array> Float64(int64_t size, double min, double max,
                                        double null_probability);

  template <typename ArrowType, typename CType = typename ArrowType::c_type>
  std::shared_ptr<arrow::Array> Numeric(int64_t size, CType min, CType max,
                                        double null_probability) {
    switch (ArrowType::type_id) {
      case Type::UINT8:
        return UInt8(size, static_cast<uint8_t>(min), static_cast<uint8_t>(max),
                     null_probability);
      case Type::INT8:
        return Int8(size, static_cast<int8_t>(min), static_cast<int8_t>(max),
                    null_probability);
      case Type::UINT16:
        return UInt16(size, static_cast<uint16_t>(min), static_cast<uint16_t>(max),
                      null_probability);
      case Type::INT16:
        return Int16(size, static_cast<int16_t>(min), static_cast<int16_t>(max),
                     null_probability);
      case Type::UINT32:
        return UInt32(size, static_cast<uint32_t>(min), static_cast<uint32_t>(max),
                      null_probability);
      case Type::INT32:
        return Int32(size, static_cast<int32_t>(min), static_cast<int32_t>(max),
                     null_probability);
      case Type::UINT64:
        return UInt64(size, static_cast<uint64_t>(min), static_cast<uint64_t>(max),
                      null_probability);
      case Type::INT64:
        return Int64(size, static_cast<int64_t>(min), static_cast<int64_t>(max),
                     null_probability);
      case Type::FLOAT:
        return Float32(size, static_cast<float>(min), static_cast<float>(max),
                       null_probability);
      case Type::DOUBLE:
        return Float64(size, static_cast<double>(min), static_cast<double>(max),
                       null_probability);
      default:
        return nullptr;
    }
  }

  /// \brief Generates a random StringArray
  ///
  /// \param[in] size the size of the array to generate
  /// \param[in] min_length the lower bound of the string length
  ///            determined by the uniform distribution
  /// \param[in] max_length the upper bound of the string length
  ///            determined by the uniform distribution
  /// \param[in] null_probability the probability of a row being null
  ///
  /// \return a generated Array
  std::shared_ptr<arrow::Array> String(int64_t size, int32_t min_length,
                                       int32_t max_length, double null_probability);

  /// \brief Generates a random StringArray with repeated values
  ///
  /// \param[in] size the size of the array to generate
  /// \param[in] unique the number of unique string values used
  ///            to populate the array
  /// \param[in] min_length the lower bound of the string length
  ///            determined by the uniform distribution
  /// \param[in] max_length the upper bound of the string length
  ///            determined by the uniform distribution
  /// \param[in] null_probability the probability of a row being null
  ///
  /// \return a generated Array
  std::shared_ptr<arrow::Array> StringWithRepeats(int64_t size, int64_t unique,
                                                  int32_t min_length, int32_t max_length,
                                                  double null_probability);

 private:
  SeedType seed() { return seed_distribution_(seed_rng_); }

  std::uniform_int_distribution<SeedType> seed_distribution_;
  std::default_random_engine seed_rng_;
};

}  // namespace random

//
// Assorted functions
//

template <typename T, typename U>
void randint(int64_t N, T lower, T upper, std::vector<U>* out) {
  const int random_seed = 0;
  std::default_random_engine gen(random_seed);
  std::uniform_int_distribution<T> d(lower, upper);
  out->resize(N, static_cast<T>(0));
  std::generate(out->begin(), out->end(), [&d, &gen] { return static_cast<U>(d(gen)); });
}

template <typename T, typename U>
void random_real(int64_t n, uint32_t seed, T min_value, T max_value,
                 std::vector<U>* out) {
  std::default_random_engine gen(seed);
  std::uniform_real_distribution<T> d(min_value, max_value);
  out->resize(n, static_cast<T>(0));
  std::generate(out->begin(), out->end(), [&d, &gen] { return static_cast<U>(d(gen)); });
}

template <typename T, typename U>
void rand_uniform_int(int64_t n, uint32_t seed, T min_value, T max_value, U* out) {
  assert(out || (n == 0));
  std::default_random_engine gen(seed);
  std::uniform_int_distribution<T> d(min_value, max_value);
  std::generate(out, out + n, [&d, &gen] { return static_cast<U>(d(gen)); });
}

}  // namespace arrow

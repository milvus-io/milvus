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

#ifndef ARROW_COMPUTE_KERNEL_H
#define ARROW_COMPUTE_KERNEL_H

#include <memory>
#include <utility>
#include <vector>

#include "arrow/array.h"
#include "arrow/record_batch.h"
#include "arrow/scalar.h"
#include "arrow/table.h"
#include "arrow/util/macros.h"
#include "arrow/util/memory.h"
#include "arrow/util/variant.h"  // IWYU pragma: export
#include "arrow/util/visibility.h"

namespace arrow {
namespace compute {

class FunctionContext;

/// \class OpKernel
/// \brief Base class for operator kernels
///
/// Note to implementors:
/// Operator kernels are intended to be the lowest level of an analytics/compute
/// engine.  They will generally not be exposed directly to end-users.  Instead
/// they will be wrapped by higher level constructs (e.g. top-level functions
/// or physical execution plan nodes).  These higher level constructs are
/// responsible for user input validation and returning the appropriate
/// error Status.
///
/// Due to this design, implementations of Call (the execution
/// method on subclasses) should use assertions (i.e. DCHECK) to double-check
/// parameter arguments when in higher level components returning an
/// InvalidArgument error might be more appropriate.
///
class ARROW_EXPORT OpKernel {
 public:
  virtual ~OpKernel() = default;
  /// \brief EXPERIMENTAL The output data type of the kernel
  /// \return the output type
  virtual std::shared_ptr<DataType> out_type() const = 0;
};

struct Datum;
static inline bool CollectionEquals(const std::vector<Datum>& left,
                                    const std::vector<Datum>& right);

// Datums variants may have a length. This special value indicate that the
// current variant does not have a length.
constexpr int64_t kUnknownLength = -1;

/// \class Datum
/// \brief Variant type for various Arrow C++ data structures
struct ARROW_EXPORT Datum {
  enum type { NONE, SCALAR, ARRAY, CHUNKED_ARRAY, RECORD_BATCH, TABLE, COLLECTION };

  util::variant<decltype(NULLPTR), std::shared_ptr<Scalar>, std::shared_ptr<ArrayData>,
                std::shared_ptr<ChunkedArray>, std::shared_ptr<RecordBatch>,
                std::shared_ptr<Table>, std::vector<Datum>>
      value;

  /// \brief Empty datum, to be populated elsewhere
  Datum() : value(NULLPTR) {}

  Datum(const std::shared_ptr<Scalar>& value)  // NOLINT implicit conversion
      : value(value) {}
  Datum(const std::shared_ptr<ArrayData>& value)  // NOLINT implicit conversion
      : value(value) {}

  Datum(const std::shared_ptr<Array>& value)  // NOLINT implicit conversion
      : Datum(value ? value->data() : NULLPTR) {}

  Datum(const std::shared_ptr<ChunkedArray>& value)  // NOLINT implicit conversion
      : value(value) {}
  Datum(const std::shared_ptr<RecordBatch>& value)  // NOLINT implicit conversion
      : value(value) {}
  Datum(const std::shared_ptr<Table>& value)  // NOLINT implicit conversion
      : value(value) {}
  Datum(const std::vector<Datum>& value)  // NOLINT implicit conversion
      : value(value) {}

  // Cast from subtypes of Array to Datum
  template <typename T,
            typename = typename std::enable_if<std::is_base_of<Array, T>::value>::type>
  Datum(const std::shared_ptr<T>& value)  // NOLINT implicit conversion
      : Datum(std::shared_ptr<Array>(value)) {}

  // Convenience constructors
  explicit Datum(bool value) : value(std::make_shared<BooleanScalar>(value)) {}
  explicit Datum(int8_t value) : value(std::make_shared<Int8Scalar>(value)) {}
  explicit Datum(uint8_t value) : value(std::make_shared<UInt8Scalar>(value)) {}
  explicit Datum(int16_t value) : value(std::make_shared<Int16Scalar>(value)) {}
  explicit Datum(uint16_t value) : value(std::make_shared<UInt16Scalar>(value)) {}
  explicit Datum(int32_t value) : value(std::make_shared<Int32Scalar>(value)) {}
  explicit Datum(uint32_t value) : value(std::make_shared<UInt32Scalar>(value)) {}
  explicit Datum(int64_t value) : value(std::make_shared<Int64Scalar>(value)) {}
  explicit Datum(uint64_t value) : value(std::make_shared<UInt64Scalar>(value)) {}
  explicit Datum(float value) : value(std::make_shared<FloatScalar>(value)) {}
  explicit Datum(double value) : value(std::make_shared<DoubleScalar>(value)) {}

  ~Datum() {}

  Datum(const Datum& other) noexcept { this->value = other.value; }

  Datum& operator=(const Datum& other) noexcept {
    value = other.value;
    return *this;
  }

  // Define move constructor and move assignment, for better performance
  Datum(Datum&& other) noexcept : value(std::move(other.value)) {}

  Datum& operator=(Datum&& other) noexcept {
    value = std::move(other.value);
    return *this;
  }

  Datum::type kind() const {
    switch (this->value.index()) {
      case 0:
        return Datum::NONE;
      case 1:
        return Datum::SCALAR;
      case 2:
        return Datum::ARRAY;
      case 3:
        return Datum::CHUNKED_ARRAY;
      case 4:
        return Datum::RECORD_BATCH;
      case 5:
        return Datum::TABLE;
      case 6:
        return Datum::COLLECTION;
      default:
        return Datum::NONE;
    }
  }

  std::shared_ptr<ArrayData> array() const {
    return util::get<std::shared_ptr<ArrayData>>(this->value);
  }

  std::shared_ptr<Array> make_array() const {
    return MakeArray(util::get<std::shared_ptr<ArrayData>>(this->value));
  }

  std::shared_ptr<ChunkedArray> chunked_array() const {
    return util::get<std::shared_ptr<ChunkedArray>>(this->value);
  }

  std::shared_ptr<RecordBatch> record_batch() const {
    return util::get<std::shared_ptr<RecordBatch>>(this->value);
  }

  std::shared_ptr<Table> table() const {
    return util::get<std::shared_ptr<Table>>(this->value);
  }

  const std::vector<Datum> collection() const {
    return util::get<std::vector<Datum>>(this->value);
  }

  std::shared_ptr<Scalar> scalar() const {
    return util::get<std::shared_ptr<Scalar>>(this->value);
  }

  bool is_array() const { return this->kind() == Datum::ARRAY; }

  bool is_arraylike() const {
    return this->kind() == Datum::ARRAY || this->kind() == Datum::CHUNKED_ARRAY;
  }

  bool is_scalar() const { return this->kind() == Datum::SCALAR; }

  /// \brief The value type of the variant, if any
  ///
  /// \return nullptr if no type
  std::shared_ptr<DataType> type() const {
    if (this->kind() == Datum::ARRAY) {
      return util::get<std::shared_ptr<ArrayData>>(this->value)->type;
    } else if (this->kind() == Datum::CHUNKED_ARRAY) {
      return util::get<std::shared_ptr<ChunkedArray>>(this->value)->type();
    } else if (this->kind() == Datum::SCALAR) {
      return util::get<std::shared_ptr<Scalar>>(this->value)->type;
    }
    return NULLPTR;
  }

  /// \brief The value length of the variant, if any
  ///
  /// \return kUnknownLength if no type
  int64_t length() const {
    if (this->kind() == Datum::ARRAY) {
      return util::get<std::shared_ptr<ArrayData>>(this->value)->length;
    } else if (this->kind() == Datum::CHUNKED_ARRAY) {
      return util::get<std::shared_ptr<ChunkedArray>>(this->value)->length();
    } else if (this->kind() == Datum::SCALAR) {
      return 1;
    }
    return kUnknownLength;
  }

  bool Equals(const Datum& other) const {
    if (this->kind() != other.kind()) return false;

    switch (this->kind()) {
      case Datum::NONE:
        return true;
      case Datum::SCALAR:
        return internal::SharedPtrEquals(this->scalar(), other.scalar());
      case Datum::ARRAY:
        return internal::SharedPtrEquals(this->make_array(), other.make_array());
      case Datum::CHUNKED_ARRAY:
        return internal::SharedPtrEquals(this->chunked_array(), other.chunked_array());
      case Datum::RECORD_BATCH:
        return internal::SharedPtrEquals(this->record_batch(), other.record_batch());
      case Datum::TABLE:
        return internal::SharedPtrEquals(this->table(), other.table());
      case Datum::COLLECTION:
        return CollectionEquals(this->collection(), other.collection());
      default:
        return false;
    }
  }
};

/// \class UnaryKernel
/// \brief An array-valued function of a single input argument.
///
/// Note to implementors:  Try to avoid making kernels that allocate memory if
/// the output size is a deterministic function of the Input Datum's metadata.
/// Instead separate the logic of the kernel and allocations necessary into
/// two different kernels.  Some reusable kernels that allocate buffers
/// and delegate computation to another kernel are available in util-internal.h.
class ARROW_EXPORT UnaryKernel : public OpKernel {
 public:
  /// \brief Executes the kernel.
  ///
  /// \param[in] ctx The function context for the kernel
  /// \param[in] input The kernel input data
  /// \param[out] out The output of the function. Each implementation of this
  /// function might assume different things about the existing contents of out
  /// (e.g. which buffers are preallocated).  In the future it is expected that
  /// there will be a more generic mechansim for understanding the necessary
  /// contracts.
  virtual Status Call(FunctionContext* ctx, const Datum& input, Datum* out) = 0;
};

/// \class BinaryKernel
/// \brief An array-valued function of a two input arguments
class ARROW_EXPORT BinaryKernel : public OpKernel {
 public:
  virtual Status Call(FunctionContext* ctx, const Datum& left, const Datum& right,
                      Datum* out) = 0;
};

static inline bool CollectionEquals(const std::vector<Datum>& left,
                                    const std::vector<Datum>& right) {
  if (left.size() != right.size()) return false;

  for (size_t i = 0; i < left.size(); i++)
    if (!left[i].Equals(right[i])) return false;

  return true;
}

}  // namespace compute
}  // namespace arrow

#endif  // ARROW_COMPUTE_KERNEL_H

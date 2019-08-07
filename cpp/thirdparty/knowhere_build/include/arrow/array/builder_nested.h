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
#include <vector>

#include "arrow/array/builder_base.h"
#include "arrow/buffer-builder.h"

namespace arrow {

// ----------------------------------------------------------------------
// List builder

/// \class ListBuilder
/// \brief Builder class for variable-length list array value types
///
/// To use this class, you must append values to the child array builder and use
/// the Append function to delimit each distinct list value (once the values
/// have been appended to the child array) or use the bulk API to append
/// a sequence of offests and null values.
///
/// A note on types.  Per arrow/type.h all types in the c++ implementation are
/// logical so even though this class always builds list array, this can
/// represent multiple different logical types.  If no logical type is provided
/// at construction time, the class defaults to List<T> where t is taken from the
/// value_builder/values that the object is constructed with.
class ARROW_EXPORT ListBuilder : public ArrayBuilder {
 public:
  /// Use this constructor to incrementally build the value array along with offsets and
  /// null bitmap.
  ListBuilder(MemoryPool* pool, const std::shared_ptr<ArrayBuilder>& value_builder,
              const std::shared_ptr<DataType>& type = NULLPTR);

  Status Resize(int64_t capacity) override;
  void Reset() override;
  Status FinishInternal(std::shared_ptr<ArrayData>* out) override;

  /// \cond FALSE
  using ArrayBuilder::Finish;
  /// \endcond

  Status Finish(std::shared_ptr<ListArray>* out) { return FinishTyped(out); }

  /// \brief Vector append
  ///
  /// If passed, valid_bytes is of equal length to values, and any zero byte
  /// will be considered as a null for that slot
  Status AppendValues(const int32_t* offsets, int64_t length,
                      const uint8_t* valid_bytes = NULLPTR);

  /// \brief Start a new variable-length list slot
  ///
  /// This function should be called before beginning to append elements to the
  /// value builder
  Status Append(bool is_valid = true);

  Status AppendNull() final { return Append(false); }

  Status AppendNulls(int64_t length) final;

  ArrayBuilder* value_builder() const;

 protected:
  TypedBufferBuilder<int32_t> offsets_builder_;
  std::shared_ptr<ArrayBuilder> value_builder_;
  std::shared_ptr<Array> values_;

  Status CheckNextOffset() const;
  Status AppendNextOffset();
  Status AppendNextOffset(int64_t num_repeats);
};

// ----------------------------------------------------------------------
// Map builder

/// \class MapBuilder
/// \brief Builder class for arrays of variable-size maps
///
/// To use this class, you must append values to the key and item array builders
/// and use the Append function to delimit each distinct map (once the keys and items
/// have been appended) or use the bulk API to append a sequence of offests and null
/// maps.
///
/// Key uniqueness and ordering are not validated.
class ARROW_EXPORT MapBuilder : public ArrayBuilder {
 public:
  /// Use this constructor to incrementally build the key and item arrays along with
  /// offsets and null bitmap.
  MapBuilder(MemoryPool* pool, const std::shared_ptr<ArrayBuilder>& key_builder,
             const std::shared_ptr<ArrayBuilder>& item_builder,
             const std::shared_ptr<DataType>& type);

  /// Derive built type from key and item builders' types
  MapBuilder(MemoryPool* pool, const std::shared_ptr<ArrayBuilder>& key_builder,
             const std::shared_ptr<ArrayBuilder>& item_builder, bool keys_sorted = false);

  Status Resize(int64_t capacity) override;
  void Reset() override;
  Status FinishInternal(std::shared_ptr<ArrayData>* out) override;

  /// \cond FALSE
  using ArrayBuilder::Finish;
  /// \endcond

  Status Finish(std::shared_ptr<MapArray>* out) { return FinishTyped(out); }

  /// \brief Vector append
  ///
  /// If passed, valid_bytes is of equal length to values, and any zero byte
  /// will be considered as a null for that slot
  Status AppendValues(const int32_t* offsets, int64_t length,
                      const uint8_t* valid_bytes = NULLPTR);

  /// \brief Start a new variable-length map slot
  ///
  /// This function should be called before beginning to append elements to the
  /// key and value builders
  Status Append();

  Status AppendNull() final;

  Status AppendNulls(int64_t length) final;

  ArrayBuilder* key_builder() const { return key_builder_.get(); }
  ArrayBuilder* item_builder() const { return item_builder_.get(); }

 protected:
  std::shared_ptr<ListBuilder> list_builder_;
  std::shared_ptr<ArrayBuilder> key_builder_;
  std::shared_ptr<ArrayBuilder> item_builder_;
};

// ----------------------------------------------------------------------
// FixedSizeList builder

/// \class FixedSizeListBuilder
/// \brief Builder class for fixed-length list array value types
class ARROW_EXPORT FixedSizeListBuilder : public ArrayBuilder {
 public:
  FixedSizeListBuilder(MemoryPool* pool,
                       std::shared_ptr<ArrayBuilder> const& value_builder,
                       int32_t list_size);

  FixedSizeListBuilder(MemoryPool* pool,
                       std::shared_ptr<ArrayBuilder> const& value_builder,
                       const std::shared_ptr<DataType>& type);

  Status Resize(int64_t capacity) override;
  void Reset() override;
  Status FinishInternal(std::shared_ptr<ArrayData>* out) override;

  /// \cond FALSE
  using ArrayBuilder::Finish;
  /// \endcond

  Status Finish(std::shared_ptr<FixedSizeListArray>* out) { return FinishTyped(out); }

  /// \brief Append a valid fixed length list.
  ///
  /// This function affects only the validity bitmap; the child values must be appended
  /// using the child array builder.
  Status Append();

  /// \brief Vector append
  ///
  /// If passed, valid_bytes wil be read and any zero byte
  /// will cause the corresponding slot to be null
  ///
  /// This function affects only the validity bitmap; the child values must be appended
  /// using the child array builder. This includes appending nulls for null lists.
  /// XXX this restriction is confusing, should this method be omitted?
  Status AppendValues(int64_t length, const uint8_t* valid_bytes = NULLPTR);

  /// \brief Append a null fixed length list.
  ///
  /// The child array builder will have the approriate number of nulls appended
  /// automatically.
  Status AppendNull() final;

  /// \brief Append length null fixed length lists.
  ///
  /// The child array builder will have the approriate number of nulls appended
  /// automatically.
  Status AppendNulls(int64_t length) final;

  ArrayBuilder* value_builder() const { return value_builder_.get(); }

 protected:
  const int32_t list_size_;
  std::shared_ptr<ArrayBuilder> value_builder_;
};

// ----------------------------------------------------------------------
// Struct

// ---------------------------------------------------------------------------------
// StructArray builder
/// Append, Resize and Reserve methods are acting on StructBuilder.
/// Please make sure all these methods of all child-builders' are consistently
/// called to maintain data-structure consistency.
class ARROW_EXPORT StructBuilder : public ArrayBuilder {
 public:
  StructBuilder(const std::shared_ptr<DataType>& type, MemoryPool* pool,
                std::vector<std::shared_ptr<ArrayBuilder>>&& field_builders);

  Status FinishInternal(std::shared_ptr<ArrayData>* out) override;

  /// \cond FALSE
  using ArrayBuilder::Finish;
  /// \endcond

  Status Finish(std::shared_ptr<StructArray>* out) { return FinishTyped(out); }

  /// Null bitmap is of equal length to every child field, and any zero byte
  /// will be considered as a null for that field, but users must using app-
  /// end methods or advance methods of the child builders' independently to
  /// insert data.
  Status AppendValues(int64_t length, const uint8_t* valid_bytes) {
    ARROW_RETURN_NOT_OK(Reserve(length));
    UnsafeAppendToBitmap(valid_bytes, length);
    return Status::OK();
  }

  /// Append an element to the Struct. All child-builders' Append method must
  /// be called independently to maintain data-structure consistency.
  Status Append(bool is_valid = true) {
    ARROW_RETURN_NOT_OK(Reserve(1));
    UnsafeAppendToBitmap(is_valid);
    return Status::OK();
  }

  Status AppendNull() final { return Append(false); }

  Status AppendNulls(int64_t length) final;

  void Reset() override;

  ArrayBuilder* field_builder(int i) const { return children_[i].get(); }

  int num_fields() const { return static_cast<int>(children_.size()); }
};

}  // namespace arrow

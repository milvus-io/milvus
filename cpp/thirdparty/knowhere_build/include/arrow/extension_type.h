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

/// User-defined extension types. EXPERIMENTAL in 0.13.0
/// \since 0.13.0

#pragma once

#include <memory>
#include <string>

#include "arrow/array.h"
#include "arrow/type.h"
#include "arrow/util/visibility.h"

namespace arrow {

/// \brief The base class for custom / user-defined types.
class ARROW_EXPORT ExtensionType : public DataType {
 public:
  static constexpr Type::type type_id = Type::EXTENSION;

  /// \brief The type of array used to represent this extension type's data
  std::shared_ptr<DataType> storage_type() const { return storage_type_; }

  DataTypeLayout layout() const override;

  std::string ToString() const override;
  std::string name() const override;

  /// \brief Unique name of extension type used to identify type for
  /// serialization
  /// \return the string name of the extension
  virtual std::string extension_name() const = 0;

  /// \brief Determine if two instances of the same extension types are
  /// equal. Invoked from ExtensionType::Equals
  /// \param[in] other the type to compare this type with
  /// \return bool true if type instances are equal
  virtual bool ExtensionEquals(const ExtensionType& other) const = 0;

  /// \brief Wrap built-in Array type in a user-defined ExtensionArray instance
  /// \param[in] data the physical storage for the extension type
  virtual std::shared_ptr<Array> MakeArray(std::shared_ptr<ArrayData> data) const = 0;

  /// \brief Create an instance of the ExtensionType given the actual storage
  /// type and the serialized representation
  /// \param[in] storage_type the physical storage type of the extension
  /// \param[in] serialized_data the serialized representation produced by
  /// Serialize
  /// \param[out] out the reconstructed extension type
  /// \return Status
  virtual Status Deserialize(std::shared_ptr<DataType> storage_type,
                             const std::string& serialized_data,
                             std::shared_ptr<DataType>* out) const = 0;

  /// \brief Create a serialized representation of the extension type's
  /// metadata. The storage type will be handled automatically in IPC code
  /// paths
  /// \return the serialized representation
  virtual std::string Serialize() const = 0;

 protected:
  explicit ExtensionType(std::shared_ptr<DataType> storage_type)
      : DataType(Type::EXTENSION), storage_type_(storage_type) {}

  std::shared_ptr<DataType> storage_type_;
};

/// \brief Base array class for user-defined extension types
class ARROW_EXPORT ExtensionArray : public Array {
 public:
  /// \brief Construct an ExtensionArray from an ArrayData.
  ///
  /// The ArrayData must have the right ExtensionType.
  explicit ExtensionArray(const std::shared_ptr<ArrayData>& data);

  /// \brief Construct an ExtensionArray from a type and the underlying storage.
  ExtensionArray(const std::shared_ptr<DataType>& type,
                 const std::shared_ptr<Array>& storage);

  const ExtensionType* extension_type() const {
    return internal::checked_cast<const ExtensionType*>(data_->type.get());
  }

  /// \brief The physical storage for the extension array
  std::shared_ptr<Array> storage() const { return storage_; }

 protected:
  void SetData(const std::shared_ptr<ArrayData>& data);
  std::shared_ptr<Array> storage_;
};

/// \brief Register an extension type globally. The name returned by the type's
/// extension_name() method should be unique. This method is thread-safe
/// \param[in] type an instance of the extension type
/// \return Status
ARROW_EXPORT
Status RegisterExtensionType(std::shared_ptr<ExtensionType> type);

/// \brief Delete an extension type from the global registry. This method is
/// thread-safe
/// \param[in] type_name the unique name of a registered extension type
/// \return Status error if the type name is unknown
ARROW_EXPORT
Status UnregisterExtensionType(const std::string& type_name);

/// \brief Retrieve an extension type from the global registry. Returns nullptr
/// if not found. This method is thread-safe
/// \return the globally-registered extension type
ARROW_EXPORT
std::shared_ptr<ExtensionType> GetExtensionType(const std::string& type_name);

}  // namespace arrow

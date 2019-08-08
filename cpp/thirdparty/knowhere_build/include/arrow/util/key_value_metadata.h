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

#ifndef ARROW_UTIL_KEY_VALUE_METADATA_H
#define ARROW_UTIL_KEY_VALUE_METADATA_H

#include <cstdint>
#include <memory>
#include <string>
#include <unordered_map>
#include <vector>

#include "arrow/util/macros.h"
#include "arrow/util/visibility.h"

namespace arrow {

/// \brief A container for key-value pair type metadata. Not thread-safe
class ARROW_EXPORT KeyValueMetadata {
 public:
  KeyValueMetadata();
  KeyValueMetadata(const std::vector<std::string>& keys,
                   const std::vector<std::string>& values);
  explicit KeyValueMetadata(const std::unordered_map<std::string, std::string>& map);
  virtual ~KeyValueMetadata() = default;

  void ToUnorderedMap(std::unordered_map<std::string, std::string>* out) const;

  void Append(const std::string& key, const std::string& value);

  void reserve(int64_t n);
  int64_t size() const;

  const std::string& key(int64_t i) const;
  const std::string& value(int64_t i) const;

  /// \brief Perform linear search for key, returning -1 if not found
  int FindKey(const std::string& key) const;

  std::shared_ptr<KeyValueMetadata> Copy() const;

  bool Equals(const KeyValueMetadata& other) const;
  std::string ToString() const;

 private:
  std::vector<std::string> keys_;
  std::vector<std::string> values_;

  ARROW_DISALLOW_COPY_AND_ASSIGN(KeyValueMetadata);
};

/// \brief Create a KeyValueMetadata instance
///
/// \param pairs key-value mapping
std::shared_ptr<KeyValueMetadata> ARROW_EXPORT
key_value_metadata(const std::unordered_map<std::string, std::string>& pairs);

/// \brief Create a KeyValueMetadata instance
///
/// \param keys sequence of metadata keys
/// \param values sequence of corresponding metadata values
std::shared_ptr<KeyValueMetadata> ARROW_EXPORT key_value_metadata(
    const std::vector<std::string>& keys, const std::vector<std::string>& values);

}  // namespace arrow

#endif  //  ARROW_UTIL_KEY_VALUE_METADATA_H

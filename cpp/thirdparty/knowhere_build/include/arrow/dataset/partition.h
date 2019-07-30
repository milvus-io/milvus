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
#include <utility>
#include <vector>

#include "arrow/dataset/dataset.h"
#include "arrow/dataset/type_fwd.h"
#include "arrow/dataset/visibility.h"

namespace arrow {
namespace dataset {

// ----------------------------------------------------------------------
// Computing partition values

// TODO(wesm): API for computing partition keys derived from raw
// values. For example, year(value) or hash_function(value) instead of
// simply value, so a dataset with a timestamp column might group all
// data with year 2009 in the same partition

// /// \brief
// class ScalarTransform {
//  public:
//   virtual Status Transform(const std::shared_ptr<Scalar>& input,
//                            std::shared_ptr<Scalar>* output) const = 0;
// };

// class PartitionField {
//  public:

//  private:
//   std::string field_name_;
// };

// ----------------------------------------------------------------------
// Partition identifiers

/// \brief A partition level identifier which can be used
///
/// TODO(wesm): Is this general enough? What other kinds of partition
/// keys exist and do we need to support them?
class PartitionKey {
 public:
  const std::vector<std::string>& fields() const { return fields_; }
  const std::vector<std::shared_ptr<Scalar>>& values() const { return values_; }

 private:
  std::vector<std::string> fields_;
  std::vector<std::shared_ptr<Scalar>> values_;
};

/// \brief Intermediate data structure for data parsed from a string
/// partition identifier.
///
/// For example, the identifier "foo=5" might be parsed with a single
/// "foo" field and the value 5. A more complex identifier might be
/// written as "foo=5,bar=2", which would yield two fields and two
/// values.
///
/// Some partition schemes may store the field names in a metadata
/// store instead of in file paths, for example
/// dataset_root/2009/11/... could be used when the partition fields
/// are "year" and "month"
struct PartitionKeyData {
  std::vector<std::string> fields;
  std::vector<std::shared_ptr<Scalar>> values;
};

// ----------------------------------------------------------------------
// Partition schemes

/// \brief
class ARROW_DS_EXPORT PartitionScheme {
 public:
  virtual ~PartitionScheme() = default;

  /// \brief The name identifying the kind of partition scheme
  virtual std::string name() const = 0;

  virtual bool PathMatchesScheme(const std::string& path) const = 0;

  virtual Status ParseKey(const std::string& path, PartitionKeyData* out) const = 0;
};

/// \brief Multi-level, directory based partitioning scheme
/// originating from Apache Hive with all data files stored in the
/// leaf directories. Data is partitioned by static values of a
/// particular column in the schema. Partition keys are represented in
/// the form $key=$value in directory names
class ARROW_DS_EXPORT HivePartitionScheme : public PartitionScheme {
 public:
  /// \brief Return true if path
  bool PathMatchesScheme(const std::string& path) const override;

  virtual Status ParseKey(const std::string& path, PartitionKeyData* out) const = 0;
};

// ----------------------------------------------------------------------
//

// Partitioned datasets come in different forms. Here is an example of
// a Hive-style partitioned dataset:
//
// dataset_root/
//   key1=$k1_v1/
//     key2=$k2_v1/
//       0.parquet
//       1.parquet
//       2.parquet
//       3.parquet
//     key2=$k2_v2/
//       0.parquet
//       1.parquet
//   key1=$k1_v2/
//     key2=$k2_v1/
//       0.parquet
//       1.parquet
//     key2=$k2_v2/
//       0.parquet
//       1.parquet
//       2.parquet
//
// In this case, the dataset has 11 fragments (11 files) to be
// scanned, or potentially more if it is configured to split Parquet
// files at the row group level

class ARROW_DS_EXPORT Partition : public DataSource {
 public:
  std::string type() const override;

  /// \brief The key for this partition source, may be nullptr,
  /// e.g. for the top-level partitioned source container
  virtual const PartitionKey* key() const = 0;

  virtual std::unique_ptr<DataFragmentIterator> GetFragments(
      const Selector& selector) = 0;
};

/// \brief Simple implementation of Partition, which consists of a
/// partition identifier, subpartitions, and some data fragments
class ARROW_DS_EXPORT SimplePartition : public Partition {
 public:
  SimplePartition(std::unique_ptr<PartitionKey> partition_key,
                  DataFragmentVector&& data_fragments, PartitionVector&& subpartitions,
                  std::shared_ptr<ScanOptions> scan_options = NULLPTR)
      : key_(std::move(partition_key)),
        data_fragments_(std::move(data_fragments)),
        subpartitions_(std::move(subpartitions)),
        scan_options_(scan_options) {}

  const PartitionKey* key() const override { return key_.get(); }

  int num_subpartitions() const { return static_cast<int>(subpartitions_.size()); }

  int num_data_fragments() const { return static_cast<int>(data_fragments__.size()); }

  const PartitionVector& subpartitions() const { return subpartitions_; }
  const DataFragmentVector& data_fragments() const { return data_fragments_; }

  std::unique_ptr<DataFragmentIterator> GetFragments(
      const FilterVector& filters) override;

 private:
  std::unique_ptr<PartitionKey> key_;

  /// \brief Data fragments belonging to this partition level. In some
  /// partition schemes such as Hive-style, this member is
  /// mutually-exclusive with subpartitions, where data fragments
  /// occur only in the partition leaves
  std::vector<std::shared_ptr<DataFragment>> data_fragments_;

  /// \brief Child partitions of this partition
  std::vector<std::shared_ptr<Partition>> subpartitions_;

  /// \brief Default scan options to use for data fragments
  std::shared_ptr<ScanOptions> scan_options_;
};

/// \brief A PartitionSource that returns fragments as the result of input iterators
class ARROW_DS_EXPORT LazyPartition : public Partition {
 public:
  const PartitionKey* key() const override;

  std::unique_ptr<DataFragmentIterator> GetFragments(
      const& DataSelector selector) override;

  // TODO(wesm): Iterate over subpartitions

 protected:
  std::unique_ptr<PartitionIterator> partition_iter_;

  // By default, once this source is consumed using GetFragments, it
  // cannot be consumed again. By setting this to true, we cache
  bool cache_manifest_ = false;
};

}  // namespace dataset
}  // namespace arrow

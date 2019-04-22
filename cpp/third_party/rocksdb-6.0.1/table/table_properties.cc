//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include "rocksdb/table_properties.h"
#include "port/port.h"
#include "rocksdb/env.h"
#include "rocksdb/iterator.h"
#include "table/block.h"
#include "table/internal_iterator.h"
#include "table/table_properties_internal.h"
#include "util/string_util.h"

namespace rocksdb {

const uint32_t TablePropertiesCollectorFactory::Context::kUnknownColumnFamily =
    port::kMaxInt32;

namespace {
  void AppendProperty(
      std::string& props,
      const std::string& key,
      const std::string& value,
      const std::string& prop_delim,
      const std::string& kv_delim) {
    props.append(key);
    props.append(kv_delim);
    props.append(value);
    props.append(prop_delim);
  }

  template <class TValue>
  void AppendProperty(
      std::string& props,
      const std::string& key,
      const TValue& value,
      const std::string& prop_delim,
      const std::string& kv_delim) {
    AppendProperty(
        props, key, ToString(value), prop_delim, kv_delim
    );
  }

  // Seek to the specified meta block.
  // Return true if it successfully seeks to that block.
  Status SeekToMetaBlock(InternalIterator* meta_iter,
                         const std::string& block_name, bool* is_found,
                         BlockHandle* block_handle = nullptr) {
    if (block_handle != nullptr) {
      *block_handle = BlockHandle::NullBlockHandle();
    }
    *is_found = true;
    meta_iter->Seek(block_name);
    if (meta_iter->status().ok()) {
      if (meta_iter->Valid() && meta_iter->key() == block_name) {
        *is_found = true;
        if (block_handle) {
          Slice v = meta_iter->value();
          return block_handle->DecodeFrom(&v);
        }
      } else {
        *is_found = false;
        return Status::OK();
      }
    }
    return meta_iter->status();
  }
}

std::string TableProperties::ToString(
    const std::string& prop_delim,
    const std::string& kv_delim) const {
  std::string result;
  result.reserve(1024);

  // Basic Info
  AppendProperty(result, "# data blocks", num_data_blocks, prop_delim,
                 kv_delim);
  AppendProperty(result, "# entries", num_entries, prop_delim, kv_delim);
  AppendProperty(result, "# deletions", num_deletions, prop_delim, kv_delim);
  AppendProperty(result, "# merge operands", num_merge_operands, prop_delim,
                 kv_delim);
  AppendProperty(result, "# range deletions", num_range_deletions, prop_delim,
                 kv_delim);

  AppendProperty(result, "raw key size", raw_key_size, prop_delim, kv_delim);
  AppendProperty(result, "raw average key size",
                 num_entries != 0 ? 1.0 * raw_key_size / num_entries : 0.0,
                 prop_delim, kv_delim);
  AppendProperty(result, "raw value size", raw_value_size, prop_delim,
                 kv_delim);
  AppendProperty(result, "raw average value size",
                 num_entries != 0 ? 1.0 * raw_value_size / num_entries : 0.0,
                 prop_delim, kv_delim);

  AppendProperty(result, "data block size", data_size, prop_delim, kv_delim);
  char index_block_size_str[80];
  snprintf(index_block_size_str, sizeof(index_block_size_str),
           "index block size (user-key? %d, delta-value? %d)",
           static_cast<int>(index_key_is_user_key),
           static_cast<int>(index_value_is_delta_encoded));
  AppendProperty(result, index_block_size_str, index_size, prop_delim,
                 kv_delim);
  if (index_partitions != 0) {
    AppendProperty(result, "# index partitions", index_partitions, prop_delim,
                   kv_delim);
    AppendProperty(result, "top-level index size", top_level_index_size, prop_delim,
                   kv_delim);
  }
  AppendProperty(result, "filter block size", filter_size, prop_delim,
                 kv_delim);
  AppendProperty(result, "(estimated) table size",
                 data_size + index_size + filter_size, prop_delim, kv_delim);

  AppendProperty(
      result, "filter policy name",
      filter_policy_name.empty() ? std::string("N/A") : filter_policy_name,
      prop_delim, kv_delim);

  AppendProperty(result, "prefix extractor name",
                 prefix_extractor_name.empty() ? std::string("N/A")
                                               : prefix_extractor_name,
                 prop_delim, kv_delim);

  AppendProperty(result, "column family ID",
                 column_family_id == rocksdb::TablePropertiesCollectorFactory::
                                         Context::kUnknownColumnFamily
                     ? std::string("N/A")
                     : rocksdb::ToString(column_family_id),
                 prop_delim, kv_delim);
  AppendProperty(
      result, "column family name",
      column_family_name.empty() ? std::string("N/A") : column_family_name,
      prop_delim, kv_delim);

  AppendProperty(result, "comparator name",
                 comparator_name.empty() ? std::string("N/A") : comparator_name,
                 prop_delim, kv_delim);

  AppendProperty(
      result, "merge operator name",
      merge_operator_name.empty() ? std::string("N/A") : merge_operator_name,
      prop_delim, kv_delim);

  AppendProperty(result, "property collectors names",
                 property_collectors_names.empty() ? std::string("N/A")
                                                   : property_collectors_names,
                 prop_delim, kv_delim);

  AppendProperty(
      result, "SST file compression algo",
      compression_name.empty() ? std::string("N/A") : compression_name,
      prop_delim, kv_delim);

  AppendProperty(result, "creation time", creation_time, prop_delim, kv_delim);

  AppendProperty(result, "time stamp of earliest key", oldest_key_time,
                 prop_delim, kv_delim);

  return result;
}

void TableProperties::Add(const TableProperties& tp) {
  data_size += tp.data_size;
  index_size += tp.index_size;
  index_partitions += tp.index_partitions;
  top_level_index_size += tp.top_level_index_size;
  index_key_is_user_key += tp.index_key_is_user_key;
  index_value_is_delta_encoded += tp.index_value_is_delta_encoded;
  filter_size += tp.filter_size;
  raw_key_size += tp.raw_key_size;
  raw_value_size += tp.raw_value_size;
  num_data_blocks += tp.num_data_blocks;
  num_entries += tp.num_entries;
  num_deletions += tp.num_deletions;
  num_merge_operands += tp.num_merge_operands;
  num_range_deletions += tp.num_range_deletions;
}

const std::string TablePropertiesNames::kDataSize  =
    "rocksdb.data.size";
const std::string TablePropertiesNames::kIndexSize =
    "rocksdb.index.size";
const std::string TablePropertiesNames::kIndexPartitions =
    "rocksdb.index.partitions";
const std::string TablePropertiesNames::kTopLevelIndexSize =
    "rocksdb.top-level.index.size";
const std::string TablePropertiesNames::kIndexKeyIsUserKey =
    "rocksdb.index.key.is.user.key";
const std::string TablePropertiesNames::kIndexValueIsDeltaEncoded =
    "rocksdb.index.value.is.delta.encoded";
const std::string TablePropertiesNames::kFilterSize =
    "rocksdb.filter.size";
const std::string TablePropertiesNames::kRawKeySize =
    "rocksdb.raw.key.size";
const std::string TablePropertiesNames::kRawValueSize =
    "rocksdb.raw.value.size";
const std::string TablePropertiesNames::kNumDataBlocks =
    "rocksdb.num.data.blocks";
const std::string TablePropertiesNames::kNumEntries =
    "rocksdb.num.entries";
const std::string TablePropertiesNames::kDeletedKeys = "rocksdb.deleted.keys";
const std::string TablePropertiesNames::kMergeOperands =
    "rocksdb.merge.operands";
const std::string TablePropertiesNames::kNumRangeDeletions =
    "rocksdb.num.range-deletions";
const std::string TablePropertiesNames::kFilterPolicy =
    "rocksdb.filter.policy";
const std::string TablePropertiesNames::kFormatVersion =
    "rocksdb.format.version";
const std::string TablePropertiesNames::kFixedKeyLen =
    "rocksdb.fixed.key.length";
const std::string TablePropertiesNames::kColumnFamilyId =
    "rocksdb.column.family.id";
const std::string TablePropertiesNames::kColumnFamilyName =
    "rocksdb.column.family.name";
const std::string TablePropertiesNames::kComparator = "rocksdb.comparator";
const std::string TablePropertiesNames::kMergeOperator =
    "rocksdb.merge.operator";
const std::string TablePropertiesNames::kPrefixExtractorName =
    "rocksdb.prefix.extractor.name";
const std::string TablePropertiesNames::kPropertyCollectors =
    "rocksdb.property.collectors";
const std::string TablePropertiesNames::kCompression = "rocksdb.compression";
const std::string TablePropertiesNames::kCreationTime = "rocksdb.creation.time";
const std::string TablePropertiesNames::kOldestKeyTime =
    "rocksdb.oldest.key.time";

extern const std::string kPropertiesBlock = "rocksdb.properties";
// Old property block name for backward compatibility
extern const std::string kPropertiesBlockOldName = "rocksdb.stats";
extern const std::string kCompressionDictBlock = "rocksdb.compression_dict";
extern const std::string kRangeDelBlock = "rocksdb.range_del";

// Seek to the properties block.
// Return true if it successfully seeks to the properties block.
Status SeekToPropertiesBlock(InternalIterator* meta_iter, bool* is_found) {
  Status status = SeekToMetaBlock(meta_iter, kPropertiesBlock, is_found);
  if (!*is_found && status.ok()) {
    status = SeekToMetaBlock(meta_iter, kPropertiesBlockOldName, is_found);
  }
  return status;
}

// Seek to the compression dictionary block.
// Return true if it successfully seeks to that block.
Status SeekToCompressionDictBlock(InternalIterator* meta_iter, bool* is_found,
                                  BlockHandle* block_handle) {
  return SeekToMetaBlock(meta_iter, kCompressionDictBlock, is_found, block_handle);
}

Status SeekToRangeDelBlock(InternalIterator* meta_iter, bool* is_found,
                           BlockHandle* block_handle = nullptr) {
  return SeekToMetaBlock(meta_iter, kRangeDelBlock, is_found, block_handle);
}

}  // namespace rocksdb

// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#ifndef ROCKSDB_LITE
#include "table/plain_table_factory.h"

#include <stdint.h>
#include <memory>
#include "db/dbformat.h"
#include "options/options_helper.h"
#include "port/port.h"
#include "rocksdb/convenience.h"
#include "table/plain_table_builder.h"
#include "table/plain_table_reader.h"
#include "util/string_util.h"

namespace rocksdb {

Status PlainTableFactory::NewTableReader(
    const TableReaderOptions& table_reader_options,
    std::unique_ptr<RandomAccessFileReader>&& file, uint64_t file_size,
    std::unique_ptr<TableReader>* table,
    bool /*prefetch_index_and_filter_in_cache*/) const {
  return PlainTableReader::Open(
      table_reader_options.ioptions, table_reader_options.env_options,
      table_reader_options.internal_comparator, std::move(file), file_size,
      table, table_options_.bloom_bits_per_key, table_options_.hash_table_ratio,
      table_options_.index_sparseness, table_options_.huge_page_tlb_size,
      table_options_.full_scan_mode, table_reader_options.immortal,
      table_reader_options.prefix_extractor);
}

TableBuilder* PlainTableFactory::NewTableBuilder(
    const TableBuilderOptions& table_builder_options, uint32_t column_family_id,
    WritableFileWriter* file) const {
  // Ignore the skip_filters flag. PlainTable format is optimized for small
  // in-memory dbs. The skip_filters optimization is not useful for plain
  // tables
  //
  return new PlainTableBuilder(
      table_builder_options.ioptions, table_builder_options.moptions,
      table_builder_options.int_tbl_prop_collector_factories, column_family_id,
      file, table_options_.user_key_len, table_options_.encoding_type,
      table_options_.index_sparseness, table_options_.bloom_bits_per_key,
      table_builder_options.column_family_name, 6,
      table_options_.huge_page_tlb_size, table_options_.hash_table_ratio,
      table_options_.store_index_in_file);
}

std::string PlainTableFactory::GetPrintableTableOptions() const {
  std::string ret;
  ret.reserve(20000);
  const int kBufferSize = 200;
  char buffer[kBufferSize];

  snprintf(buffer, kBufferSize, "  user_key_len: %u\n",
           table_options_.user_key_len);
  ret.append(buffer);
  snprintf(buffer, kBufferSize, "  bloom_bits_per_key: %d\n",
           table_options_.bloom_bits_per_key);
  ret.append(buffer);
  snprintf(buffer, kBufferSize, "  hash_table_ratio: %lf\n",
           table_options_.hash_table_ratio);
  ret.append(buffer);
  snprintf(buffer, kBufferSize, "  index_sparseness: %" ROCKSDB_PRIszt "\n",
           table_options_.index_sparseness);
  ret.append(buffer);
  snprintf(buffer, kBufferSize, "  huge_page_tlb_size: %" ROCKSDB_PRIszt "\n",
           table_options_.huge_page_tlb_size);
  ret.append(buffer);
  snprintf(buffer, kBufferSize, "  encoding_type: %d\n",
           table_options_.encoding_type);
  ret.append(buffer);
  snprintf(buffer, kBufferSize, "  full_scan_mode: %d\n",
           table_options_.full_scan_mode);
  ret.append(buffer);
  snprintf(buffer, kBufferSize, "  store_index_in_file: %d\n",
           table_options_.store_index_in_file);
  ret.append(buffer);
  return ret;
}

const PlainTableOptions& PlainTableFactory::table_options() const {
  return table_options_;
}

Status GetPlainTableOptionsFromString(const PlainTableOptions& table_options,
                                      const std::string& opts_str,
                                      PlainTableOptions* new_table_options) {
  std::unordered_map<std::string, std::string> opts_map;
  Status s = StringToMap(opts_str, &opts_map);
  if (!s.ok()) {
    return s;
  }
  return GetPlainTableOptionsFromMap(table_options, opts_map,
                                     new_table_options);
}

Status GetMemTableRepFactoryFromString(
    const std::string& opts_str,
    std::unique_ptr<MemTableRepFactory>* new_mem_factory) {
  std::vector<std::string> opts_list = StringSplit(opts_str, ':');
  size_t len = opts_list.size();

  if (opts_list.empty() || opts_list.size() > 2) {
    return Status::InvalidArgument("Can't parse memtable_factory option ",
                                   opts_str);
  }

  MemTableRepFactory* mem_factory = nullptr;

  if (opts_list[0] == "skip_list") {
    // Expecting format
    // skip_list:<lookahead>
    if (2 == len) {
      size_t lookahead = ParseSizeT(opts_list[1]);
      mem_factory = new SkipListFactory(lookahead);
    } else if (1 == len) {
      mem_factory = new SkipListFactory();
    }
  } else if (opts_list[0] == "prefix_hash") {
    // Expecting format
    // prfix_hash:<hash_bucket_count>
    if (2 == len) {
      size_t hash_bucket_count = ParseSizeT(opts_list[1]);
      mem_factory = NewHashSkipListRepFactory(hash_bucket_count);
    } else if (1 == len) {
      mem_factory = NewHashSkipListRepFactory();
    }
  } else if (opts_list[0] == "hash_linkedlist") {
    // Expecting format
    // hash_linkedlist:<hash_bucket_count>
    if (2 == len) {
      size_t hash_bucket_count = ParseSizeT(opts_list[1]);
      mem_factory = NewHashLinkListRepFactory(hash_bucket_count);
    } else if (1 == len) {
      mem_factory = NewHashLinkListRepFactory();
    }
  } else if (opts_list[0] == "vector") {
    // Expecting format
    // vector:<count>
    if (2 == len) {
      size_t count = ParseSizeT(opts_list[1]);
      mem_factory = new VectorRepFactory(count);
    } else if (1 == len) {
      mem_factory = new VectorRepFactory();
    }
  } else if (opts_list[0] == "cuckoo") {
    return Status::NotSupported(
        "cuckoo hash memtable is not supported anymore.");
  } else {
    return Status::InvalidArgument("Unrecognized memtable_factory option ",
                                   opts_str);
  }

  if (mem_factory != nullptr) {
    new_mem_factory->reset(mem_factory);
  }

  return Status::OK();
}

std::string ParsePlainTableOptions(const std::string& name,
                                   const std::string& org_value,
                                   PlainTableOptions* new_options,
                                   bool input_strings_escaped = false,
                                   bool ignore_unknown_options = false) {
  const std::string& value =
      input_strings_escaped ? UnescapeOptionString(org_value) : org_value;
  const auto iter = plain_table_type_info.find(name);
  if (iter == plain_table_type_info.end()) {
    if (ignore_unknown_options) {
      return "";
    } else {
      return "Unrecognized option";
    }
  }
  const auto& opt_info = iter->second;
  if (opt_info.verification != OptionVerificationType::kDeprecated &&
      !ParseOptionHelper(reinterpret_cast<char*>(new_options) + opt_info.offset,
                         opt_info.type, value)) {
    return "Invalid value";
  }
  return "";
}

Status GetPlainTableOptionsFromMap(
    const PlainTableOptions& table_options,
    const std::unordered_map<std::string, std::string>& opts_map,
    PlainTableOptions* new_table_options, bool input_strings_escaped,
    bool /*ignore_unknown_options*/) {
  assert(new_table_options);
  *new_table_options = table_options;
  for (const auto& o : opts_map) {
    auto error_message = ParsePlainTableOptions(
        o.first, o.second, new_table_options, input_strings_escaped);
    if (error_message != "") {
      const auto iter = plain_table_type_info.find(o.first);
      if (iter == plain_table_type_info.end() ||
          !input_strings_escaped ||  // !input_strings_escaped indicates
                                     // the old API, where everything is
                                     // parsable.
          (iter->second.verification != OptionVerificationType::kByName &&
           iter->second.verification !=
               OptionVerificationType::kByNameAllowNull &&
           iter->second.verification !=
               OptionVerificationType::kByNameAllowFromNull &&
           iter->second.verification != OptionVerificationType::kDeprecated)) {
        // Restore "new_options" to the default "base_options".
        *new_table_options = table_options;
        return Status::InvalidArgument("Can't parse PlainTableOptions:",
                                       o.first + " " + error_message);
      }
    }
  }
  return Status::OK();
}

extern TableFactory* NewPlainTableFactory(const PlainTableOptions& options) {
  return new PlainTableFactory(options);
}

const std::string PlainTablePropertyNames::kEncodingType =
    "rocksdb.plain.table.encoding.type";

const std::string PlainTablePropertyNames::kBloomVersion =
    "rocksdb.plain.table.bloom.version";

const std::string PlainTablePropertyNames::kNumBloomBlocks =
    "rocksdb.plain.table.bloom.numblocks";

}  // namespace rocksdb
#endif  // ROCKSDB_LITE

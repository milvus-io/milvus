// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#pragma once

#include <string>
#include <unordered_map>

#ifndef ROCKSDB_LITE
namespace rocksdb {
// This enum defines the RocksDB options sanity level.
enum OptionsSanityCheckLevel : unsigned char {
  // Performs no sanity check at all.
  kSanityLevelNone = 0x00,
  // Performs minimum check to ensure the RocksDB instance can be
  // opened without corrupting / mis-interpreting the data.
  kSanityLevelLooselyCompatible = 0x01,
  // Perform exact match sanity check.
  kSanityLevelExactMatch = 0xFF,
};

// The sanity check level for DB options
static const std::unordered_map<std::string, OptionsSanityCheckLevel>
    sanity_level_db_options {};

// The sanity check level for column-family options
static const std::unordered_map<std::string, OptionsSanityCheckLevel>
    sanity_level_cf_options = {
        {"comparator", kSanityLevelLooselyCompatible},
        {"table_factory", kSanityLevelLooselyCompatible},
        {"merge_operator", kSanityLevelLooselyCompatible}};

// The sanity check level for block-based table options
static const std::unordered_map<std::string, OptionsSanityCheckLevel>
    sanity_level_bbt_options {};

OptionsSanityCheckLevel DBOptionSanityCheckLevel(
    const std::string& options_name);
OptionsSanityCheckLevel CFOptionSanityCheckLevel(
    const std::string& options_name);
OptionsSanityCheckLevel BBTOptionSanityCheckLevel(
    const std::string& options_name);

}  // namespace rocksdb

#endif  // !ROCKSDB_LITE

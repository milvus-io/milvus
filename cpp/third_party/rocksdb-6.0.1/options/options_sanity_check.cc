// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#ifndef ROCKSDB_LITE

#include "options/options_sanity_check.h"

namespace rocksdb {

namespace {
OptionsSanityCheckLevel SanityCheckLevelHelper(
    const std::unordered_map<std::string, OptionsSanityCheckLevel>& smap,
    const std::string& name) {
  auto iter = smap.find(name);
  return iter != smap.end() ? iter->second : kSanityLevelExactMatch;
}
}

OptionsSanityCheckLevel DBOptionSanityCheckLevel(
    const std::string& option_name) {
  return SanityCheckLevelHelper(sanity_level_db_options, option_name);
}

OptionsSanityCheckLevel CFOptionSanityCheckLevel(
    const std::string& option_name) {
  return SanityCheckLevelHelper(sanity_level_cf_options, option_name);
}

OptionsSanityCheckLevel BBTOptionSanityCheckLevel(
    const std::string& option_name) {
  return SanityCheckLevelHelper(sanity_level_bbt_options, option_name);
}

}  // namespace rocksdb

#endif  // !ROCKSDB_LITE

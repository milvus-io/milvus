//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#pragma once

#include <string>
#include "rocksdb/options.h"
#include "rocksdb/status.h"

namespace rocksdb {
// Try to migrate DB created with old_opts to be use new_opts.
// Multiple column families is not supported.
// It is best-effort. No guarantee to succeed.
// A full compaction may be executed.
Status OptionChangeMigration(std::string dbname, const Options& old_opts,
                             const Options& new_opts);
}  // namespace rocksdb

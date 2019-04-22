// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
#pragma once

#define ROCKSDB_MAJOR 6
#define ROCKSDB_MINOR 0
#define ROCKSDB_PATCH 1

// Do not use these. We made the mistake of declaring macros starting with
// double underscore. Now we have to live with our choice. We'll deprecate these
// at some point
#define __ROCKSDB_MAJOR__ ROCKSDB_MAJOR
#define __ROCKSDB_MINOR__ ROCKSDB_MINOR
#define __ROCKSDB_PATCH__ ROCKSDB_PATCH

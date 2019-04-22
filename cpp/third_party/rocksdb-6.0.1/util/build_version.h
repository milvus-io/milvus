//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
#pragma once
#if !defined(IOS_CROSS_COMPILE)
// if we compile with Xcode, we don't run build_detect_version, so we don't
// generate these variables
// this variable tells us about the git revision
extern const char* rocksdb_build_git_sha;

// Date on which the code was compiled:
extern const char* rocksdb_build_compile_date;
#endif

//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#pragma once

// Xpress on Windows is implemeted using Win API
#if defined(ROCKSDB_PLATFORM_POSIX)
#error "Xpress compression not implemented"
#elif defined(OS_WIN)
#include "port/win/xpress_win.h"
#endif

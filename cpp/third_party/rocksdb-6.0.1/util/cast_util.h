//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#pragma once

namespace rocksdb {
// The helper function to assert the move from dynamic_cast<> to
// static_cast<> is correct. This function is to deal with legacy code.
// It is not recommanded to add new code to issue class casting. The preferred
// solution is to implement the functionality without a need of casting.
template <class DestClass, class SrcClass>
inline DestClass* static_cast_with_check(SrcClass* x) {
  DestClass* ret = static_cast<DestClass*>(x);
#ifdef ROCKSDB_USE_RTTI
  assert(ret == dynamic_cast<DestClass*>(x));
#endif
  return ret;
}
}  // namespace rocksdb

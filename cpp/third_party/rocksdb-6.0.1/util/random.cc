//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//

#include "util/random.h"

#include <stdint.h>
#include <string.h>
#include <thread>
#include <utility>

#include "port/likely.h"
#include "util/thread_local.h"

#ifdef ROCKSDB_SUPPORT_THREAD_LOCAL
#define STORAGE_DECL static __thread
#else
#define STORAGE_DECL static
#endif

namespace rocksdb {

Random* Random::GetTLSInstance() {
  STORAGE_DECL Random* tls_instance;
  STORAGE_DECL std::aligned_storage<sizeof(Random)>::type tls_instance_bytes;

  auto rv = tls_instance;
  if (UNLIKELY(rv == nullptr)) {
    size_t seed = std::hash<std::thread::id>()(std::this_thread::get_id());
    rv = new (&tls_instance_bytes) Random((uint32_t)seed);
    tls_instance = rv;
  }
  return rv;
}

}  // namespace rocksdb

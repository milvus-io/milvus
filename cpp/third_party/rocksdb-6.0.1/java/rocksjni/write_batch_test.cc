// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// This file implements the "bridge" between Java and C++ and enables
// calling c++ rocksdb::WriteBatch methods testing from Java side.
#include <memory>

#include "db/memtable.h"
#include "db/write_batch_internal.h"
#include "include/org_rocksdb_WriteBatch.h"
#include "include/org_rocksdb_WriteBatchTest.h"
#include "include/org_rocksdb_WriteBatchTestInternalHelper.h"
#include "include/org_rocksdb_WriteBatch_Handler.h"
#include "options/cf_options.h"
#include "rocksdb/db.h"
#include "rocksdb/env.h"
#include "rocksdb/memtablerep.h"
#include "rocksdb/status.h"
#include "rocksdb/write_batch.h"
#include "rocksdb/write_buffer_manager.h"
#include "rocksjni/portal.h"
#include "table/scoped_arena_iterator.h"
#include "util/string_util.h"
#include "util/testharness.h"

/*
 * Class:     org_rocksdb_WriteBatchTest
 * Method:    getContents
 * Signature: (J)[B
 */
jbyteArray Java_org_rocksdb_WriteBatchTest_getContents(JNIEnv* env,
                                                       jclass /*jclazz*/,
                                                       jlong jwb_handle) {
  auto* b = reinterpret_cast<rocksdb::WriteBatch*>(jwb_handle);
  assert(b != nullptr);

  // todo: Currently the following code is directly copied from
  // db/write_bench_test.cc.  It could be implemented in java once
  // all the necessary components can be accessed via jni api.

  rocksdb::InternalKeyComparator cmp(rocksdb::BytewiseComparator());
  auto factory = std::make_shared<rocksdb::SkipListFactory>();
  rocksdb::Options options;
  rocksdb::WriteBufferManager wb(options.db_write_buffer_size);
  options.memtable_factory = factory;
  rocksdb::MemTable* mem = new rocksdb::MemTable(
      cmp, rocksdb::ImmutableCFOptions(options),
      rocksdb::MutableCFOptions(options), &wb, rocksdb::kMaxSequenceNumber,
      0 /* column_family_id */);
  mem->Ref();
  std::string state;
  rocksdb::ColumnFamilyMemTablesDefault cf_mems_default(mem);
  rocksdb::Status s =
      rocksdb::WriteBatchInternal::InsertInto(b, &cf_mems_default, nullptr);
  int count = 0;
  rocksdb::Arena arena;
  rocksdb::ScopedArenaIterator iter(
      mem->NewIterator(rocksdb::ReadOptions(), &arena));
  for (iter->SeekToFirst(); iter->Valid(); iter->Next()) {
    rocksdb::ParsedInternalKey ikey;
    ikey.clear();
    bool parsed = rocksdb::ParseInternalKey(iter->key(), &ikey);
    if (!parsed) {
      assert(parsed);
    }
    switch (ikey.type) {
      case rocksdb::kTypeValue:
        state.append("Put(");
        state.append(ikey.user_key.ToString());
        state.append(", ");
        state.append(iter->value().ToString());
        state.append(")");
        count++;
        break;
      case rocksdb::kTypeMerge:
        state.append("Merge(");
        state.append(ikey.user_key.ToString());
        state.append(", ");
        state.append(iter->value().ToString());
        state.append(")");
        count++;
        break;
      case rocksdb::kTypeDeletion:
        state.append("Delete(");
        state.append(ikey.user_key.ToString());
        state.append(")");
        count++;
        break;
      case rocksdb::kTypeSingleDeletion:
        state.append("SingleDelete(");
        state.append(ikey.user_key.ToString());
        state.append(")");
        count++;
        break;
      case rocksdb::kTypeRangeDeletion:
        state.append("DeleteRange(");
        state.append(ikey.user_key.ToString());
        state.append(", ");
        state.append(iter->value().ToString());
        state.append(")");
        count++;
        break;
      case rocksdb::kTypeLogData:
        state.append("LogData(");
        state.append(ikey.user_key.ToString());
        state.append(")");
        count++;
        break;
      default:
        assert(false);
        state.append("Err:Expected(");
        state.append(std::to_string(ikey.type));
        state.append(")");
        count++;
        break;
    }
    state.append("@");
    state.append(rocksdb::NumberToString(ikey.sequence));
  }
  if (!s.ok()) {
    state.append(s.ToString());
  } else if (rocksdb::WriteBatchInternal::Count(b) != count) {
    state.append("Err:CountMismatch(expected=");
    state.append(std::to_string(rocksdb::WriteBatchInternal::Count(b)));
    state.append(", actual=");
    state.append(std::to_string(count));
    state.append(")");
  }
  delete mem->Unref();

  jbyteArray jstate = env->NewByteArray(static_cast<jsize>(state.size()));
  if (jstate == nullptr) {
    // exception thrown: OutOfMemoryError
    return nullptr;
  }

  env->SetByteArrayRegion(
      jstate, 0, static_cast<jsize>(state.size()),
      const_cast<jbyte*>(reinterpret_cast<const jbyte*>(state.c_str())));
  if (env->ExceptionCheck()) {
    // exception thrown: ArrayIndexOutOfBoundsException
    env->DeleteLocalRef(jstate);
    return nullptr;
  }

  return jstate;
}

/*
 * Class:     org_rocksdb_WriteBatchTestInternalHelper
 * Method:    setSequence
 * Signature: (JJ)V
 */
void Java_org_rocksdb_WriteBatchTestInternalHelper_setSequence(
    JNIEnv* /*env*/, jclass /*jclazz*/, jlong jwb_handle, jlong jsn) {
  auto* wb = reinterpret_cast<rocksdb::WriteBatch*>(jwb_handle);
  assert(wb != nullptr);

  rocksdb::WriteBatchInternal::SetSequence(
      wb, static_cast<rocksdb::SequenceNumber>(jsn));
}

/*
 * Class:     org_rocksdb_WriteBatchTestInternalHelper
 * Method:    sequence
 * Signature: (J)J
 */
jlong Java_org_rocksdb_WriteBatchTestInternalHelper_sequence(JNIEnv* /*env*/,
                                                             jclass /*jclazz*/,
                                                             jlong jwb_handle) {
  auto* wb = reinterpret_cast<rocksdb::WriteBatch*>(jwb_handle);
  assert(wb != nullptr);

  return static_cast<jlong>(rocksdb::WriteBatchInternal::Sequence(wb));
}

/*
 * Class:     org_rocksdb_WriteBatchTestInternalHelper
 * Method:    append
 * Signature: (JJ)V
 */
void Java_org_rocksdb_WriteBatchTestInternalHelper_append(JNIEnv* /*env*/,
                                                          jclass /*jclazz*/,
                                                          jlong jwb_handle_1,
                                                          jlong jwb_handle_2) {
  auto* wb1 = reinterpret_cast<rocksdb::WriteBatch*>(jwb_handle_1);
  assert(wb1 != nullptr);
  auto* wb2 = reinterpret_cast<rocksdb::WriteBatch*>(jwb_handle_2);
  assert(wb2 != nullptr);

  rocksdb::WriteBatchInternal::Append(wb1, wb2);
}

// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// This file implements the "bridge" between Java and C++ for rocksdb::Options.

#include "rocksdb/table.h"
#include <jni.h>
#include "include/org_rocksdb_BlockBasedTableConfig.h"
#include "include/org_rocksdb_PlainTableConfig.h"
#include "portal.h"
#include "rocksdb/cache.h"
#include "rocksdb/filter_policy.h"

/*
 * Class:     org_rocksdb_PlainTableConfig
 * Method:    newTableFactoryHandle
 * Signature: (IIDIIBZZ)J
 */
jlong Java_org_rocksdb_PlainTableConfig_newTableFactoryHandle(
    JNIEnv * /*env*/, jobject /*jobj*/, jint jkey_size,
    jint jbloom_bits_per_key, jdouble jhash_table_ratio, jint jindex_sparseness,
    jint jhuge_page_tlb_size, jbyte jencoding_type, jboolean jfull_scan_mode,
    jboolean jstore_index_in_file) {
  rocksdb::PlainTableOptions options = rocksdb::PlainTableOptions();
  options.user_key_len = jkey_size;
  options.bloom_bits_per_key = jbloom_bits_per_key;
  options.hash_table_ratio = jhash_table_ratio;
  options.index_sparseness = jindex_sparseness;
  options.huge_page_tlb_size = jhuge_page_tlb_size;
  options.encoding_type = static_cast<rocksdb::EncodingType>(jencoding_type);
  options.full_scan_mode = jfull_scan_mode;
  options.store_index_in_file = jstore_index_in_file;
  return reinterpret_cast<jlong>(rocksdb::NewPlainTableFactory(options));
}

/*
 * Class:     org_rocksdb_BlockBasedTableConfig
 * Method:    newTableFactoryHandle
 * Signature: (ZZZZBBDBZJJJJIIIJZZJZZIIZZJIJI)J
 */
jlong Java_org_rocksdb_BlockBasedTableConfig_newTableFactoryHandle(
    JNIEnv*, jobject, jboolean jcache_index_and_filter_blocks,
    jboolean jcache_index_and_filter_blocks_with_high_priority,
    jboolean jpin_l0_filter_and_index_blocks_in_cache,
    jboolean jpin_top_level_index_and_filter, jbyte jindex_type_value,
    jbyte jdata_block_index_type_value,
    jdouble jdata_block_hash_table_util_ratio, jbyte jchecksum_type_value,
    jboolean jno_block_cache, jlong jblock_cache_handle,
    jlong jpersistent_cache_handle,
    jlong jblock_cache_compressed_handle, jlong jblock_size,
    jint jblock_size_deviation, jint jblock_restart_interval,
    jint jindex_block_restart_interval, jlong jmetadata_block_size,
    jboolean jpartition_filters, jboolean juse_delta_encoding,
    jlong jfilter_policy_handle, jboolean jwhole_key_filtering,
    jboolean jverify_compression, jint jread_amp_bytes_per_bit,
    jint jformat_version, jboolean jenable_index_compression,
    jboolean jblock_align, jlong jblock_cache_size,
    jint jblock_cache_num_shard_bits, jlong jblock_cache_compressed_size,
    jint jblock_cache_compressed_num_shard_bits) {
  rocksdb::BlockBasedTableOptions options;
  options.cache_index_and_filter_blocks =
      static_cast<bool>(jcache_index_and_filter_blocks);
  options.cache_index_and_filter_blocks_with_high_priority =
      static_cast<bool>(jcache_index_and_filter_blocks_with_high_priority);
  options.pin_l0_filter_and_index_blocks_in_cache =
    static_cast<bool>(jpin_l0_filter_and_index_blocks_in_cache);
  options.pin_top_level_index_and_filter =
    static_cast<bool>(jpin_top_level_index_and_filter);
  options.index_type =
      rocksdb::IndexTypeJni::toCppIndexType(jindex_type_value);
  options.data_block_index_type =
      rocksdb::DataBlockIndexTypeJni::toCppDataBlockIndexType(
          jdata_block_index_type_value);
  options.data_block_hash_table_util_ratio =
      static_cast<double>(jdata_block_hash_table_util_ratio);
  options.checksum =
      rocksdb::ChecksumTypeJni::toCppChecksumType(jchecksum_type_value);
  options.no_block_cache = static_cast<bool>(jno_block_cache);
  if (options.no_block_cache) {
    options.block_cache = nullptr;
  } else {
    if (jblock_cache_handle > 0) {
      std::shared_ptr<rocksdb::Cache> *pCache =
          reinterpret_cast<std::shared_ptr<rocksdb::Cache> *>(jblock_cache_handle);
      options.block_cache = *pCache;
    } else if (jblock_cache_size > 0) {
      if (jblock_cache_num_shard_bits > 0) {
        options.block_cache = rocksdb::NewLRUCache(
            static_cast<size_t>(jblock_cache_size),
            static_cast<int>(jblock_cache_num_shard_bits));
      } else {
        options.block_cache = rocksdb::NewLRUCache(
            static_cast<size_t>(jblock_cache_size));
      }
    }
  }
  if (jpersistent_cache_handle > 0) {
    std::shared_ptr<rocksdb::PersistentCache> *pCache =
            reinterpret_cast<std::shared_ptr<rocksdb::PersistentCache> *>(jpersistent_cache_handle);
        options.persistent_cache = *pCache;
  }
  if (jblock_cache_compressed_handle > 0) {
    std::shared_ptr<rocksdb::Cache> *pCache =
            reinterpret_cast<std::shared_ptr<rocksdb::Cache> *>(jblock_cache_compressed_handle);
        options.block_cache_compressed = *pCache;
  } else if (jblock_cache_compressed_size > 0) {
    if (jblock_cache_compressed_num_shard_bits > 0) {
      options.block_cache_compressed = rocksdb::NewLRUCache(
          static_cast<size_t>(jblock_cache_compressed_size),
          static_cast<int>(jblock_cache_compressed_num_shard_bits));
    } else {
      options.block_cache_compressed = rocksdb::NewLRUCache(
          static_cast<size_t>(jblock_cache_compressed_size));
    }
  }
  options.block_size = static_cast<size_t>(jblock_size);
  options.block_size_deviation = static_cast<int>(jblock_size_deviation);
  options.block_restart_interval = static_cast<int>(jblock_restart_interval);
  options.index_block_restart_interval = static_cast<int>(jindex_block_restart_interval);
  options.metadata_block_size = static_cast<uint64_t>(jmetadata_block_size);
  options.partition_filters = static_cast<bool>(jpartition_filters);
  options.use_delta_encoding = static_cast<bool>(juse_delta_encoding);
  if (jfilter_policy_handle > 0) {
    std::shared_ptr<rocksdb::FilterPolicy> *pFilterPolicy =
        reinterpret_cast<std::shared_ptr<rocksdb::FilterPolicy> *>(
            jfilter_policy_handle);
    options.filter_policy = *pFilterPolicy;
  }
  options.whole_key_filtering = static_cast<bool>(jwhole_key_filtering);
  options.verify_compression = static_cast<bool>(jverify_compression);
  options.read_amp_bytes_per_bit = static_cast<uint32_t>(jread_amp_bytes_per_bit);
  options.format_version = static_cast<uint32_t>(jformat_version);
  options.enable_index_compression = static_cast<bool>(jenable_index_compression);
  options.block_align = static_cast<bool>(jblock_align);

  return reinterpret_cast<jlong>(rocksdb::NewBlockBasedTableFactory(options));
}

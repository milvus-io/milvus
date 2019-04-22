// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

package org.rocksdb;

/**
 * The logical mapping of tickers defined in rocksdb::Tickers.
 *
 * Java byte value mappings don't align 1:1 to the c++ values. c++ rocksdb::Tickers enumeration type
 * is uint32_t and java org.rocksdb.TickerType is byte, this causes mapping issues when
 * rocksdb::Tickers value is greater then 127 (0x7F) for jbyte jni interface as range greater is not
 * available. Without breaking interface in minor versions, value mappings for
 * org.rocksdb.TickerType leverage full byte range [-128 (-0x80), (0x7F)]. Newer tickers added
 * should descend into negative values until TICKER_ENUM_MAX reaches -128 (-0x80).
 */
public enum TickerType {

    /**
     * total block cache misses
     *
     * REQUIRES: BLOCK_CACHE_MISS == BLOCK_CACHE_INDEX_MISS +
     *     BLOCK_CACHE_FILTER_MISS +
     *     BLOCK_CACHE_DATA_MISS;
     */
    BLOCK_CACHE_MISS((byte) 0x0),

    /**
     * total block cache hit
     *
     * REQUIRES: BLOCK_CACHE_HIT == BLOCK_CACHE_INDEX_HIT +
     *     BLOCK_CACHE_FILTER_HIT +
     *     BLOCK_CACHE_DATA_HIT;
     */
    BLOCK_CACHE_HIT((byte) 0x1),

    BLOCK_CACHE_ADD((byte) 0x2),

    /**
     * # of failures when adding blocks to block cache.
     */
    BLOCK_CACHE_ADD_FAILURES((byte) 0x3),

    /**
     * # of times cache miss when accessing index block from block cache.
     */
    BLOCK_CACHE_INDEX_MISS((byte) 0x4),

    /**
     * # of times cache hit when accessing index block from block cache.
     */
    BLOCK_CACHE_INDEX_HIT((byte) 0x5),

    /**
     * # of index blocks added to block cache.
     */
    BLOCK_CACHE_INDEX_ADD((byte) 0x6),

    /**
     * # of bytes of index blocks inserted into cache
     */
    BLOCK_CACHE_INDEX_BYTES_INSERT((byte) 0x7),

    /**
     * # of bytes of index block erased from cache
     */
    BLOCK_CACHE_INDEX_BYTES_EVICT((byte) 0x8),

    /**
     * # of times cache miss when accessing filter block from block cache.
     */
    BLOCK_CACHE_FILTER_MISS((byte) 0x9),

    /**
     * # of times cache hit when accessing filter block from block cache.
     */
    BLOCK_CACHE_FILTER_HIT((byte) 0xA),

    /**
     * # of filter blocks added to block cache.
     */
    BLOCK_CACHE_FILTER_ADD((byte) 0xB),

    /**
     * # of bytes of bloom filter blocks inserted into cache
     */
    BLOCK_CACHE_FILTER_BYTES_INSERT((byte) 0xC),

    /**
     * # of bytes of bloom filter block erased from cache
     */
    BLOCK_CACHE_FILTER_BYTES_EVICT((byte) 0xD),

    /**
     * # of times cache miss when accessing data block from block cache.
     */
    BLOCK_CACHE_DATA_MISS((byte) 0xE),

    /**
     * # of times cache hit when accessing data block from block cache.
     */
    BLOCK_CACHE_DATA_HIT((byte) 0xF),

    /**
     * # of data blocks added to block cache.
     */
    BLOCK_CACHE_DATA_ADD((byte) 0x10),

    /**
     * # of bytes of data blocks inserted into cache
     */
    BLOCK_CACHE_DATA_BYTES_INSERT((byte) 0x11),

    /**
     * # of bytes read from cache.
     */
    BLOCK_CACHE_BYTES_READ((byte) 0x12),

    /**
     * # of bytes written into cache.
     */
    BLOCK_CACHE_BYTES_WRITE((byte) 0x13),

    /**
     * # of times bloom filter has avoided file reads.
     */
    BLOOM_FILTER_USEFUL((byte) 0x14),

    /**
     * # persistent cache hit
     */
    PERSISTENT_CACHE_HIT((byte) 0x15),

    /**
     * # persistent cache miss
     */
    PERSISTENT_CACHE_MISS((byte) 0x16),

    /**
     * # total simulation block cache hits
     */
    SIM_BLOCK_CACHE_HIT((byte) 0x17),

    /**
     * # total simulation block cache misses
     */
    SIM_BLOCK_CACHE_MISS((byte) 0x18),

    /**
     * # of memtable hits.
     */
    MEMTABLE_HIT((byte) 0x19),

    /**
     * # of memtable misses.
     */
    MEMTABLE_MISS((byte) 0x1A),

    /**
     * # of Get() queries served by L0
     */
    GET_HIT_L0((byte) 0x1B),

    /**
     * # of Get() queries served by L1
     */
    GET_HIT_L1((byte) 0x1C),

    /**
     * # of Get() queries served by L2 and up
     */
    GET_HIT_L2_AND_UP((byte) 0x1D),

    /**
     * COMPACTION_KEY_DROP_* count the reasons for key drop during compaction
     * There are 4 reasons currently.
     */

    /**
     * key was written with a newer value.
     */
    COMPACTION_KEY_DROP_NEWER_ENTRY((byte) 0x1E),

    /**
     * Also includes keys dropped for range del.
     * The key is obsolete.
     */
    COMPACTION_KEY_DROP_OBSOLETE((byte) 0x1F),

    /**
     * key was covered by a range tombstone.
     */
    COMPACTION_KEY_DROP_RANGE_DEL((byte) 0x20),

    /**
     * User compaction function has dropped the key.
     */
    COMPACTION_KEY_DROP_USER((byte) 0x21),

    /**
     * all keys in range were deleted.
     */
    COMPACTION_RANGE_DEL_DROP_OBSOLETE((byte) 0x22),

    /**
     * Number of keys written to the database via the Put and Write call's.
     */
    NUMBER_KEYS_WRITTEN((byte) 0x23),

    /**
     * Number of Keys read.
     */
    NUMBER_KEYS_READ((byte) 0x24),

    /**
     * Number keys updated, if inplace update is enabled
     */
    NUMBER_KEYS_UPDATED((byte) 0x25),

    /**
     * The number of uncompressed bytes issued by DB::Put(), DB::Delete(),\
     * DB::Merge(), and DB::Write().
     */
    BYTES_WRITTEN((byte) 0x26),

    /**
     * The number of uncompressed bytes read from DB::Get().  It could be
     * either from memtables, cache, or table files.
     *
     * For the number of logical bytes read from DB::MultiGet(),
     * please use {@link #NUMBER_MULTIGET_BYTES_READ}.
     */
    BYTES_READ((byte) 0x27),

    /**
     * The number of calls to seek.
     */
    NUMBER_DB_SEEK((byte) 0x28),

    /**
     * The number of calls to next.
     */
    NUMBER_DB_NEXT((byte) 0x29),

    /**
     * The number of calls to prev.
     */
    NUMBER_DB_PREV((byte) 0x2A),

    /**
     * The number of calls to seek that returned data.
     */
    NUMBER_DB_SEEK_FOUND((byte) 0x2B),

    /**
     * The number of calls to next that returned data.
     */
    NUMBER_DB_NEXT_FOUND((byte) 0x2C),

    /**
     * The number of calls to prev that returned data.
     */
    NUMBER_DB_PREV_FOUND((byte) 0x2D),

    /**
     * The number of uncompressed bytes read from an iterator.
     * Includes size of key and value.
     */
    ITER_BYTES_READ((byte) 0x2E),

    NO_FILE_CLOSES((byte) 0x2F),

    NO_FILE_OPENS((byte) 0x30),

    NO_FILE_ERRORS((byte) 0x31),

    /**
     * Time system had to wait to do LO-L1 compactions.
     *
     * @deprecated
     */
    @Deprecated
    STALL_L0_SLOWDOWN_MICROS((byte) 0x32),

    /**
     * Time system had to wait to move memtable to L1.
     *
     * @deprecated
     */
    @Deprecated
    STALL_MEMTABLE_COMPACTION_MICROS((byte) 0x33),

    /**
     * write throttle because of too many files in L0.
     *
     * @deprecated
     */
    @Deprecated
    STALL_L0_NUM_FILES_MICROS((byte) 0x34),

    /**
     * Writer has to wait for compaction or flush to finish.
     */
    STALL_MICROS((byte) 0x35),

    /**
     * The wait time for db mutex.
     *
     * Disabled by default. To enable it set stats level to {@link StatsLevel#ALL}
     */
    DB_MUTEX_WAIT_MICROS((byte) 0x36),

    RATE_LIMIT_DELAY_MILLIS((byte) 0x37),

    /**
     * Number of iterators created.
     *
     */
    NO_ITERATORS((byte) 0x38),

    /**
     * Number of MultiGet calls.
     */
    NUMBER_MULTIGET_CALLS((byte) 0x39),

    /**
     * Number of MultiGet keys read.
     */
    NUMBER_MULTIGET_KEYS_READ((byte) 0x3A),

    /**
     * Number of MultiGet bytes read.
     */
    NUMBER_MULTIGET_BYTES_READ((byte) 0x3B),

    /**
     * Number of deletes records that were not required to be
     * written to storage because key does not exist.
     */
    NUMBER_FILTERED_DELETES((byte) 0x3C),
    NUMBER_MERGE_FAILURES((byte) 0x3D),

    /**
     * Number of times bloom was checked before creating iterator on a
     * file, and the number of times the check was useful in avoiding
     * iterator creation (and thus likely IOPs).
     */
    BLOOM_FILTER_PREFIX_CHECKED((byte) 0x3E),
    BLOOM_FILTER_PREFIX_USEFUL((byte) 0x3F),

    /**
     * Number of times we had to reseek inside an iteration to skip
     * over large number of keys with same userkey.
     */
    NUMBER_OF_RESEEKS_IN_ITERATION((byte) 0x40),

    /**
     * Record the number of calls to {@link RocksDB#getUpdatesSince(long)}. Useful to keep track of
     * transaction log iterator refreshes.
     */
    GET_UPDATES_SINCE_CALLS((byte) 0x41),

    /**
     * Miss in the compressed block cache.
     */
    BLOCK_CACHE_COMPRESSED_MISS((byte) 0x42),

    /**
     * Hit in the compressed block cache.
     */
    BLOCK_CACHE_COMPRESSED_HIT((byte) 0x43),

    /**
     * Number of blocks added to compressed block cache.
     */
    BLOCK_CACHE_COMPRESSED_ADD((byte) 0x44),

    /**
     * Number of failures when adding blocks to compressed block cache.
     */
    BLOCK_CACHE_COMPRESSED_ADD_FAILURES((byte) 0x45),

    /**
     * Number of times WAL sync is done.
     */
    WAL_FILE_SYNCED((byte) 0x46),

    /**
     * Number of bytes written to WAL.
     */
    WAL_FILE_BYTES((byte) 0x47),

    /**
     * Writes can be processed by requesting thread or by the thread at the
     * head of the writers queue.
     */
    WRITE_DONE_BY_SELF((byte) 0x48),

    /**
     * Equivalent to writes done for others.
     */
    WRITE_DONE_BY_OTHER((byte) 0x49),

    /**
     * Number of writes ending up with timed-out.
     */
    WRITE_TIMEDOUT((byte) 0x4A),

    /**
     * Number of Write calls that request WAL.
     */
    WRITE_WITH_WAL((byte) 0x4B),

    /**
     * Bytes read during compaction.
     */
    COMPACT_READ_BYTES((byte) 0x4C),

    /**
     * Bytes written during compaction.
     */
    COMPACT_WRITE_BYTES((byte) 0x4D),

    /**
     * Bytes written during flush.
     */
    FLUSH_WRITE_BYTES((byte) 0x4E),

    /**
     * Number of table's properties loaded directly from file, without creating
     * table reader object.
     */
    NUMBER_DIRECT_LOAD_TABLE_PROPERTIES((byte) 0x4F),
    NUMBER_SUPERVERSION_ACQUIRES((byte) 0x50),
    NUMBER_SUPERVERSION_RELEASES((byte) 0x51),
    NUMBER_SUPERVERSION_CLEANUPS((byte) 0x52),

    /**
     * # of compressions/decompressions executed
     */
    NUMBER_BLOCK_COMPRESSED((byte) 0x53),
    NUMBER_BLOCK_DECOMPRESSED((byte) 0x54),

    NUMBER_BLOCK_NOT_COMPRESSED((byte) 0x55),
    MERGE_OPERATION_TOTAL_TIME((byte) 0x56),
    FILTER_OPERATION_TOTAL_TIME((byte) 0x57),

    /**
     * Row cache.
     */
    ROW_CACHE_HIT((byte) 0x58),
    ROW_CACHE_MISS((byte) 0x59),

    /**
     * Read amplification statistics.
     *
     * Read amplification can be calculated using this formula
     * (READ_AMP_TOTAL_READ_BYTES / READ_AMP_ESTIMATE_USEFUL_BYTES)
     *
     * REQUIRES: ReadOptions::read_amp_bytes_per_bit to be enabled
     */

    /**
     * Estimate of total bytes actually used.
     */
    READ_AMP_ESTIMATE_USEFUL_BYTES((byte) 0x5A),

    /**
     * Total size of loaded data blocks.
     */
    READ_AMP_TOTAL_READ_BYTES((byte) 0x5B),

    /**
     * Number of refill intervals where rate limiter's bytes are fully consumed.
     */
    NUMBER_RATE_LIMITER_DRAINS((byte) 0x5C),

    /**
     * Number of internal skipped during iteration
     */
    NUMBER_ITER_SKIP((byte) 0x5D),

    /**
     * Number of MultiGet keys found (vs number requested)
     */
    NUMBER_MULTIGET_KEYS_FOUND((byte) 0x5E),

    // -0x01 to fixate the new value that incorrectly changed TICKER_ENUM_MAX
    /**
     * Number of iterators created.
     */
    NO_ITERATOR_CREATED((byte) -0x01),

    /**
     * Number of iterators deleted.
     */
    NO_ITERATOR_DELETED((byte) 0x60),

    /**
     * Deletions obsoleted before bottom level due to file gap optimization.
     */
    COMPACTION_OPTIMIZED_DEL_DROP_OBSOLETE((byte) 0x61),

    /**
     * If a compaction was cancelled in sfm to prevent ENOSPC
     */
    COMPACTION_CANCELLED((byte) 0x62),

    /**
     * # of times bloom FullFilter has not avoided the reads.
     */
    BLOOM_FILTER_FULL_POSITIVE((byte) 0x63),

    /**
     * # of times bloom FullFilter has not avoided the reads and data actually
     * exist.
     */
    BLOOM_FILTER_FULL_TRUE_POSITIVE((byte) 0x64),

    /**
     * BlobDB specific stats
     * # of Put/PutTTL/PutUntil to BlobDB.
     */
    BLOB_DB_NUM_PUT((byte) 0x65),

    /**
     * # of Write to BlobDB.
     */
    BLOB_DB_NUM_WRITE((byte) 0x66),

    /**
     * # of Get to BlobDB.
     */
    BLOB_DB_NUM_GET((byte) 0x67),

    /**
     * # of MultiGet to BlobDB.
     */
    BLOB_DB_NUM_MULTIGET((byte) 0x68),

    /**
     * # of Seek/SeekToFirst/SeekToLast/SeekForPrev to BlobDB iterator.
     */
    BLOB_DB_NUM_SEEK((byte) 0x69),

    /**
     * # of Next to BlobDB iterator.
     */
    BLOB_DB_NUM_NEXT((byte) 0x6A),

    /**
     * # of Prev to BlobDB iterator.
     */
    BLOB_DB_NUM_PREV((byte) 0x6B),

    /**
     * # of keys written to BlobDB.
     */
    BLOB_DB_NUM_KEYS_WRITTEN((byte) 0x6C),

    /**
     * # of keys read from BlobDB.
     */
    BLOB_DB_NUM_KEYS_READ((byte) 0x6D),

    /**
     * # of bytes (key + value) written to BlobDB.
     */
    BLOB_DB_BYTES_WRITTEN((byte) 0x6E),

    /**
     * # of bytes (keys + value) read from BlobDB.
     */
    BLOB_DB_BYTES_READ((byte) 0x6F),

    /**
     * # of keys written by BlobDB as non-TTL inlined value.
     */
    BLOB_DB_WRITE_INLINED((byte) 0x70),

    /**
     * # of keys written by BlobDB as TTL inlined value.
     */
    BLOB_DB_WRITE_INLINED_TTL((byte) 0x71),

    /**
     * # of keys written by BlobDB as non-TTL blob value.
     */
    BLOB_DB_WRITE_BLOB((byte) 0x72),

    /**
     * # of keys written by BlobDB as TTL blob value.
     */
    BLOB_DB_WRITE_BLOB_TTL((byte) 0x73),

    /**
     * # of bytes written to blob file.
     */
    BLOB_DB_BLOB_FILE_BYTES_WRITTEN((byte) 0x74),

    /**
     * # of bytes read from blob file.
     */
    BLOB_DB_BLOB_FILE_BYTES_READ((byte) 0x75),

    /**
     * # of times a blob files being synced.
     */
    BLOB_DB_BLOB_FILE_SYNCED((byte) 0x76),

    /**
     * # of blob index evicted from base DB by BlobDB compaction filter because
     * of expiration.
     */
    BLOB_DB_BLOB_INDEX_EXPIRED_COUNT((byte) 0x77),

    /**
     * Size of blob index evicted from base DB by BlobDB compaction filter
     * because of expiration.
     */
    BLOB_DB_BLOB_INDEX_EXPIRED_SIZE((byte) 0x78),

    /**
     * # of blob index evicted from base DB by BlobDB compaction filter because
     * of corresponding file deleted.
     */
    BLOB_DB_BLOB_INDEX_EVICTED_COUNT((byte) 0x79),

    /**
     * Size of blob index evicted from base DB by BlobDB compaction filter
     * because of corresponding file deleted.
     */
    BLOB_DB_BLOB_INDEX_EVICTED_SIZE((byte) 0x7A),

    /**
     * # of blob files being garbage collected.
     */
    BLOB_DB_GC_NUM_FILES((byte) 0x7B),

    /**
     * # of blob files generated by garbage collection.
     */
    BLOB_DB_GC_NUM_NEW_FILES((byte) 0x7C),

    /**
     * # of BlobDB garbage collection failures.
     */
    BLOB_DB_GC_FAILURES((byte) 0x7D),

    /**
     * # of keys drop by BlobDB garbage collection because they had been
     * overwritten.
     */
    BLOB_DB_GC_NUM_KEYS_OVERWRITTEN((byte) 0x7E),

    /**
     * # of keys drop by BlobDB garbage collection because of expiration.
     */
    BLOB_DB_GC_NUM_KEYS_EXPIRED((byte) 0x7F),

    /**
     * # of keys relocated to new blob file by garbage collection.
     */
    BLOB_DB_GC_NUM_KEYS_RELOCATED((byte) -0x02),

    /**
     * # of bytes drop by BlobDB garbage collection because they had been
     * overwritten.
     */
    BLOB_DB_GC_BYTES_OVERWRITTEN((byte) -0x03),

    /**
     * # of bytes drop by BlobDB garbage collection because of expiration.
     */
    BLOB_DB_GC_BYTES_EXPIRED((byte) -0x04),

    /**
     * # of bytes relocated to new blob file by garbage collection.
     */
    BLOB_DB_GC_BYTES_RELOCATED((byte) -0x05),

    /**
     * # of blob files evicted because of BlobDB is full.
     */
    BLOB_DB_FIFO_NUM_FILES_EVICTED((byte) -0x06),

    /**
     * # of keys in the blob files evicted because of BlobDB is full.
     */
    BLOB_DB_FIFO_NUM_KEYS_EVICTED((byte) -0x07),

    /**
     * # of bytes in the blob files evicted because of BlobDB is full.
     */
    BLOB_DB_FIFO_BYTES_EVICTED((byte) -0x08),

    /**
     * These counters indicate a performance issue in WritePrepared transactions.
     * We should not seem them ticking them much.
     * # of times prepare_mutex_ is acquired in the fast path.
     */
    TXN_PREPARE_MUTEX_OVERHEAD((byte) -0x09),

    /**
     * # of times old_commit_map_mutex_ is acquired in the fast path.
     */
    TXN_OLD_COMMIT_MAP_MUTEX_OVERHEAD((byte) -0x0A),

    /**
     * # of times we checked a batch for duplicate keys.
     */
    TXN_DUPLICATE_KEY_OVERHEAD((byte) -0x0B),

    /**
     * # of times snapshot_mutex_ is acquired in the fast path.
     */
    TXN_SNAPSHOT_MUTEX_OVERHEAD((byte) -0x0C),

    TICKER_ENUM_MAX((byte) 0x5F);

    private final byte value;

    TickerType(final byte value) {
        this.value = value;
    }

    /**
     * @deprecated Exposes internal value of native enum mappings.
     *     This method will be marked package private in the next major release.
     *
     * @return the internal representation
     */
    @Deprecated
    public byte getValue() {
        return value;
    }
}

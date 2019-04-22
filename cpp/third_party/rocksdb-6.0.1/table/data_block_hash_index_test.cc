// Copyright (c) 2011-present, Facebook, Inc. All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include <cstdlib>
#include <string>
#include <unordered_map>

#include "db/table_properties_collector.h"
#include "rocksdb/slice.h"
#include "table/block.h"
#include "table/block_based_table_reader.h"
#include "table/block_builder.h"
#include "table/data_block_hash_index.h"
#include "table/get_context.h"
#include "table/table_builder.h"
#include "util/testharness.h"
#include "util/testutil.h"

namespace rocksdb {

bool SearchForOffset(DataBlockHashIndex& index, const char* data,
                     uint16_t map_offset, const Slice& key,
                     uint8_t& restart_point) {
  uint8_t entry = index.Lookup(data, map_offset, key);
  if (entry == kCollision) {
    return true;
  }

  if (entry == kNoEntry) {
    return false;
  }

  return entry == restart_point;
}

// Random KV generator similer to block_test
static std::string RandomString(Random* rnd, int len) {
  std::string r;
  test::RandomString(rnd, len, &r);
  return r;
}
std::string GenerateKey(int primary_key, int secondary_key, int padding_size,
                        Random* rnd) {
  char buf[50];
  char* p = &buf[0];
  snprintf(buf, sizeof(buf), "%6d%4d", primary_key, secondary_key);
  std::string k(p);
  if (padding_size) {
    k += RandomString(rnd, padding_size);
  }

  return k;
}

// Generate random key value pairs.
// The generated key will be sorted. You can tune the parameters to generated
// different kinds of test key/value pairs for different scenario.
void GenerateRandomKVs(std::vector<std::string>* keys,
                       std::vector<std::string>* values, const int from,
                       const int len, const int step = 1,
                       const int padding_size = 0,
                       const int keys_share_prefix = 1) {
  Random rnd(302);

  // generate different prefix
  for (int i = from; i < from + len; i += step) {
    // generating keys that shares the prefix
    for (int j = 0; j < keys_share_prefix; ++j) {
      keys->emplace_back(GenerateKey(i, j, padding_size, &rnd));

      // 100 bytes values
      values->emplace_back(RandomString(&rnd, 100));
    }
  }
}

TEST(DataBlockHashIndex, DataBlockHashTestSmall) {
  DataBlockHashIndexBuilder builder;
  builder.Initialize(0.75 /*util_ratio*/);
  for (int j = 0; j < 5; j++) {
    for (uint8_t i = 0; i < 2 + j; i++) {
      std::string key("key" + std::to_string(i));
      uint8_t restart_point = i;
      builder.Add(key, restart_point);
    }

    size_t estimated_size = builder.EstimateSize();

    std::string buffer("fake"), buffer2;
    size_t original_size = buffer.size();
    estimated_size += original_size;
    builder.Finish(buffer);

    ASSERT_EQ(buffer.size(), estimated_size);

    buffer2 = buffer;  // test for the correctness of relative offset

    Slice s(buffer2);
    DataBlockHashIndex index;
    uint16_t map_offset;
    index.Initialize(s.data(), static_cast<uint16_t>(s.size()), &map_offset);

    // the additional hash map should start at the end of the buffer
    ASSERT_EQ(original_size, map_offset);
    for (uint8_t i = 0; i < 2; i++) {
      std::string key("key" + std::to_string(i));
      uint8_t restart_point = i;
      ASSERT_TRUE(
          SearchForOffset(index, s.data(), map_offset, key, restart_point));
    }
    builder.Reset();
  }
}

TEST(DataBlockHashIndex, DataBlockHashTest) {
  // bucket_num = 200, #keys = 100. 50% utilization
  DataBlockHashIndexBuilder builder;
  builder.Initialize(0.75 /*util_ratio*/);

  for (uint8_t i = 0; i < 100; i++) {
    std::string key("key" + std::to_string(i));
    uint8_t restart_point = i;
    builder.Add(key, restart_point);
  }

  size_t estimated_size = builder.EstimateSize();

  std::string buffer("fake content"), buffer2;
  size_t original_size = buffer.size();
  estimated_size += original_size;
  builder.Finish(buffer);

  ASSERT_EQ(buffer.size(), estimated_size);

  buffer2 = buffer; // test for the correctness of relative offset

  Slice s(buffer2);
  DataBlockHashIndex index;
  uint16_t map_offset;
  index.Initialize(s.data(), static_cast<uint16_t>(s.size()), &map_offset);

  // the additional hash map should start at the end of the buffer
  ASSERT_EQ(original_size, map_offset);
  for (uint8_t i = 0; i < 100; i++) {
    std::string key("key" + std::to_string(i));
    uint8_t restart_point = i;
    ASSERT_TRUE(
        SearchForOffset(index, s.data(), map_offset, key, restart_point));
  }
}

TEST(DataBlockHashIndex, DataBlockHashTestCollision) {
  // bucket_num = 2. There will be intense hash collisions
  DataBlockHashIndexBuilder builder;
  builder.Initialize(0.75 /*util_ratio*/);

  for (uint8_t i = 0; i < 100; i++) {
    std::string key("key" + std::to_string(i));
    uint8_t restart_point = i;
    builder.Add(key, restart_point);
  }

  size_t estimated_size = builder.EstimateSize();

  std::string buffer("some other fake content to take up space"), buffer2;
  size_t original_size = buffer.size();
  estimated_size += original_size;
  builder.Finish(buffer);

  ASSERT_EQ(buffer.size(), estimated_size);

  buffer2 = buffer; // test for the correctness of relative offset

  Slice s(buffer2);
  DataBlockHashIndex index;
  uint16_t map_offset;
  index.Initialize(s.data(), static_cast<uint16_t>(s.size()), &map_offset);

  // the additional hash map should start at the end of the buffer
  ASSERT_EQ(original_size, map_offset);
  for (uint8_t i = 0; i < 100; i++) {
    std::string key("key" + std::to_string(i));
    uint8_t restart_point = i;
    ASSERT_TRUE(
        SearchForOffset(index, s.data(), map_offset, key, restart_point));
  }
}

TEST(DataBlockHashIndex, DataBlockHashTestLarge) {
  DataBlockHashIndexBuilder builder;
  builder.Initialize(0.75 /*util_ratio*/);
  std::unordered_map<std::string, uint8_t> m;

  for (uint8_t i = 0; i < 100; i++) {
    if (i % 2) {
      continue;  // leave half of the keys out
    }
    std::string key = "key" + std::to_string(i);
    uint8_t restart_point = i;
    builder.Add(key, restart_point);
    m[key] = restart_point;
  }

  size_t estimated_size = builder.EstimateSize();

  std::string buffer("filling stuff"), buffer2;
  size_t original_size = buffer.size();
  estimated_size += original_size;
  builder.Finish(buffer);

  ASSERT_EQ(buffer.size(), estimated_size);

  buffer2 = buffer; // test for the correctness of relative offset

  Slice s(buffer2);
  DataBlockHashIndex index;
  uint16_t map_offset;
  index.Initialize(s.data(), static_cast<uint16_t>(s.size()), &map_offset);

  // the additional hash map should start at the end of the buffer
  ASSERT_EQ(original_size, map_offset);
  for (uint8_t i = 0; i < 100; i++) {
    std::string key = "key" + std::to_string(i);
    uint8_t restart_point = i;
    if (m.count(key)) {
      ASSERT_TRUE(m[key] == restart_point);
      ASSERT_TRUE(
          SearchForOffset(index, s.data(), map_offset, key, restart_point));
    } else {
      // we allow false positve, so don't test the nonexisting keys.
      // when false positive happens, the search will continue to the
      // restart intervals to see if the key really exist.
    }
  }
}

TEST(DataBlockHashIndex, RestartIndexExceedMax) {
  DataBlockHashIndexBuilder builder;
  builder.Initialize(0.75 /*util_ratio*/);
  std::unordered_map<std::string, uint8_t> m;

  for (uint8_t i = 0; i <= 253; i++) {
    std::string key = "key" + std::to_string(i);
    uint8_t restart_point = i;
    builder.Add(key, restart_point);
  }
  ASSERT_TRUE(builder.Valid());

  builder.Reset();

  for (uint8_t i = 0; i <= 254; i++) {
    std::string key = "key" + std::to_string(i);
    uint8_t restart_point = i;
    builder.Add(key, restart_point);
  }

  ASSERT_FALSE(builder.Valid());

  builder.Reset();
  ASSERT_TRUE(builder.Valid());
}

TEST(DataBlockHashIndex, BlockRestartIndexExceedMax) {
  Options options = Options();

  BlockBuilder builder(1 /* block_restart_interval */,
                       true /* use_delta_encoding */,
                       false /* use_value_delta_encoding */,
                       BlockBasedTableOptions::kDataBlockBinaryAndHash);

  // #restarts <= 253. HashIndex is valid
  for (int i = 0; i <= 253; i++) {
    std::string ukey = "key" + std::to_string(i);
    InternalKey ikey(ukey, 0, kTypeValue);
    builder.Add(ikey.Encode().ToString(), "value");
  }

  {
    // read serialized contents of the block
    Slice rawblock = builder.Finish();

    // create block reader
    BlockContents contents;
    contents.data = rawblock;
    Block reader(std::move(contents), kDisableGlobalSequenceNumber);

    ASSERT_EQ(reader.IndexType(),
              BlockBasedTableOptions::kDataBlockBinaryAndHash);
  }

  builder.Reset();

  // #restarts > 253. HashIndex is not used
  for (int i = 0; i <= 254; i++) {
    std::string ukey = "key" + std::to_string(i);
    InternalKey ikey(ukey, 0, kTypeValue);
    builder.Add(ikey.Encode().ToString(), "value");
  }

  {
    // read serialized contents of the block
    Slice rawblock = builder.Finish();

    // create block reader
    BlockContents contents;
    contents.data = rawblock;
    Block reader(std::move(contents), kDisableGlobalSequenceNumber);

    ASSERT_EQ(reader.IndexType(),
              BlockBasedTableOptions::kDataBlockBinarySearch);
  }
}

TEST(DataBlockHashIndex, BlockSizeExceedMax) {
  Options options = Options();
  std::string ukey(10, 'k');
  InternalKey ikey(ukey, 0, kTypeValue);

  BlockBuilder builder(1 /* block_restart_interval */,
                       false /* use_delta_encoding */,
                       false /* use_value_delta_encoding */,
                       BlockBasedTableOptions::kDataBlockBinaryAndHash);

  {
    // insert a large value. The block size plus HashIndex is 65536.
    std::string value(65502, 'v');

    builder.Add(ikey.Encode().ToString(), value);

    // read serialized contents of the block
    Slice rawblock = builder.Finish();
    ASSERT_LE(rawblock.size(), kMaxBlockSizeSupportedByHashIndex);
    std::cerr << "block size: " << rawblock.size() << std::endl;

    // create block reader
    BlockContents contents;
    contents.data = rawblock;
    Block reader(std::move(contents), kDisableGlobalSequenceNumber);

    ASSERT_EQ(reader.IndexType(),
              BlockBasedTableOptions::kDataBlockBinaryAndHash);
  }

  builder.Reset();

  {
    // insert a large value. The block size plus HashIndex would be 65537.
    // This excceed the max block size supported by HashIndex (65536).
    // So when build finishes HashIndex will not be created for the block.
    std::string value(65503, 'v');

    builder.Add(ikey.Encode().ToString(), value);

    // read serialized contents of the block
    Slice rawblock = builder.Finish();
    ASSERT_LE(rawblock.size(), kMaxBlockSizeSupportedByHashIndex);
    std::cerr << "block size: " << rawblock.size() << std::endl;

    // create block reader
    BlockContents contents;
    contents.data = rawblock;
    Block reader(std::move(contents), kDisableGlobalSequenceNumber);

    // the index type have fallen back to binary when build finish.
    ASSERT_EQ(reader.IndexType(),
              BlockBasedTableOptions::kDataBlockBinarySearch);
  }
}

TEST(DataBlockHashIndex, BlockTestSingleKey) {
  Options options = Options();

  BlockBuilder builder(16 /* block_restart_interval */,
                       true /* use_delta_encoding */,
                       false /* use_value_delta_encoding */,
                       BlockBasedTableOptions::kDataBlockBinaryAndHash);

  std::string ukey("gopher");
  std::string value("gold");
  InternalKey ikey(ukey, 10, kTypeValue);
  builder.Add(ikey.Encode().ToString(), value /*value*/);

  // read serialized contents of the block
  Slice rawblock = builder.Finish();

  // create block reader
  BlockContents contents;
  contents.data = rawblock;
  Block reader(std::move(contents), kDisableGlobalSequenceNumber);

  const InternalKeyComparator icmp(BytewiseComparator());
  auto iter = reader.NewIterator<DataBlockIter>(&icmp, icmp.user_comparator());
  bool may_exist;
  // search in block for the key just inserted
  {
    InternalKey seek_ikey(ukey, 10, kValueTypeForSeek);
    may_exist = iter->SeekForGet(seek_ikey.Encode().ToString());
    ASSERT_TRUE(may_exist);
    ASSERT_TRUE(iter->Valid());
    ASSERT_EQ(
        options.comparator->Compare(iter->key(), ikey.Encode().ToString()), 0);
    ASSERT_EQ(iter->value(), value);
  }

  // search in block for the existing ukey, but with higher seqno
  {
    InternalKey seek_ikey(ukey, 20, kValueTypeForSeek);

    // HashIndex should be able to set the iter correctly
    may_exist = iter->SeekForGet(seek_ikey.Encode().ToString());
    ASSERT_TRUE(may_exist);
    ASSERT_TRUE(iter->Valid());

    // user key should match
    ASSERT_EQ(options.comparator->Compare(ExtractUserKey(iter->key()), ukey),
              0);

    // seek_key seqno number should be greater than that of iter result
    ASSERT_GT(GetInternalKeySeqno(seek_ikey.Encode()),
              GetInternalKeySeqno(iter->key()));

    ASSERT_EQ(iter->value(), value);
  }

  // Search in block for the existing ukey, but with lower seqno
  // in this case, hash can find the only occurrence of the user_key, but
  // ParseNextDataKey() will skip it as it does not have a older seqno.
  // In this case, GetForSeek() is effective to locate the user_key, and
  // iter->Valid() == false indicates that we've reached to the end of
  // the block and the caller should continue searching the next block.
  {
    InternalKey seek_ikey(ukey, 5, kValueTypeForSeek);
    may_exist = iter->SeekForGet(seek_ikey.Encode().ToString());
    ASSERT_TRUE(may_exist);
    ASSERT_FALSE(iter->Valid());  // should have reached to the end of block
  }

  delete iter;
}

TEST(DataBlockHashIndex, BlockTestLarge) {
  Random rnd(1019);
  Options options = Options();
  std::vector<std::string> keys;
  std::vector<std::string> values;

  BlockBuilder builder(16 /* block_restart_interval */,
                       true /* use_delta_encoding */,
                       false /* use_value_delta_encoding */,
                       BlockBasedTableOptions::kDataBlockBinaryAndHash);
  int num_records = 500;

  GenerateRandomKVs(&keys, &values, 0, num_records);

  // Generate keys. Adding a trailing "1" to indicate existent keys.
  // Later will Seeking for keys with a trailing "0" to test seeking
  // non-existent keys.
  for (int i = 0; i < num_records; i++) {
    std::string ukey(keys[i] + "1" /* existing key marker */);
    InternalKey ikey(ukey, 0, kTypeValue);
    builder.Add(ikey.Encode().ToString(), values[i]);
  }

  // read serialized contents of the block
  Slice rawblock = builder.Finish();

  // create block reader
  BlockContents contents;
  contents.data = rawblock;
  Block reader(std::move(contents), kDisableGlobalSequenceNumber);
  const InternalKeyComparator icmp(BytewiseComparator());

  // random seek existent keys
  for (int i = 0; i < num_records; i++) {
    auto iter =
        reader.NewIterator<DataBlockIter>(&icmp, icmp.user_comparator());
    // find a random key in the lookaside array
    int index = rnd.Uniform(num_records);
    std::string ukey(keys[index] + "1" /* existing key marker */);
    InternalKey ikey(ukey, 0, kTypeValue);

    // search in block for this key
    bool may_exist = iter->SeekForGet(ikey.Encode().ToString());
    ASSERT_TRUE(may_exist);
    ASSERT_TRUE(iter->Valid());
    ASSERT_EQ(values[index], iter->value());

    delete iter;
  }

  // random seek non-existent user keys
  // In this case A), the user_key cannot be found in HashIndex. The key may
  // exist in the next block. So the iter is set invalidated to tell the
  // caller to search the next block. This test case belongs to this case A).
  //
  // Note that for non-existent keys, there is possibility of false positive,
  // i.e. the key is still hashed into some restart interval.
  // Two additional possible outcome:
  // B) linear seek the restart interval and not found, the iter stops at the
  //    starting of the next restart interval. The key does not exist
  //    anywhere.
  // C) linear seek the restart interval and not found, the iter stops at the
  //    the end of the block, i.e. restarts_. The key may exist in the next
  //    block.
  // So these combinations are possible when searching non-existent user_key:
  //
  // case#    may_exist  iter->Valid()
  //     A         true          false
  //     B        false           true
  //     C         true          false

  for (int i = 0; i < num_records; i++) {
    auto iter =
        reader.NewIterator<DataBlockIter>(&icmp, icmp.user_comparator());
    // find a random key in the lookaside array
    int index = rnd.Uniform(num_records);
    std::string ukey(keys[index] + "0" /* non-existing key marker */);
    InternalKey ikey(ukey, 0, kTypeValue);

    // search in block for this key
    bool may_exist = iter->SeekForGet(ikey.Encode().ToString());
    if (!may_exist) {
      ASSERT_TRUE(iter->Valid());
    }
    if (!iter->Valid()) {
      ASSERT_TRUE(may_exist);
    }

    delete iter;
  }
}

// helper routine for DataBlockHashIndex.BlockBoundary
void TestBoundary(InternalKey& ik1, std::string& v1, InternalKey& ik2,
                  std::string& v2, InternalKey& seek_ikey,
                  GetContext& get_context, Options& options) {
  std::unique_ptr<WritableFileWriter> file_writer;
  std::unique_ptr<RandomAccessFileReader> file_reader;
  std::unique_ptr<TableReader> table_reader;
  int level_ = -1;

  std::vector<std::string> keys;
  const ImmutableCFOptions ioptions(options);
  const MutableCFOptions moptions(options);
  const InternalKeyComparator internal_comparator(options.comparator);

  EnvOptions soptions;

  soptions.use_mmap_reads = ioptions.allow_mmap_reads;
  file_writer.reset(
      test::GetWritableFileWriter(new test::StringSink(), "" /* don't care */));
  std::unique_ptr<TableBuilder> builder;
  std::vector<std::unique_ptr<IntTblPropCollectorFactory>>
      int_tbl_prop_collector_factories;
  std::string column_family_name;
  builder.reset(ioptions.table_factory->NewTableBuilder(
      TableBuilderOptions(ioptions, moptions, internal_comparator,
                          &int_tbl_prop_collector_factories,
                          options.compression, CompressionOptions(),
                          false /* skip_filters */, column_family_name, level_),
      TablePropertiesCollectorFactory::Context::kUnknownColumnFamily,
      file_writer.get()));

  builder->Add(ik1.Encode().ToString(), v1);
  builder->Add(ik2.Encode().ToString(), v2);
  EXPECT_TRUE(builder->status().ok());

  Status s = builder->Finish();
  file_writer->Flush();
  EXPECT_TRUE(s.ok()) << s.ToString();

  EXPECT_EQ(static_cast<test::StringSink*>(file_writer->writable_file())
                ->contents()
                .size(),
            builder->FileSize());

  // Open the table
  file_reader.reset(test::GetRandomAccessFileReader(new test::StringSource(
      static_cast<test::StringSink*>(file_writer->writable_file())->contents(),
      0 /*uniq_id*/, ioptions.allow_mmap_reads)));
  const bool kSkipFilters = true;
  const bool kImmortal = true;
  ioptions.table_factory->NewTableReader(
      TableReaderOptions(ioptions, moptions.prefix_extractor.get(), soptions,
                         internal_comparator, !kSkipFilters, !kImmortal,
                         level_),
      std::move(file_reader),
      static_cast<test::StringSink*>(file_writer->writable_file())
          ->contents()
          .size(),
      &table_reader);
  // Search using Get()
  ReadOptions ro;

  ASSERT_OK(table_reader->Get(ro, seek_ikey.Encode().ToString(), &get_context,
                              moptions.prefix_extractor.get()));
}

TEST(DataBlockHashIndex, BlockBoundary) {
  BlockBasedTableOptions table_options;
  table_options.data_block_index_type =
      BlockBasedTableOptions::kDataBlockBinaryAndHash;
  table_options.block_restart_interval = 1;
  table_options.block_size = 4096;

  Options options;
  options.comparator = BytewiseComparator();

  options.table_factory.reset(NewBlockBasedTableFactory(table_options));

  // insert two large k/v pair. Given that the block_size is 4096, one k/v
  // pair will take up one block.
  // [    k1/v1   ][    k2/v2  ]
  // [   Block N  ][ Block N+1 ]

  {
    // [ "aab"@100 ][ "axy"@10  ]
    // | Block  N  ][ Block N+1 ]
    // seek for "axy"@60
    std::string uk1("aab");
    InternalKey ik1(uk1, 100, kTypeValue);
    std::string v1(4100, '1');  // large value

    std::string uk2("axy");
    InternalKey ik2(uk2, 10, kTypeValue);
    std::string v2(4100, '2');  // large value

    PinnableSlice value;
    std::string seek_ukey("axy");
    InternalKey seek_ikey(seek_ukey, 60, kTypeValue);
    GetContext get_context(options.comparator, nullptr, nullptr, nullptr,
                           GetContext::kNotFound, seek_ukey, &value, nullptr,
                           nullptr, nullptr, nullptr);

    TestBoundary(ik1, v1, ik2, v2, seek_ikey, get_context, options);
    ASSERT_EQ(get_context.State(), GetContext::kFound);
    ASSERT_EQ(value, v2);
    value.Reset();
  }

  {
    // [ "axy"@100 ][ "axy"@10  ]
    // | Block  N  ][ Block N+1 ]
    // seek for "axy"@60
    std::string uk1("axy");
    InternalKey ik1(uk1, 100, kTypeValue);
    std::string v1(4100, '1');  // large value

    std::string uk2("axy");
    InternalKey ik2(uk2, 10, kTypeValue);
    std::string v2(4100, '2');  // large value

    PinnableSlice value;
    std::string seek_ukey("axy");
    InternalKey seek_ikey(seek_ukey, 60, kTypeValue);
    GetContext get_context(options.comparator, nullptr, nullptr, nullptr,
                           GetContext::kNotFound, seek_ukey, &value, nullptr,
                           nullptr, nullptr, nullptr);

    TestBoundary(ik1, v1, ik2, v2, seek_ikey, get_context, options);
    ASSERT_EQ(get_context.State(), GetContext::kFound);
    ASSERT_EQ(value, v2);
    value.Reset();
  }

  {
    // [ "axy"@100 ][ "axy"@10  ]
    // | Block  N  ][ Block N+1 ]
    // seek for "axy"@120
    std::string uk1("axy");
    InternalKey ik1(uk1, 100, kTypeValue);
    std::string v1(4100, '1');  // large value

    std::string uk2("axy");
    InternalKey ik2(uk2, 10, kTypeValue);
    std::string v2(4100, '2');  // large value

    PinnableSlice value;
    std::string seek_ukey("axy");
    InternalKey seek_ikey(seek_ukey, 120, kTypeValue);
    GetContext get_context(options.comparator, nullptr, nullptr, nullptr,
                           GetContext::kNotFound, seek_ukey, &value, nullptr,
                           nullptr, nullptr, nullptr);

    TestBoundary(ik1, v1, ik2, v2, seek_ikey, get_context, options);
    ASSERT_EQ(get_context.State(), GetContext::kFound);
    ASSERT_EQ(value, v1);
    value.Reset();
  }

  {
    // [ "axy"@100 ][ "axy"@10  ]
    // | Block  N  ][ Block N+1 ]
    // seek for "axy"@5
    std::string uk1("axy");
    InternalKey ik1(uk1, 100, kTypeValue);
    std::string v1(4100, '1');  // large value

    std::string uk2("axy");
    InternalKey ik2(uk2, 10, kTypeValue);
    std::string v2(4100, '2');  // large value

    PinnableSlice value;
    std::string seek_ukey("axy");
    InternalKey seek_ikey(seek_ukey, 5, kTypeValue);
    GetContext get_context(options.comparator, nullptr, nullptr, nullptr,
                           GetContext::kNotFound, seek_ukey, &value, nullptr,
                           nullptr, nullptr, nullptr);

    TestBoundary(ik1, v1, ik2, v2, seek_ikey, get_context, options);
    ASSERT_EQ(get_context.State(), GetContext::kNotFound);
    value.Reset();
  }
}

}  // namespace rocksdb

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}

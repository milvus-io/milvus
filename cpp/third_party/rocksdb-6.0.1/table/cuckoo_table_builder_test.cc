// Copyright (c) 2011-present, Facebook, Inc. All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#ifndef ROCKSDB_LITE

#include <vector>
#include <string>
#include <map>
#include <utility>

#include "table/meta_blocks.h"
#include "table/cuckoo_table_builder.h"
#include "util/file_reader_writer.h"
#include "util/testharness.h"
#include "util/testutil.h"

namespace rocksdb {
extern const uint64_t kCuckooTableMagicNumber;

namespace {
std::unordered_map<std::string, std::vector<uint64_t>> hash_map;

uint64_t GetSliceHash(const Slice& s, uint32_t index,
                      uint64_t /*max_num_buckets*/) {
  return hash_map[s.ToString()][index];
}
}  // namespace

class CuckooBuilderTest : public testing::Test {
 public:
  CuckooBuilderTest() {
    env_ = Env::Default();
    Options options;
    options.allow_mmap_reads = true;
    env_options_ = EnvOptions(options);
  }

  void CheckFileContents(const std::vector<std::string>& keys,
      const std::vector<std::string>& values,
      const std::vector<uint64_t>& expected_locations,
      std::string expected_unused_bucket, uint64_t expected_table_size,
      uint32_t expected_num_hash_func, bool expected_is_last_level,
      uint32_t expected_cuckoo_block_size = 1) {
    uint64_t num_deletions = 0;
    for (const auto& key : keys) {
      ParsedInternalKey parsed;
      if (ParseInternalKey(key, &parsed) && parsed.type == kTypeDeletion) {
        num_deletions++;
      }
    }
    // Read file
    std::unique_ptr<RandomAccessFile> read_file;
    ASSERT_OK(env_->NewRandomAccessFile(fname, &read_file, env_options_));
    uint64_t read_file_size;
    ASSERT_OK(env_->GetFileSize(fname, &read_file_size));

   // @lint-ignore TXT2 T25377293 Grandfathered in
	  Options options;
	  options.allow_mmap_reads = true;
	  ImmutableCFOptions ioptions(options);

    // Assert Table Properties.
    TableProperties* props = nullptr;
    std::unique_ptr<RandomAccessFileReader> file_reader(
        new RandomAccessFileReader(std::move(read_file), fname));
    ASSERT_OK(ReadTableProperties(file_reader.get(), read_file_size,
                                  kCuckooTableMagicNumber, ioptions,
                                  &props, true /* compression_type_missing */));
    // Check unused bucket.
    std::string unused_key = props->user_collected_properties[
      CuckooTablePropertyNames::kEmptyKey];
    ASSERT_EQ(expected_unused_bucket.substr(0,
          props->fixed_key_len), unused_key);

    uint64_t value_len_found =
      *reinterpret_cast<const uint64_t*>(props->user_collected_properties[
                CuckooTablePropertyNames::kValueLength].data());
    ASSERT_EQ(values.empty() ? 0 : values[0].size(), value_len_found);
    ASSERT_EQ(props->raw_value_size, values.size()*value_len_found);
    const uint64_t table_size =
      *reinterpret_cast<const uint64_t*>(props->user_collected_properties[
                CuckooTablePropertyNames::kHashTableSize].data());
    ASSERT_EQ(expected_table_size, table_size);
    const uint32_t num_hash_func_found =
      *reinterpret_cast<const uint32_t*>(props->user_collected_properties[
                CuckooTablePropertyNames::kNumHashFunc].data());
    ASSERT_EQ(expected_num_hash_func, num_hash_func_found);
    const uint32_t cuckoo_block_size =
      *reinterpret_cast<const uint32_t*>(props->user_collected_properties[
                CuckooTablePropertyNames::kCuckooBlockSize].data());
    ASSERT_EQ(expected_cuckoo_block_size, cuckoo_block_size);
    const bool is_last_level_found =
      *reinterpret_cast<const bool*>(props->user_collected_properties[
                CuckooTablePropertyNames::kIsLastLevel].data());
    ASSERT_EQ(expected_is_last_level, is_last_level_found);

    ASSERT_EQ(props->num_entries, keys.size());
    ASSERT_EQ(props->num_deletions, num_deletions);
    ASSERT_EQ(props->fixed_key_len, keys.empty() ? 0 : keys[0].size());
    ASSERT_EQ(props->data_size, expected_unused_bucket.size() *
        (expected_table_size + expected_cuckoo_block_size - 1));
    ASSERT_EQ(props->raw_key_size, keys.size()*props->fixed_key_len);
    ASSERT_EQ(props->column_family_id, 0);
    ASSERT_EQ(props->column_family_name, kDefaultColumnFamilyName);
    delete props;

    // Check contents of the bucket.
    std::vector<bool> keys_found(keys.size(), false);
    size_t bucket_size = expected_unused_bucket.size();
    for (uint32_t i = 0; i < table_size + cuckoo_block_size - 1; ++i) {
      Slice read_slice;
      ASSERT_OK(file_reader->Read(i * bucket_size, bucket_size, &read_slice,
                                  nullptr));
      size_t key_idx =
          std::find(expected_locations.begin(), expected_locations.end(), i) -
          expected_locations.begin();
      if (key_idx == keys.size()) {
        // i is not one of the expected locations. Empty bucket.
        if (read_slice.data() == nullptr) {
          ASSERT_EQ(0, expected_unused_bucket.size());
        } else {
          ASSERT_EQ(read_slice.compare(expected_unused_bucket), 0);
        }
      } else {
        keys_found[key_idx] = true;
        ASSERT_EQ(read_slice.compare(keys[key_idx] + values[key_idx]), 0);
      }
    }
    for (auto key_found : keys_found) {
      // Check that all keys wereReader found.
      ASSERT_TRUE(key_found);
    }
  }

  std::string GetInternalKey(Slice user_key, bool zero_seqno,
                             ValueType type = kTypeValue) {
    IterKey ikey;
    ikey.SetInternalKey(user_key, zero_seqno ? 0 : 1000, type);
    return ikey.GetInternalKey().ToString();
  }

  uint64_t NextPowOf2(uint64_t num) {
    uint64_t n = 2;
    while (n <= num) {
      n *= 2;
    }
    return n;
  }

  uint64_t GetExpectedTableSize(uint64_t num) {
    return NextPowOf2(static_cast<uint64_t>(num / kHashTableRatio));
  }


  Env* env_;
  EnvOptions env_options_;
  std::string fname;
  const double kHashTableRatio = 0.9;
};

TEST_F(CuckooBuilderTest, SuccessWithEmptyFile) {
  std::unique_ptr<WritableFile> writable_file;
  fname = test::PerThreadDBPath("EmptyFile");
  ASSERT_OK(env_->NewWritableFile(fname, &writable_file, env_options_));
  std::unique_ptr<WritableFileWriter> file_writer(
      new WritableFileWriter(std::move(writable_file), fname, EnvOptions()));
  CuckooTableBuilder builder(file_writer.get(), kHashTableRatio, 4, 100,
                             BytewiseComparator(), 1, false, false,
                             GetSliceHash, 0 /* column_family_id */,
                             kDefaultColumnFamilyName);
  ASSERT_OK(builder.status());
  ASSERT_EQ(0UL, builder.FileSize());
  ASSERT_OK(builder.Finish());
  ASSERT_OK(file_writer->Close());
  CheckFileContents({}, {}, {}, "", 2, 2, false);
}

TEST_F(CuckooBuilderTest, WriteSuccessNoCollisionFullKey) {
  for (auto type : {kTypeValue, kTypeDeletion}) {
    uint32_t num_hash_fun = 4;
    std::vector<std::string> user_keys = {"key01", "key02", "key03", "key04"};
    std::vector<std::string> values;
    if (type == kTypeValue) {
      values = {"v01", "v02", "v03", "v04"};
    } else {
      values = {"", "", "", ""};
    }
    // Need to have a temporary variable here as VS compiler does not currently
    // support operator= with initializer_list as a parameter
    std::unordered_map<std::string, std::vector<uint64_t>> hm = {
        {user_keys[0], {0, 1, 2, 3}},
        {user_keys[1], {1, 2, 3, 4}},
        {user_keys[2], {2, 3, 4, 5}},
        {user_keys[3], {3, 4, 5, 6}}};
    hash_map = std::move(hm);

    std::vector<uint64_t> expected_locations = {0, 1, 2, 3};
    std::vector<std::string> keys;
    for (auto& user_key : user_keys) {
      keys.push_back(GetInternalKey(user_key, false, type));
    }
    uint64_t expected_table_size = GetExpectedTableSize(keys.size());

    std::unique_ptr<WritableFile> writable_file;
    fname = test::PerThreadDBPath("NoCollisionFullKey");
    ASSERT_OK(env_->NewWritableFile(fname, &writable_file, env_options_));
    std::unique_ptr<WritableFileWriter> file_writer(
        new WritableFileWriter(std::move(writable_file), fname, EnvOptions()));
    CuckooTableBuilder builder(file_writer.get(), kHashTableRatio, num_hash_fun,
                               100, BytewiseComparator(), 1, false, false,
                               GetSliceHash, 0 /* column_family_id */,
                               kDefaultColumnFamilyName);
    ASSERT_OK(builder.status());
    for (uint32_t i = 0; i < user_keys.size(); i++) {
      builder.Add(Slice(keys[i]), Slice(values[i]));
      ASSERT_EQ(builder.NumEntries(), i + 1);
      ASSERT_OK(builder.status());
    }
    size_t bucket_size = keys[0].size() + values[0].size();
    ASSERT_EQ(expected_table_size * bucket_size - 1, builder.FileSize());
    ASSERT_OK(builder.Finish());
    ASSERT_OK(file_writer->Close());
    ASSERT_LE(expected_table_size * bucket_size, builder.FileSize());

    std::string expected_unused_bucket = GetInternalKey("key00", true);
    expected_unused_bucket += std::string(values[0].size(), 'a');
    CheckFileContents(keys, values, expected_locations, expected_unused_bucket,
                      expected_table_size, 2, false);
  }
}

TEST_F(CuckooBuilderTest, WriteSuccessWithCollisionFullKey) {
  uint32_t num_hash_fun = 4;
  std::vector<std::string> user_keys = {"key01", "key02", "key03", "key04"};
  std::vector<std::string> values = {"v01", "v02", "v03", "v04"};
  // Need to have a temporary variable here as VS compiler does not currently
  // support operator= with initializer_list as a parameter
  std::unordered_map<std::string, std::vector<uint64_t>> hm = {
      {user_keys[0], {0, 1, 2, 3}},
      {user_keys[1], {0, 1, 2, 3}},
      {user_keys[2], {0, 1, 2, 3}},
      {user_keys[3], {0, 1, 2, 3}},
  };
  hash_map = std::move(hm);

  std::vector<uint64_t> expected_locations = {0, 1, 2, 3};
  std::vector<std::string> keys;
  for (auto& user_key : user_keys) {
    keys.push_back(GetInternalKey(user_key, false));
  }
  uint64_t expected_table_size = GetExpectedTableSize(keys.size());

  std::unique_ptr<WritableFile> writable_file;
  fname = test::PerThreadDBPath("WithCollisionFullKey");
  ASSERT_OK(env_->NewWritableFile(fname, &writable_file, env_options_));
  std::unique_ptr<WritableFileWriter> file_writer(
      new WritableFileWriter(std::move(writable_file), fname, EnvOptions()));
  CuckooTableBuilder builder(file_writer.get(), kHashTableRatio, num_hash_fun,
                             100, BytewiseComparator(), 1, false, false,
                             GetSliceHash, 0 /* column_family_id */,
                             kDefaultColumnFamilyName);
  ASSERT_OK(builder.status());
  for (uint32_t i = 0; i < user_keys.size(); i++) {
    builder.Add(Slice(keys[i]), Slice(values[i]));
    ASSERT_EQ(builder.NumEntries(), i + 1);
    ASSERT_OK(builder.status());
  }
  size_t bucket_size = keys[0].size() + values[0].size();
  ASSERT_EQ(expected_table_size * bucket_size - 1, builder.FileSize());
  ASSERT_OK(builder.Finish());
  ASSERT_OK(file_writer->Close());
  ASSERT_LE(expected_table_size * bucket_size, builder.FileSize());

  std::string expected_unused_bucket = GetInternalKey("key00", true);
  expected_unused_bucket += std::string(values[0].size(), 'a');
  CheckFileContents(keys, values, expected_locations,
      expected_unused_bucket, expected_table_size, 4, false);
}

TEST_F(CuckooBuilderTest, WriteSuccessWithCollisionAndCuckooBlock) {
  uint32_t num_hash_fun = 4;
  std::vector<std::string> user_keys = {"key01", "key02", "key03", "key04"};
  std::vector<std::string> values = {"v01", "v02", "v03", "v04"};
  // Need to have a temporary variable here as VS compiler does not currently
  // support operator= with initializer_list as a parameter
  std::unordered_map<std::string, std::vector<uint64_t>> hm = {
      {user_keys[0], {0, 1, 2, 3}},
      {user_keys[1], {0, 1, 2, 3}},
      {user_keys[2], {0, 1, 2, 3}},
      {user_keys[3], {0, 1, 2, 3}},
  };
  hash_map = std::move(hm);

  std::vector<uint64_t> expected_locations = {0, 1, 2, 3};
  std::vector<std::string> keys;
  for (auto& user_key : user_keys) {
    keys.push_back(GetInternalKey(user_key, false));
  }
  uint64_t expected_table_size = GetExpectedTableSize(keys.size());

  std::unique_ptr<WritableFile> writable_file;
  uint32_t cuckoo_block_size = 2;
  fname = test::PerThreadDBPath("WithCollisionFullKey2");
  ASSERT_OK(env_->NewWritableFile(fname, &writable_file, env_options_));
  std::unique_ptr<WritableFileWriter> file_writer(
      new WritableFileWriter(std::move(writable_file), fname, EnvOptions()));
  CuckooTableBuilder builder(
      file_writer.get(), kHashTableRatio, num_hash_fun, 100,
      BytewiseComparator(), cuckoo_block_size, false, false, GetSliceHash,
      0 /* column_family_id */, kDefaultColumnFamilyName);
  ASSERT_OK(builder.status());
  for (uint32_t i = 0; i < user_keys.size(); i++) {
    builder.Add(Slice(keys[i]), Slice(values[i]));
    ASSERT_EQ(builder.NumEntries(), i + 1);
    ASSERT_OK(builder.status());
  }
  size_t bucket_size = keys[0].size() + values[0].size();
  ASSERT_EQ(expected_table_size * bucket_size - 1, builder.FileSize());
  ASSERT_OK(builder.Finish());
  ASSERT_OK(file_writer->Close());
  ASSERT_LE(expected_table_size * bucket_size, builder.FileSize());

  std::string expected_unused_bucket = GetInternalKey("key00", true);
  expected_unused_bucket += std::string(values[0].size(), 'a');
  CheckFileContents(keys, values, expected_locations,
      expected_unused_bucket, expected_table_size, 3, false, cuckoo_block_size);
}

TEST_F(CuckooBuilderTest, WithCollisionPathFullKey) {
  // Have two hash functions. Insert elements with overlapping hashes.
  // Finally insert an element with hash value somewhere in the middle
  // so that it displaces all the elements after that.
  uint32_t num_hash_fun = 2;
  std::vector<std::string> user_keys = {"key01", "key02", "key03",
    "key04", "key05"};
  std::vector<std::string> values = {"v01", "v02", "v03", "v04", "v05"};
  // Need to have a temporary variable here as VS compiler does not currently
  // support operator= with initializer_list as a parameter
  std::unordered_map<std::string, std::vector<uint64_t>> hm = {
      {user_keys[0], {0, 1}},
      {user_keys[1], {1, 2}},
      {user_keys[2], {2, 3}},
      {user_keys[3], {3, 4}},
      {user_keys[4], {0, 2}},
  };
  hash_map = std::move(hm);

  std::vector<uint64_t> expected_locations = {0, 1, 3, 4, 2};
  std::vector<std::string> keys;
  for (auto& user_key : user_keys) {
    keys.push_back(GetInternalKey(user_key, false));
  }
  uint64_t expected_table_size = GetExpectedTableSize(keys.size());

  std::unique_ptr<WritableFile> writable_file;
  fname = test::PerThreadDBPath("WithCollisionPathFullKey");
  ASSERT_OK(env_->NewWritableFile(fname, &writable_file, env_options_));
  std::unique_ptr<WritableFileWriter> file_writer(
      new WritableFileWriter(std::move(writable_file), fname, EnvOptions()));
  CuckooTableBuilder builder(file_writer.get(), kHashTableRatio, num_hash_fun,
                             100, BytewiseComparator(), 1, false, false,
                             GetSliceHash, 0 /* column_family_id */,
                             kDefaultColumnFamilyName);
  ASSERT_OK(builder.status());
  for (uint32_t i = 0; i < user_keys.size(); i++) {
    builder.Add(Slice(keys[i]), Slice(values[i]));
    ASSERT_EQ(builder.NumEntries(), i + 1);
    ASSERT_OK(builder.status());
  }
  size_t bucket_size = keys[0].size() + values[0].size();
  ASSERT_EQ(expected_table_size * bucket_size - 1, builder.FileSize());
  ASSERT_OK(builder.Finish());
  ASSERT_OK(file_writer->Close());
  ASSERT_LE(expected_table_size * bucket_size, builder.FileSize());

  std::string expected_unused_bucket = GetInternalKey("key00", true);
  expected_unused_bucket += std::string(values[0].size(), 'a');
  CheckFileContents(keys, values, expected_locations,
      expected_unused_bucket, expected_table_size, 2, false);
}

TEST_F(CuckooBuilderTest, WithCollisionPathFullKeyAndCuckooBlock) {
  uint32_t num_hash_fun = 2;
  std::vector<std::string> user_keys = {"key01", "key02", "key03",
    "key04", "key05"};
  std::vector<std::string> values = {"v01", "v02", "v03", "v04", "v05"};
  // Need to have a temporary variable here as VS compiler does not currently
  // support operator= with initializer_list as a parameter
  std::unordered_map<std::string, std::vector<uint64_t>> hm = {
      {user_keys[0], {0, 1}},
      {user_keys[1], {1, 2}},
      {user_keys[2], {3, 4}},
      {user_keys[3], {4, 5}},
      {user_keys[4], {0, 3}},
  };
  hash_map = std::move(hm);

  std::vector<uint64_t> expected_locations = {2, 1, 3, 4, 0};
  std::vector<std::string> keys;
  for (auto& user_key : user_keys) {
    keys.push_back(GetInternalKey(user_key, false));
  }
  uint64_t expected_table_size = GetExpectedTableSize(keys.size());

  std::unique_ptr<WritableFile> writable_file;
  fname = test::PerThreadDBPath("WithCollisionPathFullKeyAndCuckooBlock");
  ASSERT_OK(env_->NewWritableFile(fname, &writable_file, env_options_));
  std::unique_ptr<WritableFileWriter> file_writer(
      new WritableFileWriter(std::move(writable_file), fname, EnvOptions()));
  CuckooTableBuilder builder(file_writer.get(), kHashTableRatio, num_hash_fun,
                             100, BytewiseComparator(), 2, false, false,
                             GetSliceHash, 0 /* column_family_id */,
                             kDefaultColumnFamilyName);
  ASSERT_OK(builder.status());
  for (uint32_t i = 0; i < user_keys.size(); i++) {
    builder.Add(Slice(keys[i]), Slice(values[i]));
    ASSERT_EQ(builder.NumEntries(), i + 1);
    ASSERT_OK(builder.status());
  }
  size_t bucket_size = keys[0].size() + values[0].size();
  ASSERT_EQ(expected_table_size * bucket_size - 1, builder.FileSize());
  ASSERT_OK(builder.Finish());
  ASSERT_OK(file_writer->Close());
  ASSERT_LE(expected_table_size * bucket_size, builder.FileSize());

  std::string expected_unused_bucket = GetInternalKey("key00", true);
  expected_unused_bucket += std::string(values[0].size(), 'a');
  CheckFileContents(keys, values, expected_locations,
      expected_unused_bucket, expected_table_size, 2, false, 2);
}

TEST_F(CuckooBuilderTest, WriteSuccessNoCollisionUserKey) {
  uint32_t num_hash_fun = 4;
  std::vector<std::string> user_keys = {"key01", "key02", "key03", "key04"};
  std::vector<std::string> values = {"v01", "v02", "v03", "v04"};
  // Need to have a temporary variable here as VS compiler does not currently
  // support operator= with initializer_list as a parameter
  std::unordered_map<std::string, std::vector<uint64_t>> hm = {
      {user_keys[0], {0, 1, 2, 3}},
      {user_keys[1], {1, 2, 3, 4}},
      {user_keys[2], {2, 3, 4, 5}},
      {user_keys[3], {3, 4, 5, 6}}};
  hash_map = std::move(hm);

  std::vector<uint64_t> expected_locations = {0, 1, 2, 3};
  uint64_t expected_table_size = GetExpectedTableSize(user_keys.size());

  std::unique_ptr<WritableFile> writable_file;
  fname = test::PerThreadDBPath("NoCollisionUserKey");
  ASSERT_OK(env_->NewWritableFile(fname, &writable_file, env_options_));
  std::unique_ptr<WritableFileWriter> file_writer(
      new WritableFileWriter(std::move(writable_file), fname, EnvOptions()));
  CuckooTableBuilder builder(file_writer.get(), kHashTableRatio, num_hash_fun,
                             100, BytewiseComparator(), 1, false, false,
                             GetSliceHash, 0 /* column_family_id */,
                             kDefaultColumnFamilyName);
  ASSERT_OK(builder.status());
  for (uint32_t i = 0; i < user_keys.size(); i++) {
    builder.Add(Slice(GetInternalKey(user_keys[i], true)), Slice(values[i]));
    ASSERT_EQ(builder.NumEntries(), i + 1);
    ASSERT_OK(builder.status());
  }
  size_t bucket_size = user_keys[0].size() + values[0].size();
  ASSERT_EQ(expected_table_size * bucket_size - 1, builder.FileSize());
  ASSERT_OK(builder.Finish());
  ASSERT_OK(file_writer->Close());
  ASSERT_LE(expected_table_size * bucket_size, builder.FileSize());

  std::string expected_unused_bucket = "key00";
  expected_unused_bucket += std::string(values[0].size(), 'a');
  CheckFileContents(user_keys, values, expected_locations,
      expected_unused_bucket, expected_table_size, 2, true);
}

TEST_F(CuckooBuilderTest, WriteSuccessWithCollisionUserKey) {
  uint32_t num_hash_fun = 4;
  std::vector<std::string> user_keys = {"key01", "key02", "key03", "key04"};
  std::vector<std::string> values = {"v01", "v02", "v03", "v04"};
  // Need to have a temporary variable here as VS compiler does not currently
  // support operator= with initializer_list as a parameter
  std::unordered_map<std::string, std::vector<uint64_t>> hm = {
      {user_keys[0], {0, 1, 2, 3}},
      {user_keys[1], {0, 1, 2, 3}},
      {user_keys[2], {0, 1, 2, 3}},
      {user_keys[3], {0, 1, 2, 3}},
  };
  hash_map = std::move(hm);

  std::vector<uint64_t> expected_locations = {0, 1, 2, 3};
  uint64_t expected_table_size = GetExpectedTableSize(user_keys.size());

  std::unique_ptr<WritableFile> writable_file;
  fname = test::PerThreadDBPath("WithCollisionUserKey");
  ASSERT_OK(env_->NewWritableFile(fname, &writable_file, env_options_));
  std::unique_ptr<WritableFileWriter> file_writer(
      new WritableFileWriter(std::move(writable_file), fname, EnvOptions()));
  CuckooTableBuilder builder(file_writer.get(), kHashTableRatio, num_hash_fun,
                             100, BytewiseComparator(), 1, false, false,
                             GetSliceHash, 0 /* column_family_id */,
                             kDefaultColumnFamilyName);
  ASSERT_OK(builder.status());
  for (uint32_t i = 0; i < user_keys.size(); i++) {
    builder.Add(Slice(GetInternalKey(user_keys[i], true)), Slice(values[i]));
    ASSERT_EQ(builder.NumEntries(), i + 1);
    ASSERT_OK(builder.status());
  }
  size_t bucket_size = user_keys[0].size() + values[0].size();
  ASSERT_EQ(expected_table_size * bucket_size - 1, builder.FileSize());
  ASSERT_OK(builder.Finish());
  ASSERT_OK(file_writer->Close());
  ASSERT_LE(expected_table_size * bucket_size, builder.FileSize());

  std::string expected_unused_bucket = "key00";
  expected_unused_bucket += std::string(values[0].size(), 'a');
  CheckFileContents(user_keys, values, expected_locations,
      expected_unused_bucket, expected_table_size, 4, true);
}

TEST_F(CuckooBuilderTest, WithCollisionPathUserKey) {
  uint32_t num_hash_fun = 2;
  std::vector<std::string> user_keys = {"key01", "key02", "key03",
    "key04", "key05"};
  std::vector<std::string> values = {"v01", "v02", "v03", "v04", "v05"};
  // Need to have a temporary variable here as VS compiler does not currently
  // support operator= with initializer_list as a parameter
  std::unordered_map<std::string, std::vector<uint64_t>> hm = {
      {user_keys[0], {0, 1}},
      {user_keys[1], {1, 2}},
      {user_keys[2], {2, 3}},
      {user_keys[3], {3, 4}},
      {user_keys[4], {0, 2}},
  };
  hash_map = std::move(hm);

  std::vector<uint64_t> expected_locations = {0, 1, 3, 4, 2};
  uint64_t expected_table_size = GetExpectedTableSize(user_keys.size());

  std::unique_ptr<WritableFile> writable_file;
  fname = test::PerThreadDBPath("WithCollisionPathUserKey");
  ASSERT_OK(env_->NewWritableFile(fname, &writable_file, env_options_));
  std::unique_ptr<WritableFileWriter> file_writer(
      new WritableFileWriter(std::move(writable_file), fname, EnvOptions()));
  CuckooTableBuilder builder(file_writer.get(), kHashTableRatio, num_hash_fun,
                             2, BytewiseComparator(), 1, false, false,
                             GetSliceHash, 0 /* column_family_id */,
                             kDefaultColumnFamilyName);
  ASSERT_OK(builder.status());
  for (uint32_t i = 0; i < user_keys.size(); i++) {
    builder.Add(Slice(GetInternalKey(user_keys[i], true)), Slice(values[i]));
    ASSERT_EQ(builder.NumEntries(), i + 1);
    ASSERT_OK(builder.status());
  }
  size_t bucket_size = user_keys[0].size() + values[0].size();
  ASSERT_EQ(expected_table_size * bucket_size - 1, builder.FileSize());
  ASSERT_OK(builder.Finish());
  ASSERT_OK(file_writer->Close());
  ASSERT_LE(expected_table_size * bucket_size, builder.FileSize());

  std::string expected_unused_bucket = "key00";
  expected_unused_bucket += std::string(values[0].size(), 'a');
  CheckFileContents(user_keys, values, expected_locations,
      expected_unused_bucket, expected_table_size, 2, true);
}

TEST_F(CuckooBuilderTest, FailWhenCollisionPathTooLong) {
  // Have two hash functions. Insert elements with overlapping hashes.
  // Finally try inserting an element with hash value somewhere in the middle
  // and it should fail because the no. of elements to displace is too high.
  uint32_t num_hash_fun = 2;
  std::vector<std::string> user_keys = {"key01", "key02", "key03",
    "key04", "key05"};
  // Need to have a temporary variable here as VS compiler does not currently
  // support operator= with initializer_list as a parameter
  std::unordered_map<std::string, std::vector<uint64_t>> hm = {
      {user_keys[0], {0, 1}},
      {user_keys[1], {1, 2}},
      {user_keys[2], {2, 3}},
      {user_keys[3], {3, 4}},
      {user_keys[4], {0, 1}},
  };
  hash_map = std::move(hm);

  std::unique_ptr<WritableFile> writable_file;
  fname = test::PerThreadDBPath("WithCollisionPathUserKey");
  ASSERT_OK(env_->NewWritableFile(fname, &writable_file, env_options_));
  std::unique_ptr<WritableFileWriter> file_writer(
      new WritableFileWriter(std::move(writable_file), fname, EnvOptions()));
  CuckooTableBuilder builder(file_writer.get(), kHashTableRatio, num_hash_fun,
                             2, BytewiseComparator(), 1, false, false,
                             GetSliceHash, 0 /* column_family_id */,
                             kDefaultColumnFamilyName);
  ASSERT_OK(builder.status());
  for (uint32_t i = 0; i < user_keys.size(); i++) {
    builder.Add(Slice(GetInternalKey(user_keys[i], false)), Slice("value"));
    ASSERT_EQ(builder.NumEntries(), i + 1);
    ASSERT_OK(builder.status());
  }
  ASSERT_TRUE(builder.Finish().IsNotSupported());
  ASSERT_OK(file_writer->Close());
}

TEST_F(CuckooBuilderTest, FailWhenSameKeyInserted) {
  // Need to have a temporary variable here as VS compiler does not currently
  // support operator= with initializer_list as a parameter
  std::unordered_map<std::string, std::vector<uint64_t>> hm = {
      {"repeatedkey", {0, 1, 2, 3}}};
  hash_map = std::move(hm);
  uint32_t num_hash_fun = 4;
  std::string user_key = "repeatedkey";

  std::unique_ptr<WritableFile> writable_file;
  fname = test::PerThreadDBPath("FailWhenSameKeyInserted");
  ASSERT_OK(env_->NewWritableFile(fname, &writable_file, env_options_));
  std::unique_ptr<WritableFileWriter> file_writer(
      new WritableFileWriter(std::move(writable_file), fname, EnvOptions()));
  CuckooTableBuilder builder(file_writer.get(), kHashTableRatio, num_hash_fun,
                             100, BytewiseComparator(), 1, false, false,
                             GetSliceHash, 0 /* column_family_id */,
                             kDefaultColumnFamilyName);
  ASSERT_OK(builder.status());

  builder.Add(Slice(GetInternalKey(user_key, false)), Slice("value1"));
  ASSERT_EQ(builder.NumEntries(), 1u);
  ASSERT_OK(builder.status());
  builder.Add(Slice(GetInternalKey(user_key, true)), Slice("value2"));
  ASSERT_EQ(builder.NumEntries(), 2u);
  ASSERT_OK(builder.status());

  ASSERT_TRUE(builder.Finish().IsNotSupported());
  ASSERT_OK(file_writer->Close());
}
}  // namespace rocksdb

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}

#else
#include <stdio.h>

int main(int /*argc*/, char** /*argv*/) {
  fprintf(stderr, "SKIPPED as Cuckoo table is not supported in ROCKSDB_LITE\n");
  return 0;
}

#endif  // ROCKSDB_LITE

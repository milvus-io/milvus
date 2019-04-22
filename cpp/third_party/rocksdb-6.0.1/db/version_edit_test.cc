//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "db/version_edit.h"
#include "util/coding.h"
#include "util/sync_point.h"
#include "util/testharness.h"

namespace rocksdb {

static void TestEncodeDecode(const VersionEdit& edit) {
  std::string encoded, encoded2;
  edit.EncodeTo(&encoded);
  VersionEdit parsed;
  Status s = parsed.DecodeFrom(encoded);
  ASSERT_TRUE(s.ok()) << s.ToString();
  parsed.EncodeTo(&encoded2);
  ASSERT_EQ(encoded, encoded2);
}

class VersionEditTest : public testing::Test {};

TEST_F(VersionEditTest, EncodeDecode) {
  static const uint64_t kBig = 1ull << 50;
  static const uint32_t kBig32Bit = 1ull << 30;

  VersionEdit edit;
  for (int i = 0; i < 4; i++) {
    TestEncodeDecode(edit);
    edit.AddFile(3, kBig + 300 + i, kBig32Bit + 400 + i, 0,
                 InternalKey("foo", kBig + 500 + i, kTypeValue),
                 InternalKey("zoo", kBig + 600 + i, kTypeDeletion),
                 kBig + 500 + i, kBig + 600 + i, false);
    edit.DeleteFile(4, kBig + 700 + i);
  }

  edit.SetComparatorName("foo");
  edit.SetLogNumber(kBig + 100);
  edit.SetNextFile(kBig + 200);
  edit.SetLastSequence(kBig + 1000);
  TestEncodeDecode(edit);
}

TEST_F(VersionEditTest, EncodeDecodeNewFile4) {
  static const uint64_t kBig = 1ull << 50;

  VersionEdit edit;
  edit.AddFile(3, 300, 3, 100, InternalKey("foo", kBig + 500, kTypeValue),
               InternalKey("zoo", kBig + 600, kTypeDeletion), kBig + 500,
               kBig + 600, true);
  edit.AddFile(4, 301, 3, 100, InternalKey("foo", kBig + 501, kTypeValue),
               InternalKey("zoo", kBig + 601, kTypeDeletion), kBig + 501,
               kBig + 601, false);
  edit.AddFile(5, 302, 0, 100, InternalKey("foo", kBig + 502, kTypeValue),
               InternalKey("zoo", kBig + 602, kTypeDeletion), kBig + 502,
               kBig + 602, true);

  edit.DeleteFile(4, 700);

  edit.SetComparatorName("foo");
  edit.SetLogNumber(kBig + 100);
  edit.SetNextFile(kBig + 200);
  edit.SetLastSequence(kBig + 1000);
  TestEncodeDecode(edit);

  std::string encoded, encoded2;
  edit.EncodeTo(&encoded);
  VersionEdit parsed;
  Status s = parsed.DecodeFrom(encoded);
  ASSERT_TRUE(s.ok()) << s.ToString();
  auto& new_files = parsed.GetNewFiles();
  ASSERT_TRUE(new_files[0].second.marked_for_compaction);
  ASSERT_TRUE(!new_files[1].second.marked_for_compaction);
  ASSERT_TRUE(new_files[2].second.marked_for_compaction);
  ASSERT_EQ(3, new_files[0].second.fd.GetPathId());
  ASSERT_EQ(3, new_files[1].second.fd.GetPathId());
  ASSERT_EQ(0, new_files[2].second.fd.GetPathId());
}

TEST_F(VersionEditTest, ForwardCompatibleNewFile4) {
  static const uint64_t kBig = 1ull << 50;
  VersionEdit edit;
  edit.AddFile(3, 300, 3, 100, InternalKey("foo", kBig + 500, kTypeValue),
               InternalKey("zoo", kBig + 600, kTypeDeletion), kBig + 500,
               kBig + 600, true);
  edit.AddFile(4, 301, 3, 100, InternalKey("foo", kBig + 501, kTypeValue),
               InternalKey("zoo", kBig + 601, kTypeDeletion), kBig + 501,
               kBig + 601, false);
  edit.DeleteFile(4, 700);

  edit.SetComparatorName("foo");
  edit.SetLogNumber(kBig + 100);
  edit.SetNextFile(kBig + 200);
  edit.SetLastSequence(kBig + 1000);

  std::string encoded;

  // Call back function to add extra customized builds.
  bool first = true;
  rocksdb::SyncPoint::GetInstance()->SetCallBack(
      "VersionEdit::EncodeTo:NewFile4:CustomizeFields", [&](void* arg) {
        std::string* str = reinterpret_cast<std::string*>(arg);
        PutVarint32(str, 33);
        const std::string str1 = "random_string";
        PutLengthPrefixedSlice(str, str1);
        if (first) {
          first = false;
          PutVarint32(str, 22);
          const std::string str2 = "s";
          PutLengthPrefixedSlice(str, str2);
        }
      });
  rocksdb::SyncPoint::GetInstance()->EnableProcessing();
  edit.EncodeTo(&encoded);
  rocksdb::SyncPoint::GetInstance()->DisableProcessing();

  VersionEdit parsed;
  Status s = parsed.DecodeFrom(encoded);
  ASSERT_TRUE(s.ok()) << s.ToString();
  ASSERT_TRUE(!first);
  auto& new_files = parsed.GetNewFiles();
  ASSERT_TRUE(new_files[0].second.marked_for_compaction);
  ASSERT_TRUE(!new_files[1].second.marked_for_compaction);
  ASSERT_EQ(3, new_files[0].second.fd.GetPathId());
  ASSERT_EQ(3, new_files[1].second.fd.GetPathId());
  ASSERT_EQ(1u, parsed.GetDeletedFiles().size());
}

TEST_F(VersionEditTest, NewFile4NotSupportedField) {
  static const uint64_t kBig = 1ull << 50;
  VersionEdit edit;
  edit.AddFile(3, 300, 3, 100, InternalKey("foo", kBig + 500, kTypeValue),
               InternalKey("zoo", kBig + 600, kTypeDeletion), kBig + 500,
               kBig + 600, true);

  edit.SetComparatorName("foo");
  edit.SetLogNumber(kBig + 100);
  edit.SetNextFile(kBig + 200);
  edit.SetLastSequence(kBig + 1000);

  std::string encoded;

  // Call back function to add extra customized builds.
  rocksdb::SyncPoint::GetInstance()->SetCallBack(
      "VersionEdit::EncodeTo:NewFile4:CustomizeFields", [&](void* arg) {
        std::string* str = reinterpret_cast<std::string*>(arg);
        const std::string str1 = "s";
        PutLengthPrefixedSlice(str, str1);
      });
  rocksdb::SyncPoint::GetInstance()->EnableProcessing();
  edit.EncodeTo(&encoded);
  rocksdb::SyncPoint::GetInstance()->DisableProcessing();

  VersionEdit parsed;
  Status s = parsed.DecodeFrom(encoded);
  ASSERT_NOK(s);
}

TEST_F(VersionEditTest, EncodeEmptyFile) {
  VersionEdit edit;
  edit.AddFile(0, 0, 0, 0, InternalKey(), InternalKey(), 0, 0, false);
  std::string buffer;
  ASSERT_TRUE(!edit.EncodeTo(&buffer));
}

TEST_F(VersionEditTest, ColumnFamilyTest) {
  VersionEdit edit;
  edit.SetColumnFamily(2);
  edit.AddColumnFamily("column_family");
  edit.SetMaxColumnFamily(5);
  TestEncodeDecode(edit);

  edit.Clear();
  edit.SetColumnFamily(3);
  edit.DropColumnFamily();
  TestEncodeDecode(edit);
}

TEST_F(VersionEditTest, MinLogNumberToKeep) {
  VersionEdit edit;
  edit.SetMinLogNumberToKeep(13);
  TestEncodeDecode(edit);

  edit.Clear();
  edit.SetMinLogNumberToKeep(23);
  TestEncodeDecode(edit);
}

TEST_F(VersionEditTest, AtomicGroupTest) {
  VersionEdit edit;
  edit.MarkAtomicGroup(1);
  TestEncodeDecode(edit);
}

TEST_F(VersionEditTest, IgnorableField) {
  VersionEdit ve;
  std::string encoded;

  // Size of ignorable field is too large
  PutVarint32Varint64(&encoded, 2 /* kLogNumber */, 66);
  // This is a customized ignorable tag
  PutVarint32Varint64(&encoded,
                      0x2710 /* A field with kTagSafeIgnoreMask set */,
                      5 /* fieldlength 5 */);
  encoded += "abc";  // Only fills 3 bytes,
  ASSERT_NOK(ve.DecodeFrom(encoded));

  encoded.clear();
  // Error when seeing unidentified tag that is not ignorable
  PutVarint32Varint64(&encoded, 2 /* kLogNumber */, 66);
  // This is a customized ignorable tag
  PutVarint32Varint64(&encoded, 666 /* A field with kTagSafeIgnoreMask unset */,
                      3 /* fieldlength 3 */);
  encoded += "abc";  //  Fill 3 bytes
  PutVarint32Varint64(&encoded, 3 /* next file number */, 88);
  ASSERT_NOK(ve.DecodeFrom(encoded));

  // Safely ignore an identified but safely ignorable entry
  encoded.clear();
  PutVarint32Varint64(&encoded, 2 /* kLogNumber */, 66);
  // This is a customized ignorable tag
  PutVarint32Varint64(&encoded,
                      0x2710 /* A field with kTagSafeIgnoreMask set */,
                      3 /* fieldlength 3 */);
  encoded += "abc";  //  Fill 3 bytes
  PutVarint32Varint64(&encoded, 3 /* kNextFileNumber */, 88);

  ASSERT_OK(ve.DecodeFrom(encoded));

  ASSERT_TRUE(ve.has_log_number());
  ASSERT_TRUE(ve.has_next_file_number());
  ASSERT_EQ(66, ve.log_number());
  ASSERT_EQ(88, ve.next_file_number());
}

}  // namespace rocksdb

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}

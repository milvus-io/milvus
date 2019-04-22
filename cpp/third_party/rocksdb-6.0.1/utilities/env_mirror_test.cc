//  Copyright (c) 2015, Red Hat, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#ifndef ROCKSDB_LITE

#include "rocksdb/utilities/env_mirror.h"
#include "env/mock_env.h"
#include "util/testharness.h"

namespace rocksdb {

class EnvMirrorTest : public testing::Test {
 public:
  Env* default_;
  MockEnv* a_, *b_;
  EnvMirror* env_;
  const EnvOptions soptions_;

  EnvMirrorTest()
      : default_(Env::Default()),
        a_(new MockEnv(default_)),
        b_(new MockEnv(default_)),
        env_(new EnvMirror(a_, b_)) {}
  ~EnvMirrorTest() {
    delete env_;
    delete a_;
    delete b_;
  }
};

TEST_F(EnvMirrorTest, Basics) {
  uint64_t file_size;
  std::unique_ptr<WritableFile> writable_file;
  std::vector<std::string> children;

  ASSERT_OK(env_->CreateDir("/dir"));

  // Check that the directory is empty.
  ASSERT_EQ(Status::NotFound(), env_->FileExists("/dir/non_existent"));
  ASSERT_TRUE(!env_->GetFileSize("/dir/non_existent", &file_size).ok());
  ASSERT_OK(env_->GetChildren("/dir", &children));
  ASSERT_EQ(0U, children.size());

  // Create a file.
  ASSERT_OK(env_->NewWritableFile("/dir/f", &writable_file, soptions_));
  writable_file.reset();

  // Check that the file exists.
  ASSERT_OK(env_->FileExists("/dir/f"));
  ASSERT_OK(a_->FileExists("/dir/f"));
  ASSERT_OK(b_->FileExists("/dir/f"));
  ASSERT_OK(env_->GetFileSize("/dir/f", &file_size));
  ASSERT_EQ(0U, file_size);
  ASSERT_OK(env_->GetChildren("/dir", &children));
  ASSERT_EQ(1U, children.size());
  ASSERT_EQ("f", children[0]);
  ASSERT_OK(a_->GetChildren("/dir", &children));
  ASSERT_EQ(1U, children.size());
  ASSERT_EQ("f", children[0]);
  ASSERT_OK(b_->GetChildren("/dir", &children));
  ASSERT_EQ(1U, children.size());
  ASSERT_EQ("f", children[0]);

  // Write to the file.
  ASSERT_OK(env_->NewWritableFile("/dir/f", &writable_file, soptions_));
  ASSERT_OK(writable_file->Append("abc"));
  writable_file.reset();

  // Check for expected size.
  ASSERT_OK(env_->GetFileSize("/dir/f", &file_size));
  ASSERT_EQ(3U, file_size);
  ASSERT_OK(a_->GetFileSize("/dir/f", &file_size));
  ASSERT_EQ(3U, file_size);
  ASSERT_OK(b_->GetFileSize("/dir/f", &file_size));
  ASSERT_EQ(3U, file_size);

  // Check that renaming works.
  ASSERT_TRUE(!env_->RenameFile("/dir/non_existent", "/dir/g").ok());
  ASSERT_OK(env_->RenameFile("/dir/f", "/dir/g"));
  ASSERT_EQ(Status::NotFound(), env_->FileExists("/dir/f"));
  ASSERT_OK(env_->FileExists("/dir/g"));
  ASSERT_OK(env_->GetFileSize("/dir/g", &file_size));
  ASSERT_EQ(3U, file_size);
  ASSERT_OK(a_->FileExists("/dir/g"));
  ASSERT_OK(a_->GetFileSize("/dir/g", &file_size));
  ASSERT_EQ(3U, file_size);
  ASSERT_OK(b_->FileExists("/dir/g"));
  ASSERT_OK(b_->GetFileSize("/dir/g", &file_size));
  ASSERT_EQ(3U, file_size);

  // Check that opening non-existent file fails.
  std::unique_ptr<SequentialFile> seq_file;
  std::unique_ptr<RandomAccessFile> rand_file;
  ASSERT_TRUE(
      !env_->NewSequentialFile("/dir/non_existent", &seq_file, soptions_).ok());
  ASSERT_TRUE(!seq_file);
  ASSERT_TRUE(!env_->NewRandomAccessFile("/dir/non_existent", &rand_file,
                                         soptions_).ok());
  ASSERT_TRUE(!rand_file);

  // Check that deleting works.
  ASSERT_TRUE(!env_->DeleteFile("/dir/non_existent").ok());
  ASSERT_OK(env_->DeleteFile("/dir/g"));
  ASSERT_EQ(Status::NotFound(), env_->FileExists("/dir/g"));
  ASSERT_OK(env_->GetChildren("/dir", &children));
  ASSERT_EQ(0U, children.size());
  ASSERT_OK(env_->DeleteDir("/dir"));
}

TEST_F(EnvMirrorTest, ReadWrite) {
  std::unique_ptr<WritableFile> writable_file;
  std::unique_ptr<SequentialFile> seq_file;
  std::unique_ptr<RandomAccessFile> rand_file;
  Slice result;
  char scratch[100];

  ASSERT_OK(env_->CreateDir("/dir"));

  ASSERT_OK(env_->NewWritableFile("/dir/f", &writable_file, soptions_));
  ASSERT_OK(writable_file->Append("hello "));
  ASSERT_OK(writable_file->Append("world"));
  writable_file.reset();

  // Read sequentially.
  ASSERT_OK(env_->NewSequentialFile("/dir/f", &seq_file, soptions_));
  ASSERT_OK(seq_file->Read(5, &result, scratch));  // Read "hello".
  ASSERT_EQ(0, result.compare("hello"));
  ASSERT_OK(seq_file->Skip(1));
  ASSERT_OK(seq_file->Read(1000, &result, scratch));  // Read "world".
  ASSERT_EQ(0, result.compare("world"));
  ASSERT_OK(seq_file->Read(1000, &result, scratch));  // Try reading past EOF.
  ASSERT_EQ(0U, result.size());
  ASSERT_OK(seq_file->Skip(100));  // Try to skip past end of file.
  ASSERT_OK(seq_file->Read(1000, &result, scratch));
  ASSERT_EQ(0U, result.size());

  // Random reads.
  ASSERT_OK(env_->NewRandomAccessFile("/dir/f", &rand_file, soptions_));
  ASSERT_OK(rand_file->Read(6, 5, &result, scratch));  // Read "world".
  ASSERT_EQ(0, result.compare("world"));
  ASSERT_OK(rand_file->Read(0, 5, &result, scratch));  // Read "hello".
  ASSERT_EQ(0, result.compare("hello"));
  ASSERT_OK(rand_file->Read(10, 100, &result, scratch));  // Read "d".
  ASSERT_EQ(0, result.compare("d"));

  // Too high offset.
  ASSERT_TRUE(!rand_file->Read(1000, 5, &result, scratch).ok());
}

TEST_F(EnvMirrorTest, Locks) {
  FileLock* lock;

  // These are no-ops, but we test they return success.
  ASSERT_OK(env_->LockFile("some file", &lock));
  ASSERT_OK(env_->UnlockFile(lock));
}

TEST_F(EnvMirrorTest, Misc) {
  std::string test_dir;
  ASSERT_OK(env_->GetTestDirectory(&test_dir));
  ASSERT_TRUE(!test_dir.empty());

  std::unique_ptr<WritableFile> writable_file;
  ASSERT_OK(env_->NewWritableFile("/a/b", &writable_file, soptions_));

  // These are no-ops, but we test they return success.
  ASSERT_OK(writable_file->Sync());
  ASSERT_OK(writable_file->Flush());
  ASSERT_OK(writable_file->Close());
  writable_file.reset();
}

TEST_F(EnvMirrorTest, LargeWrite) {
  const size_t kWriteSize = 300 * 1024;
  char* scratch = new char[kWriteSize * 2];

  std::string write_data;
  for (size_t i = 0; i < kWriteSize; ++i) {
    write_data.append(1, static_cast<char>(i));
  }

  std::unique_ptr<WritableFile> writable_file;
  ASSERT_OK(env_->NewWritableFile("/dir/f", &writable_file, soptions_));
  ASSERT_OK(writable_file->Append("foo"));
  ASSERT_OK(writable_file->Append(write_data));
  writable_file.reset();

  std::unique_ptr<SequentialFile> seq_file;
  Slice result;
  ASSERT_OK(env_->NewSequentialFile("/dir/f", &seq_file, soptions_));
  ASSERT_OK(seq_file->Read(3, &result, scratch));  // Read "foo".
  ASSERT_EQ(0, result.compare("foo"));

  size_t read = 0;
  std::string read_data;
  while (read < kWriteSize) {
    ASSERT_OK(seq_file->Read(kWriteSize - read, &result, scratch));
    read_data.append(result.data(), result.size());
    read += result.size();
  }
  ASSERT_TRUE(write_data == read_data);
  delete[] scratch;
}

}  // namespace rocksdb

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}

#else
#include <stdio.h>

int main(int argc, char** argv) {
  fprintf(stderr, "SKIPPED as EnvMirror is not supported in ROCKSDB_LITE\n");
  return 0;
}

#endif  // !ROCKSDB_LITE

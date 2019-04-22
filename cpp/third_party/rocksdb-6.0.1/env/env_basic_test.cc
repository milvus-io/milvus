// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include <memory>
#include <string>
#include <vector>
#include <algorithm>

#include "env/mock_env.h"
#include "rocksdb/env.h"
#include "rocksdb/utilities/object_registry.h"
#include "util/testharness.h"

namespace rocksdb {

// Normalizes trivial differences across Envs such that these test cases can
// run on all Envs.
class NormalizingEnvWrapper : public EnvWrapper {
 public:
  explicit NormalizingEnvWrapper(Env* base) : EnvWrapper(base) {}

  // Removes . and .. from directory listing
  Status GetChildren(const std::string& dir,
                     std::vector<std::string>* result) override {
    Status status = EnvWrapper::GetChildren(dir, result);
    if (status.ok()) {
      result->erase(std::remove_if(result->begin(), result->end(),
                                   [](const std::string& s) {
                                     return s == "." || s == "..";
                                   }),
                    result->end());
    }
    return status;
  }

  // Removes . and .. from directory listing
  Status GetChildrenFileAttributes(
      const std::string& dir, std::vector<FileAttributes>* result) override {
    Status status = EnvWrapper::GetChildrenFileAttributes(dir, result);
    if (status.ok()) {
      result->erase(std::remove_if(result->begin(), result->end(),
                                   [](const FileAttributes& fa) {
                                     return fa.name == "." || fa.name == "..";
                                   }),
                    result->end());
    }
    return status;
  }
};

class EnvBasicTestWithParam : public testing::Test,
                              public ::testing::WithParamInterface<Env*> {
 public:
  Env* env_;
  const EnvOptions soptions_;
  std::string test_dir_;

  EnvBasicTestWithParam() : env_(GetParam()) {
    test_dir_ = test::PerThreadDBPath(env_, "env_basic_test");
  }

  void SetUp() override { env_->CreateDirIfMissing(test_dir_); }

  void TearDown() override {
    std::vector<std::string> files;
    env_->GetChildren(test_dir_, &files);
    for (const auto& file : files) {
      // don't know whether it's file or directory, try both. The tests must
      // only create files or empty directories, so one must succeed, else the
      // directory's corrupted.
      Status s = env_->DeleteFile(test_dir_ + "/" + file);
      if (!s.ok()) {
        ASSERT_OK(env_->DeleteDir(test_dir_ + "/" + file));
      }
    }
  }
};

class EnvMoreTestWithParam : public EnvBasicTestWithParam {};

static std::unique_ptr<Env> def_env(new NormalizingEnvWrapper(Env::Default()));
INSTANTIATE_TEST_CASE_P(EnvDefault, EnvBasicTestWithParam,
                        ::testing::Values(def_env.get()));
INSTANTIATE_TEST_CASE_P(EnvDefault, EnvMoreTestWithParam,
                        ::testing::Values(def_env.get()));

static std::unique_ptr<Env> mock_env(new MockEnv(Env::Default()));
INSTANTIATE_TEST_CASE_P(MockEnv, EnvBasicTestWithParam,
                        ::testing::Values(mock_env.get()));
#ifndef ROCKSDB_LITE
static std::unique_ptr<Env> mem_env(NewMemEnv(Env::Default()));
INSTANTIATE_TEST_CASE_P(MemEnv, EnvBasicTestWithParam,
                        ::testing::Values(mem_env.get()));

namespace {

// Returns a vector of 0 or 1 Env*, depending whether an Env is registered for
// TEST_ENV_URI.
//
// The purpose of returning an empty vector (instead of nullptr) is that gtest
// ValuesIn() will skip running tests when given an empty collection.
std::vector<Env*> GetCustomEnvs() {
  static Env* custom_env;
  static std::unique_ptr<Env> custom_env_guard;
  static bool init = false;
  if (!init) {
    init = true;
    const char* uri = getenv("TEST_ENV_URI");
    if (uri != nullptr) {
      custom_env = NewCustomObject<Env>(uri, &custom_env_guard);
    }
  }

  std::vector<Env*> res;
  if (custom_env != nullptr) {
    res.emplace_back(custom_env);
  }
  return res;
}

}  // anonymous namespace

INSTANTIATE_TEST_CASE_P(CustomEnv, EnvBasicTestWithParam,
                        ::testing::ValuesIn(GetCustomEnvs()));

INSTANTIATE_TEST_CASE_P(CustomEnv, EnvMoreTestWithParam,
                        ::testing::ValuesIn(GetCustomEnvs()));

#endif  // ROCKSDB_LITE

TEST_P(EnvBasicTestWithParam, Basics) {
  uint64_t file_size;
  std::unique_ptr<WritableFile> writable_file;
  std::vector<std::string> children;

  // Check that the directory is empty.
  ASSERT_EQ(Status::NotFound(), env_->FileExists(test_dir_ + "/non_existent"));
  ASSERT_TRUE(!env_->GetFileSize(test_dir_ + "/non_existent", &file_size).ok());
  ASSERT_OK(env_->GetChildren(test_dir_, &children));
  ASSERT_EQ(0U, children.size());

  // Create a file.
  ASSERT_OK(env_->NewWritableFile(test_dir_ + "/f", &writable_file, soptions_));
  ASSERT_OK(writable_file->Close());
  writable_file.reset();

  // Check that the file exists.
  ASSERT_OK(env_->FileExists(test_dir_ + "/f"));
  ASSERT_OK(env_->GetFileSize(test_dir_ + "/f", &file_size));
  ASSERT_EQ(0U, file_size);
  ASSERT_OK(env_->GetChildren(test_dir_, &children));
  ASSERT_EQ(1U, children.size());
  ASSERT_EQ("f", children[0]);
  ASSERT_OK(env_->DeleteFile(test_dir_ + "/f"));

  // Write to the file.
  ASSERT_OK(
      env_->NewWritableFile(test_dir_ + "/f1", &writable_file, soptions_));
  ASSERT_OK(writable_file->Append("abc"));
  ASSERT_OK(writable_file->Close());
  writable_file.reset();
  ASSERT_OK(
      env_->NewWritableFile(test_dir_ + "/f2", &writable_file, soptions_));
  ASSERT_OK(writable_file->Close());
  writable_file.reset();

  // Check for expected size.
  ASSERT_OK(env_->GetFileSize(test_dir_ + "/f1", &file_size));
  ASSERT_EQ(3U, file_size);

  // Check that renaming works.
  ASSERT_TRUE(
      !env_->RenameFile(test_dir_ + "/non_existent", test_dir_ + "/g").ok());
  ASSERT_OK(env_->RenameFile(test_dir_ + "/f1", test_dir_ + "/g"));
  ASSERT_EQ(Status::NotFound(), env_->FileExists(test_dir_ + "/f1"));
  ASSERT_OK(env_->FileExists(test_dir_ + "/g"));
  ASSERT_OK(env_->GetFileSize(test_dir_ + "/g", &file_size));
  ASSERT_EQ(3U, file_size);

  // Check that renaming overwriting works
  ASSERT_OK(env_->RenameFile(test_dir_ + "/f2", test_dir_ + "/g"));
  ASSERT_OK(env_->GetFileSize(test_dir_ + "/g", &file_size));
  ASSERT_EQ(0U, file_size);

  // Check that opening non-existent file fails.
  std::unique_ptr<SequentialFile> seq_file;
  std::unique_ptr<RandomAccessFile> rand_file;
  ASSERT_TRUE(!env_->NewSequentialFile(test_dir_ + "/non_existent", &seq_file,
                                       soptions_)
                   .ok());
  ASSERT_TRUE(!seq_file);
  ASSERT_TRUE(!env_->NewRandomAccessFile(test_dir_ + "/non_existent",
                                         &rand_file, soptions_)
                   .ok());
  ASSERT_TRUE(!rand_file);

  // Check that deleting works.
  ASSERT_TRUE(!env_->DeleteFile(test_dir_ + "/non_existent").ok());
  ASSERT_OK(env_->DeleteFile(test_dir_ + "/g"));
  ASSERT_EQ(Status::NotFound(), env_->FileExists(test_dir_ + "/g"));
  ASSERT_OK(env_->GetChildren(test_dir_, &children));
  ASSERT_EQ(0U, children.size());
  ASSERT_TRUE(
      env_->GetChildren(test_dir_ + "/non_existent", &children).IsNotFound());
}

TEST_P(EnvBasicTestWithParam, ReadWrite) {
  std::unique_ptr<WritableFile> writable_file;
  std::unique_ptr<SequentialFile> seq_file;
  std::unique_ptr<RandomAccessFile> rand_file;
  Slice result;
  char scratch[100];

  ASSERT_OK(env_->NewWritableFile(test_dir_ + "/f", &writable_file, soptions_));
  ASSERT_OK(writable_file->Append("hello "));
  ASSERT_OK(writable_file->Append("world"));
  ASSERT_OK(writable_file->Close());
  writable_file.reset();

  // Read sequentially.
  ASSERT_OK(env_->NewSequentialFile(test_dir_ + "/f", &seq_file, soptions_));
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
  ASSERT_OK(env_->NewRandomAccessFile(test_dir_ + "/f", &rand_file, soptions_));
  ASSERT_OK(rand_file->Read(6, 5, &result, scratch));  // Read "world".
  ASSERT_EQ(0, result.compare("world"));
  ASSERT_OK(rand_file->Read(0, 5, &result, scratch));  // Read "hello".
  ASSERT_EQ(0, result.compare("hello"));
  ASSERT_OK(rand_file->Read(10, 100, &result, scratch));  // Read "d".
  ASSERT_EQ(0, result.compare("d"));

  // Too high offset.
  ASSERT_TRUE(rand_file->Read(1000, 5, &result, scratch).ok());
}

TEST_P(EnvBasicTestWithParam, Misc) {
  std::unique_ptr<WritableFile> writable_file;
  ASSERT_OK(env_->NewWritableFile(test_dir_ + "/b", &writable_file, soptions_));

  // These are no-ops, but we test they return success.
  ASSERT_OK(writable_file->Sync());
  ASSERT_OK(writable_file->Flush());
  ASSERT_OK(writable_file->Close());
  writable_file.reset();
}

TEST_P(EnvBasicTestWithParam, LargeWrite) {
  const size_t kWriteSize = 300 * 1024;
  char* scratch = new char[kWriteSize * 2];

  std::string write_data;
  for (size_t i = 0; i < kWriteSize; ++i) {
    write_data.append(1, static_cast<char>(i));
  }

  std::unique_ptr<WritableFile> writable_file;
  ASSERT_OK(env_->NewWritableFile(test_dir_ + "/f", &writable_file, soptions_));
  ASSERT_OK(writable_file->Append("foo"));
  ASSERT_OK(writable_file->Append(write_data));
  ASSERT_OK(writable_file->Close());
  writable_file.reset();

  std::unique_ptr<SequentialFile> seq_file;
  Slice result;
  ASSERT_OK(env_->NewSequentialFile(test_dir_ + "/f", &seq_file, soptions_));
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
  delete [] scratch;
}

TEST_P(EnvMoreTestWithParam, GetModTime) {
  ASSERT_OK(env_->CreateDirIfMissing(test_dir_ + "/dir1"));
  uint64_t mtime1 = 0x0;
  ASSERT_OK(env_->GetFileModificationTime(test_dir_ + "/dir1", &mtime1));
}

TEST_P(EnvMoreTestWithParam, MakeDir) {
  ASSERT_OK(env_->CreateDir(test_dir_ + "/j"));
  ASSERT_OK(env_->FileExists(test_dir_ + "/j"));
  std::vector<std::string> children;
  env_->GetChildren(test_dir_, &children);
  ASSERT_EQ(1U, children.size());
  // fail because file already exists
  ASSERT_TRUE(!env_->CreateDir(test_dir_ + "/j").ok());
  ASSERT_OK(env_->CreateDirIfMissing(test_dir_ + "/j"));
  ASSERT_OK(env_->DeleteDir(test_dir_ + "/j"));
  ASSERT_EQ(Status::NotFound(), env_->FileExists(test_dir_ + "/j"));
}

TEST_P(EnvMoreTestWithParam, GetChildren) {
  // empty folder returns empty vector
  std::vector<std::string> children;
  std::vector<Env::FileAttributes> childAttr;
  ASSERT_OK(env_->CreateDirIfMissing(test_dir_));
  ASSERT_OK(env_->GetChildren(test_dir_, &children));
  ASSERT_OK(env_->FileExists(test_dir_));
  ASSERT_OK(env_->GetChildrenFileAttributes(test_dir_, &childAttr));
  ASSERT_EQ(0U, children.size());
  ASSERT_EQ(0U, childAttr.size());

  // folder with contents returns relative path to test dir
  ASSERT_OK(env_->CreateDirIfMissing(test_dir_ + "/niu"));
  ASSERT_OK(env_->CreateDirIfMissing(test_dir_ + "/you"));
  ASSERT_OK(env_->CreateDirIfMissing(test_dir_ + "/guo"));
  ASSERT_OK(env_->GetChildren(test_dir_, &children));
  ASSERT_OK(env_->GetChildrenFileAttributes(test_dir_, &childAttr));
  ASSERT_EQ(3U, children.size());
  ASSERT_EQ(3U, childAttr.size());
  for (auto each : children) {
    env_->DeleteDir(test_dir_ + "/" + each);
  }  // necessary for default POSIX env

  // non-exist directory returns IOError
  ASSERT_OK(env_->DeleteDir(test_dir_));
  ASSERT_TRUE(!env_->FileExists(test_dir_).ok());
  ASSERT_TRUE(!env_->GetChildren(test_dir_, &children).ok());
  ASSERT_TRUE(!env_->GetChildrenFileAttributes(test_dir_, &childAttr).ok());

  // if dir is a file, returns IOError
  ASSERT_OK(env_->CreateDir(test_dir_));
  std::unique_ptr<WritableFile> writable_file;
  ASSERT_OK(
      env_->NewWritableFile(test_dir_ + "/file", &writable_file, soptions_));
  ASSERT_OK(writable_file->Close());
  writable_file.reset();
  ASSERT_TRUE(!env_->GetChildren(test_dir_ + "/file", &children).ok());
  ASSERT_EQ(0U, children.size());
}

}  // namespace rocksdb
int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}

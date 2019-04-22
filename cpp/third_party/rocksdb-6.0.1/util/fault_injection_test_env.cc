//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright 2014 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

// This test uses a custom Env to keep track of the state of a filesystem as of
// the last "sync". It then checks for data loss errors by purposely dropping
// file data (or entire files) not protected by a "sync".

#include "util/fault_injection_test_env.h"
#include <functional>
#include <utility>

namespace rocksdb {

// Assume a filename, and not a directory name like "/foo/bar/"
std::string GetDirName(const std::string filename) {
  size_t found = filename.find_last_of("/\\");
  if (found == std::string::npos) {
    return "";
  } else {
    return filename.substr(0, found);
  }
}

// A basic file truncation function suitable for this test.
Status Truncate(Env* env, const std::string& filename, uint64_t length) {
  std::unique_ptr<SequentialFile> orig_file;
  const EnvOptions options;
  Status s = env->NewSequentialFile(filename, &orig_file, options);
  if (!s.ok()) {
    fprintf(stderr, "Cannot open file %s for truncation: %s\n",
            filename.c_str(), s.ToString().c_str());
    return s;
  }

  std::unique_ptr<char[]> scratch(new char[length]);
  rocksdb::Slice result;
  s = orig_file->Read(length, &result, scratch.get());
#ifdef OS_WIN
  orig_file.reset();
#endif
  if (s.ok()) {
    std::string tmp_name = GetDirName(filename) + "/truncate.tmp";
    std::unique_ptr<WritableFile> tmp_file;
    s = env->NewWritableFile(tmp_name, &tmp_file, options);
    if (s.ok()) {
      s = tmp_file->Append(result);
      if (s.ok()) {
        s = env->RenameFile(tmp_name, filename);
      } else {
        fprintf(stderr, "Cannot rename file %s to %s: %s\n", tmp_name.c_str(),
                filename.c_str(), s.ToString().c_str());
        env->DeleteFile(tmp_name);
      }
    }
  }
  if (!s.ok()) {
    fprintf(stderr, "Cannot truncate file %s: %s\n", filename.c_str(),
            s.ToString().c_str());
  }

  return s;
}

// Trim the tailing "/" in the end of `str`
std::string TrimDirname(const std::string& str) {
  size_t found = str.find_last_not_of("/");
  if (found == std::string::npos) {
    return str;
  }
  return str.substr(0, found + 1);
}

// Return pair <parent directory name, file name> of a full path.
std::pair<std::string, std::string> GetDirAndName(const std::string& name) {
  std::string dirname = GetDirName(name);
  std::string fname = name.substr(dirname.size() + 1);
  return std::make_pair(dirname, fname);
}

Status FileState::DropUnsyncedData(Env* env) const {
  ssize_t sync_pos = pos_at_last_sync_ == -1 ? 0 : pos_at_last_sync_;
  return Truncate(env, filename_, sync_pos);
}

Status FileState::DropRandomUnsyncedData(Env* env, Random* rand) const {
  ssize_t sync_pos = pos_at_last_sync_ == -1 ? 0 : pos_at_last_sync_;
  assert(pos_ >= sync_pos);
  int range = static_cast<int>(pos_ - sync_pos);
  uint64_t truncated_size =
      static_cast<uint64_t>(sync_pos) + rand->Uniform(range);
  return Truncate(env, filename_, truncated_size);
}

Status TestDirectory::Fsync() {
  env_->SyncDir(dirname_);
  return dir_->Fsync();
}

TestWritableFile::TestWritableFile(const std::string& fname,
                                   std::unique_ptr<WritableFile>&& f,
                                   FaultInjectionTestEnv* env)
    : state_(fname),
      target_(std::move(f)),
      writable_file_opened_(true),
      env_(env) {
  assert(target_ != nullptr);
  state_.pos_ = 0;
}

TestWritableFile::~TestWritableFile() {
  if (writable_file_opened_) {
    Close();
  }
}

Status TestWritableFile::Append(const Slice& data) {
  if (!env_->IsFilesystemActive()) {
    return env_->GetError();
  }
  Status s = target_->Append(data);
  if (s.ok()) {
    state_.pos_ += data.size();
    env_->WritableFileAppended(state_);
  }
  return s;
}

Status TestWritableFile::Close() {
  writable_file_opened_ = false;
  Status s = target_->Close();
  if (s.ok()) {
    env_->WritableFileClosed(state_);
  }
  return s;
}

Status TestWritableFile::Flush() {
  Status s = target_->Flush();
  if (s.ok() && env_->IsFilesystemActive()) {
    state_.pos_at_last_flush_ = state_.pos_;
  }
  return s;
}

Status TestWritableFile::Sync() {
  if (!env_->IsFilesystemActive()) {
    return Status::IOError("FaultInjectionTestEnv: not active");
  }
  // No need to actual sync.
  state_.pos_at_last_sync_ = state_.pos_;
  env_->WritableFileSynced(state_);
  return Status::OK();
}

Status FaultInjectionTestEnv::NewDirectory(const std::string& name,
                                           std::unique_ptr<Directory>* result) {
  std::unique_ptr<Directory> r;
  Status s = target()->NewDirectory(name, &r);
  assert(s.ok());
  if (!s.ok()) {
    return s;
  }
  result->reset(new TestDirectory(this, TrimDirname(name), r.release()));
  return Status::OK();
}

Status FaultInjectionTestEnv::NewWritableFile(
    const std::string& fname, std::unique_ptr<WritableFile>* result,
    const EnvOptions& soptions) {
  if (!IsFilesystemActive()) {
    return GetError();
  }
  // Not allow overwriting files
  Status s = target()->FileExists(fname);
  if (s.ok()) {
    return Status::Corruption("File already exists.");
  } else if (!s.IsNotFound()) {
    assert(s.IsIOError());
    return s;
  }
  s = target()->NewWritableFile(fname, result, soptions);
  if (s.ok()) {
    result->reset(new TestWritableFile(fname, std::move(*result), this));
    // WritableFileWriter* file is opened
    // again then it will be truncated - so forget our saved state.
    UntrackFile(fname);
    MutexLock l(&mutex_);
    open_files_.insert(fname);
    auto dir_and_name = GetDirAndName(fname);
    auto& list = dir_to_new_files_since_last_sync_[dir_and_name.first];
    list.insert(dir_and_name.second);
  }
  return s;
}

Status FaultInjectionTestEnv::ReopenWritableFile(
    const std::string& fname, std::unique_ptr<WritableFile>* result,
    const EnvOptions& soptions) {
  if (!IsFilesystemActive()) {
    return GetError();
  }
  Status s = target()->ReopenWritableFile(fname, result, soptions);
  if (s.ok()) {
    result->reset(new TestWritableFile(fname, std::move(*result), this));
    // WritableFileWriter* file is opened
    // again then it will be truncated - so forget our saved state.
    UntrackFile(fname);
    MutexLock l(&mutex_);
    open_files_.insert(fname);
    auto dir_and_name = GetDirAndName(fname);
    auto& list = dir_to_new_files_since_last_sync_[dir_and_name.first];
    list.insert(dir_and_name.second);
  }
  return s;
}

Status FaultInjectionTestEnv::NewRandomAccessFile(
    const std::string& fname, std::unique_ptr<RandomAccessFile>* result,
    const EnvOptions& soptions) {
  if (!IsFilesystemActive()) {
    return GetError();
  }
  return target()->NewRandomAccessFile(fname, result, soptions);
}

Status FaultInjectionTestEnv::DeleteFile(const std::string& f) {
  if (!IsFilesystemActive()) {
    return GetError();
  }
  Status s = EnvWrapper::DeleteFile(f);
  if (!s.ok()) {
    fprintf(stderr, "Cannot delete file %s: %s\n", f.c_str(),
            s.ToString().c_str());
  }
  assert(s.ok());
  if (s.ok()) {
    UntrackFile(f);
  }
  return s;
}

Status FaultInjectionTestEnv::RenameFile(const std::string& s,
                                         const std::string& t) {
  if (!IsFilesystemActive()) {
    return GetError();
  }
  Status ret = EnvWrapper::RenameFile(s, t);

  if (ret.ok()) {
    MutexLock l(&mutex_);
    if (db_file_state_.find(s) != db_file_state_.end()) {
      db_file_state_[t] = db_file_state_[s];
      db_file_state_.erase(s);
    }

    auto sdn = GetDirAndName(s);
    auto tdn = GetDirAndName(t);
    if (dir_to_new_files_since_last_sync_[sdn.first].erase(sdn.second) != 0) {
      auto& tlist = dir_to_new_files_since_last_sync_[tdn.first];
      assert(tlist.find(tdn.second) == tlist.end());
      tlist.insert(tdn.second);
    }
  }

  return ret;
}

void FaultInjectionTestEnv::WritableFileClosed(const FileState& state) {
  MutexLock l(&mutex_);
  if (open_files_.find(state.filename_) != open_files_.end()) {
    db_file_state_[state.filename_] = state;
    open_files_.erase(state.filename_);
  }
}

void FaultInjectionTestEnv::WritableFileSynced(const FileState& state) {
  MutexLock l(&mutex_);
  if (open_files_.find(state.filename_) != open_files_.end()) {
    if (db_file_state_.find(state.filename_) == db_file_state_.end()) {
      db_file_state_.insert(std::make_pair(state.filename_, state));
    } else {
      db_file_state_[state.filename_] = state;
    }
  }
}

void FaultInjectionTestEnv::WritableFileAppended(const FileState& state) {
  MutexLock l(&mutex_);
  if (open_files_.find(state.filename_) != open_files_.end()) {
    if (db_file_state_.find(state.filename_) == db_file_state_.end()) {
      db_file_state_.insert(std::make_pair(state.filename_, state));
    } else {
      db_file_state_[state.filename_] = state;
    }
  }
}

// For every file that is not fully synced, make a call to `func` with
// FileState of the file as the parameter.
Status FaultInjectionTestEnv::DropFileData(
    std::function<Status(Env*, FileState)> func) {
  Status s;
  MutexLock l(&mutex_);
  for (std::map<std::string, FileState>::const_iterator it =
           db_file_state_.begin();
       s.ok() && it != db_file_state_.end(); ++it) {
    const FileState& state = it->second;
    if (!state.IsFullySynced()) {
      s = func(target(), state);
    }
  }
  return s;
}

Status FaultInjectionTestEnv::DropUnsyncedFileData() {
  return DropFileData([&](Env* env, const FileState& state) {
    return state.DropUnsyncedData(env);
  });
}

Status FaultInjectionTestEnv::DropRandomUnsyncedFileData(Random* rnd) {
  return DropFileData([&](Env* env, const FileState& state) {
    return state.DropRandomUnsyncedData(env, rnd);
  });
}

Status FaultInjectionTestEnv::DeleteFilesCreatedAfterLastDirSync() {
  // Because DeleteFile access this container make a copy to avoid deadlock
  std::map<std::string, std::set<std::string>> map_copy;
  {
    MutexLock l(&mutex_);
    map_copy.insert(dir_to_new_files_since_last_sync_.begin(),
                    dir_to_new_files_since_last_sync_.end());
  }

  for (auto& pair : map_copy) {
    for (std::string name : pair.second) {
      Status s = DeleteFile(pair.first + "/" + name);
      if (!s.ok()) {
        return s;
      }
    }
  }
  return Status::OK();
}
void FaultInjectionTestEnv::ResetState() {
  MutexLock l(&mutex_);
  db_file_state_.clear();
  dir_to_new_files_since_last_sync_.clear();
  SetFilesystemActiveNoLock(true);
}

void FaultInjectionTestEnv::UntrackFile(const std::string& f) {
  MutexLock l(&mutex_);
  auto dir_and_name = GetDirAndName(f);
  dir_to_new_files_since_last_sync_[dir_and_name.first].erase(
      dir_and_name.second);
  db_file_state_.erase(f);
  open_files_.erase(f);
}
}  // namespace rocksdb

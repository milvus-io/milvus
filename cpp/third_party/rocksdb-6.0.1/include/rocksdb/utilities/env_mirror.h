// Copyright (c) 2015, Red Hat, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.
//
// MirrorEnv is an Env implementation that mirrors all file-related
// operations to two backing Env's (provided at construction time).
// Writes are mirrored.  For read operations, we do the read from both
// backends and assert that the results match.
//
// This is useful when implementing a new Env and ensuring that the
// semantics and behavior are correct (in that they match that of an
// existing, stable Env, like the default POSIX one).

#pragma once

#ifndef ROCKSDB_LITE

#include <iostream>
#include <algorithm>
#include <vector>
#include "rocksdb/env.h"

namespace rocksdb {

class SequentialFileMirror;
class RandomAccessFileMirror;
class WritableFileMirror;

class EnvMirror : public EnvWrapper {
  Env* a_, *b_;
  bool free_a_, free_b_;

 public:
  EnvMirror(Env* a, Env* b, bool free_a=false, bool free_b=false)
    : EnvWrapper(a),
      a_(a),
      b_(b),
      free_a_(free_a),
      free_b_(free_b) {}
  ~EnvMirror() {
    if (free_a_)
      delete a_;
    if (free_b_)
      delete b_;
  }

  Status NewSequentialFile(const std::string& f,
                           std::unique_ptr<SequentialFile>* r,
                           const EnvOptions& options) override;
  Status NewRandomAccessFile(const std::string& f,
                             std::unique_ptr<RandomAccessFile>* r,
                             const EnvOptions& options) override;
  Status NewWritableFile(const std::string& f, std::unique_ptr<WritableFile>* r,
                         const EnvOptions& options) override;
  Status ReuseWritableFile(const std::string& fname,
                           const std::string& old_fname,
                           std::unique_ptr<WritableFile>* r,
                           const EnvOptions& options) override;
  virtual Status NewDirectory(const std::string& name,
                              std::unique_ptr<Directory>* result) override {
    std::unique_ptr<Directory> br;
    Status as = a_->NewDirectory(name, result);
    Status bs = b_->NewDirectory(name, &br);
    assert(as == bs);
    return as;
  }
  Status FileExists(const std::string& f) override {
    Status as = a_->FileExists(f);
    Status bs = b_->FileExists(f);
    assert(as == bs);
    return as;
  }
#if defined(_MSC_VER)
#pragma warning(push)
// logical operation on address of string constant
#pragma warning(disable : 4130)
#endif
  Status GetChildren(const std::string& dir,
                     std::vector<std::string>* r) override {
    std::vector<std::string> ar, br;
    Status as = a_->GetChildren(dir, &ar);
    Status bs = b_->GetChildren(dir, &br);
    assert(as == bs);
    std::sort(ar.begin(), ar.end());
    std::sort(br.begin(), br.end());
    if (!as.ok() || ar != br) {
      assert(0 == "getchildren results don't match");
    }
    *r = ar;
    return as;
  }
#if defined(_MSC_VER)
#pragma warning(pop)
#endif
  Status DeleteFile(const std::string& f) override {
    Status as = a_->DeleteFile(f);
    Status bs = b_->DeleteFile(f);
    assert(as == bs);
    return as;
  }
  Status CreateDir(const std::string& d) override {
    Status as = a_->CreateDir(d);
    Status bs = b_->CreateDir(d);
    assert(as == bs);
    return as;
  }
  Status CreateDirIfMissing(const std::string& d) override {
    Status as = a_->CreateDirIfMissing(d);
    Status bs = b_->CreateDirIfMissing(d);
    assert(as == bs);
    return as;
  }
  Status DeleteDir(const std::string& d) override {
    Status as = a_->DeleteDir(d);
    Status bs = b_->DeleteDir(d);
    assert(as == bs);
    return as;
  }
  Status GetFileSize(const std::string& f, uint64_t* s) override {
    uint64_t asize, bsize;
    Status as = a_->GetFileSize(f, &asize);
    Status bs = b_->GetFileSize(f, &bsize);
    assert(as == bs);
    assert(!as.ok() || asize == bsize);
    *s = asize;
    return as;
  }

  Status GetFileModificationTime(const std::string& fname,
                                 uint64_t* file_mtime) override {
    uint64_t amtime, bmtime;
    Status as = a_->GetFileModificationTime(fname, &amtime);
    Status bs = b_->GetFileModificationTime(fname, &bmtime);
    assert(as == bs);
    assert(!as.ok() || amtime - bmtime < 10000 || bmtime - amtime < 10000);
    *file_mtime = amtime;
    return as;
  }

  Status RenameFile(const std::string& s, const std::string& t) override {
    Status as = a_->RenameFile(s, t);
    Status bs = b_->RenameFile(s, t);
    assert(as == bs);
    return as;
  }

  Status LinkFile(const std::string& s, const std::string& t) override {
    Status as = a_->LinkFile(s, t);
    Status bs = b_->LinkFile(s, t);
    assert(as == bs);
    return as;
  }

  class FileLockMirror : public FileLock {
   public:
    FileLock* a_, *b_;
    FileLockMirror(FileLock* a, FileLock* b) : a_(a), b_(b) {}
  };

  Status LockFile(const std::string& f, FileLock** l) override {
    FileLock* al, *bl;
    Status as = a_->LockFile(f, &al);
    Status bs = b_->LockFile(f, &bl);
    assert(as == bs);
    if (as.ok()) *l = new FileLockMirror(al, bl);
    return as;
  }

  Status UnlockFile(FileLock* l) override {
    FileLockMirror* ml = static_cast<FileLockMirror*>(l);
    Status as = a_->UnlockFile(ml->a_);
    Status bs = b_->UnlockFile(ml->b_);
    assert(as == bs);
    delete ml;
    return as;
  }
};

}  // namespace rocksdb

#endif  // ROCKSDB_LITE

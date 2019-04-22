//  Copyright (c) 2016-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#if !defined(ROCKSDB_LITE) && !defined(OS_WIN)

#include "env/env_chroot.h"

#include <errno.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

#include <string>
#include <utility>
#include <vector>

#include "rocksdb/status.h"

namespace rocksdb {

class ChrootEnv : public EnvWrapper {
 public:
  ChrootEnv(Env* base_env, const std::string& chroot_dir)
      : EnvWrapper(base_env) {
#if defined(OS_AIX)
    char resolvedName[PATH_MAX];
    char* real_chroot_dir = realpath(chroot_dir.c_str(), resolvedName);
#else
    char* real_chroot_dir = realpath(chroot_dir.c_str(), nullptr);
#endif
    // chroot_dir must exist so realpath() returns non-nullptr.
    assert(real_chroot_dir != nullptr);
    chroot_dir_ = real_chroot_dir;
#if !defined(OS_AIX)
    free(real_chroot_dir);
#endif
  }

  Status NewSequentialFile(const std::string& fname,
                           std::unique_ptr<SequentialFile>* result,
                           const EnvOptions& options) override {
    auto status_and_enc_path = EncodePathWithNewBasename(fname);
    if (!status_and_enc_path.first.ok()) {
      return status_and_enc_path.first;
    }
    return EnvWrapper::NewSequentialFile(status_and_enc_path.second, result,
                                         options);
  }

  Status NewRandomAccessFile(const std::string& fname,
                             std::unique_ptr<RandomAccessFile>* result,
                             const EnvOptions& options) override {
    auto status_and_enc_path = EncodePathWithNewBasename(fname);
    if (!status_and_enc_path.first.ok()) {
      return status_and_enc_path.first;
    }
    return EnvWrapper::NewRandomAccessFile(status_and_enc_path.second, result,
                                           options);
  }

  Status NewWritableFile(const std::string& fname,
                         std::unique_ptr<WritableFile>* result,
                         const EnvOptions& options) override {
    auto status_and_enc_path = EncodePathWithNewBasename(fname);
    if (!status_and_enc_path.first.ok()) {
      return status_and_enc_path.first;
    }
    return EnvWrapper::NewWritableFile(status_and_enc_path.second, result,
                                       options);
  }

  Status ReuseWritableFile(const std::string& fname,
                           const std::string& old_fname,
                           std::unique_ptr<WritableFile>* result,
                           const EnvOptions& options) override {
    auto status_and_enc_path = EncodePathWithNewBasename(fname);
    if (!status_and_enc_path.first.ok()) {
      return status_and_enc_path.first;
    }
    auto status_and_old_enc_path = EncodePath(old_fname);
    if (!status_and_old_enc_path.first.ok()) {
      return status_and_old_enc_path.first;
    }
    return EnvWrapper::ReuseWritableFile(status_and_old_enc_path.second,
                                         status_and_old_enc_path.second, result,
                                         options);
  }

  Status NewRandomRWFile(const std::string& fname,
                         std::unique_ptr<RandomRWFile>* result,
                         const EnvOptions& options) override {
    auto status_and_enc_path = EncodePathWithNewBasename(fname);
    if (!status_and_enc_path.first.ok()) {
      return status_and_enc_path.first;
    }
    return EnvWrapper::NewRandomRWFile(status_and_enc_path.second, result,
                                       options);
  }

  Status NewDirectory(const std::string& dir,
                      std::unique_ptr<Directory>* result) override {
    auto status_and_enc_path = EncodePathWithNewBasename(dir);
    if (!status_and_enc_path.first.ok()) {
      return status_and_enc_path.first;
    }
    return EnvWrapper::NewDirectory(status_and_enc_path.second, result);
  }

  Status FileExists(const std::string& fname) override {
    auto status_and_enc_path = EncodePathWithNewBasename(fname);
    if (!status_and_enc_path.first.ok()) {
      return status_and_enc_path.first;
    }
    return EnvWrapper::FileExists(status_and_enc_path.second);
  }

  Status GetChildren(const std::string& dir,
                     std::vector<std::string>* result) override {
    auto status_and_enc_path = EncodePath(dir);
    if (!status_and_enc_path.first.ok()) {
      return status_and_enc_path.first;
    }
    return EnvWrapper::GetChildren(status_and_enc_path.second, result);
  }

  Status GetChildrenFileAttributes(
      const std::string& dir, std::vector<FileAttributes>* result) override {
    auto status_and_enc_path = EncodePath(dir);
    if (!status_and_enc_path.first.ok()) {
      return status_and_enc_path.first;
    }
    return EnvWrapper::GetChildrenFileAttributes(status_and_enc_path.second,
                                                 result);
  }

  Status DeleteFile(const std::string& fname) override {
    auto status_and_enc_path = EncodePath(fname);
    if (!status_and_enc_path.first.ok()) {
      return status_and_enc_path.first;
    }
    return EnvWrapper::DeleteFile(status_and_enc_path.second);
  }

  Status CreateDir(const std::string& dirname) override {
    auto status_and_enc_path = EncodePathWithNewBasename(dirname);
    if (!status_and_enc_path.first.ok()) {
      return status_and_enc_path.first;
    }
    return EnvWrapper::CreateDir(status_and_enc_path.second);
  }

  Status CreateDirIfMissing(const std::string& dirname) override {
    auto status_and_enc_path = EncodePathWithNewBasename(dirname);
    if (!status_and_enc_path.first.ok()) {
      return status_and_enc_path.first;
    }
    return EnvWrapper::CreateDirIfMissing(status_and_enc_path.second);
  }

  Status DeleteDir(const std::string& dirname) override {
    auto status_and_enc_path = EncodePath(dirname);
    if (!status_and_enc_path.first.ok()) {
      return status_and_enc_path.first;
    }
    return EnvWrapper::DeleteDir(status_and_enc_path.second);
  }

  Status GetFileSize(const std::string& fname, uint64_t* file_size) override {
    auto status_and_enc_path = EncodePath(fname);
    if (!status_and_enc_path.first.ok()) {
      return status_and_enc_path.first;
    }
    return EnvWrapper::GetFileSize(status_and_enc_path.second, file_size);
  }

  Status GetFileModificationTime(const std::string& fname,
                                 uint64_t* file_mtime) override {
    auto status_and_enc_path = EncodePath(fname);
    if (!status_and_enc_path.first.ok()) {
      return status_and_enc_path.first;
    }
    return EnvWrapper::GetFileModificationTime(status_and_enc_path.second,
                                               file_mtime);
  }

  Status RenameFile(const std::string& src, const std::string& dest) override {
    auto status_and_src_enc_path = EncodePath(src);
    if (!status_and_src_enc_path.first.ok()) {
      return status_and_src_enc_path.first;
    }
    auto status_and_dest_enc_path = EncodePathWithNewBasename(dest);
    if (!status_and_dest_enc_path.first.ok()) {
      return status_and_dest_enc_path.first;
    }
    return EnvWrapper::RenameFile(status_and_src_enc_path.second,
                                  status_and_dest_enc_path.second);
  }

  Status LinkFile(const std::string& src, const std::string& dest) override {
    auto status_and_src_enc_path = EncodePath(src);
    if (!status_and_src_enc_path.first.ok()) {
      return status_and_src_enc_path.first;
    }
    auto status_and_dest_enc_path = EncodePathWithNewBasename(dest);
    if (!status_and_dest_enc_path.first.ok()) {
      return status_and_dest_enc_path.first;
    }
    return EnvWrapper::LinkFile(status_and_src_enc_path.second,
                                status_and_dest_enc_path.second);
  }

  Status LockFile(const std::string& fname, FileLock** lock) override {
    auto status_and_enc_path = EncodePathWithNewBasename(fname);
    if (!status_and_enc_path.first.ok()) {
      return status_and_enc_path.first;
    }
    // FileLock subclasses may store path (e.g., PosixFileLock stores it). We
    // can skip stripping the chroot directory from this path because callers
    // shouldn't use it.
    return EnvWrapper::LockFile(status_and_enc_path.second, lock);
  }

  Status GetTestDirectory(std::string* path) override {
    // Adapted from PosixEnv's implementation since it doesn't provide a way to
    // create directory in the chroot.
    char buf[256];
    snprintf(buf, sizeof(buf), "/rocksdbtest-%d", static_cast<int>(geteuid()));
    *path = buf;

    // Directory may already exist, so ignore return
    CreateDir(*path);
    return Status::OK();
  }

  Status NewLogger(const std::string& fname,
                   std::shared_ptr<Logger>* result) override {
    auto status_and_enc_path = EncodePathWithNewBasename(fname);
    if (!status_and_enc_path.first.ok()) {
      return status_and_enc_path.first;
    }
    return EnvWrapper::NewLogger(status_and_enc_path.second, result);
  }

  Status GetAbsolutePath(const std::string& db_path,
                         std::string* output_path) override {
    auto status_and_enc_path = EncodePath(db_path);
    if (!status_and_enc_path.first.ok()) {
      return status_and_enc_path.first;
    }
    return EnvWrapper::GetAbsolutePath(status_and_enc_path.second, output_path);
  }

 private:
  // Returns status and expanded absolute path including the chroot directory.
  // Checks whether the provided path breaks out of the chroot. If it returns
  // non-OK status, the returned path should not be used.
  std::pair<Status, std::string> EncodePath(const std::string& path) {
    if (path.empty() || path[0] != '/') {
      return {Status::InvalidArgument(path, "Not an absolute path"), ""};
    }
    std::pair<Status, std::string> res;
    res.second = chroot_dir_ + path;
#if defined(OS_AIX)
    char resolvedName[PATH_MAX];
    char* normalized_path = realpath(res.second.c_str(), resolvedName);
#else
    char* normalized_path = realpath(res.second.c_str(), nullptr);
#endif
    if (normalized_path == nullptr) {
      res.first = Status::NotFound(res.second, strerror(errno));
    } else if (strlen(normalized_path) < chroot_dir_.size() ||
               strncmp(normalized_path, chroot_dir_.c_str(),
                       chroot_dir_.size()) != 0) {
      res.first = Status::IOError(res.second,
                                  "Attempted to access path outside chroot");
    } else {
      res.first = Status::OK();
    }
#if !defined(OS_AIX)
    free(normalized_path);
#endif
    return res;
  }

  // Similar to EncodePath() except assumes the basename in the path hasn't been
  // created yet.
  std::pair<Status, std::string> EncodePathWithNewBasename(
      const std::string& path) {
    if (path.empty() || path[0] != '/') {
      return {Status::InvalidArgument(path, "Not an absolute path"), ""};
    }
    // Basename may be followed by trailing slashes
    size_t final_idx = path.find_last_not_of('/');
    if (final_idx == std::string::npos) {
      // It's only slashes so no basename to extract
      return EncodePath(path);
    }

    // Pull off the basename temporarily since realname(3) (used by
    // EncodePath()) requires a path that exists
    size_t base_sep = path.rfind('/', final_idx);
    auto status_and_enc_path = EncodePath(path.substr(0, base_sep + 1));
    status_and_enc_path.second.append(path.substr(base_sep + 1));
    return status_and_enc_path;
  }

  std::string chroot_dir_;
};

Env* NewChrootEnv(Env* base_env, const std::string& chroot_dir) {
  if (!base_env->FileExists(chroot_dir).ok()) {
    return nullptr;
  }
  return new ChrootEnv(base_env, chroot_dir);
}

}  // namespace rocksdb

#endif  // !defined(ROCKSDB_LITE) && !defined(OS_WIN)

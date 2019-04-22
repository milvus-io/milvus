// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "rocksdb/utilities/env_librados.h"
#include "util/random.h"
#include <mutex>
#include <cstdlib>

namespace rocksdb {
/* GLOBAL DIFINE */
// #define DEBUG
#ifdef DEBUG
#include <cstdio>
#include <sys/syscall.h>
#include <unistd.h>
#define LOG_DEBUG(...)  do{\
    printf("[%ld:%s:%i:%s]", syscall(SYS_gettid), __FILE__, __LINE__, __FUNCTION__);\
    printf(__VA_ARGS__);\
  }while(0)
#else
#define LOG_DEBUG(...)
#endif

/* GLOBAL CONSTANT */
const char *default_db_name     = "default_envlibrados_db";
const char *default_pool_name   = "default_envlibrados_pool";
const char *default_config_path = "CEPH_CONFIG_PATH";           // the env variable name of ceph configure file
// maximum dir/file that can store in the fs
const int MAX_ITEMS_IN_FS = 1 << 30;
// root dir tag
const std::string ROOT_DIR_KEY = "/";
const std::string DIR_ID_VALUE = "<DIR>";

/**
 * @brief convert error code to status
 * @details Convert internal linux error code to Status
 *
 * @param r [description]
 * @return [description]
 */
Status err_to_status(int r)
{
  switch (r) {
  case 0:
    return Status::OK();
  case -ENOENT:
    return Status::IOError();
  case -ENODATA:
  case -ENOTDIR:
    return Status::NotFound(Status::kNone);
  case -EINVAL:
    return Status::InvalidArgument(Status::kNone);
  case -EIO:
    return Status::IOError(Status::kNone);
  default:
    // FIXME :(
    assert(0 == "unrecognized error code");
    return Status::NotSupported(Status::kNone);
  }
}

/**
 * @brief split file path into dir path and file name
 * @details
 * Because rocksdb only need a 2-level structure (dir/file), all input path will be shortened to dir/file format
 *  For example:
 *    b/c => dir '/b', file 'c'
 *    /a/b/c => dir '/b', file 'c'
 *
 * @param fn [description]
 * @param dir [description]
 * @param file [description]
 */
void split(const std::string &fn, std::string *dir, std::string *file) {
  LOG_DEBUG("[IN]%s\n", fn.c_str());
  int pos = fn.size() - 1;
  while ('/' == fn[pos]) --pos;
  size_t fstart = fn.rfind('/', pos);
  *file = fn.substr(fstart + 1, pos - fstart);

  pos = fstart;
  while (pos >= 0 && '/' == fn[pos]) --pos;

  if (pos < 0) {
    *dir = "/";
  } else {
    size_t dstart = fn.rfind('/', pos);
    *dir = fn.substr(dstart + 1, pos - dstart);
    *dir = std::string("/") + *dir;
  }

  LOG_DEBUG("[OUT]%s | %s\n", dir->c_str(), file->c_str());
}

// A file abstraction for reading sequentially through a file
class LibradosSequentialFile : public SequentialFile {
  librados::IoCtx * _io_ctx;
  std::string _fid;
  std::string _hint;
  int _offset;
public:
  LibradosSequentialFile(librados::IoCtx * io_ctx, std::string fid, std::string hint):
    _io_ctx(io_ctx), _fid(fid), _hint(hint), _offset(0) {}

  ~LibradosSequentialFile() {}

  /**
   * @brief read file
   * @details
   *  Read up to "n" bytes from the file.  "scratch[0..n-1]" may be
   *  written by this routine.  Sets "*result" to the data that was
   *  read (including if fewer than "n" bytes were successfully read).
   *  May set "*result" to point at data in "scratch[0..n-1]", so
   *  "scratch[0..n-1]" must be live when "*result" is used.
   *  If an error was encountered, returns a non-OK status.
   *
   *  REQUIRES: External synchronization
   *
   * @param n [description]
   * @param result [description]
   * @param scratch [description]
   * @return [description]
   */
  Status Read(size_t n, Slice* result, char* scratch) {
    LOG_DEBUG("[IN]%i\n", (int)n);
    librados::bufferlist buffer;
    Status s;
    int r = _io_ctx->read(_fid, buffer, n, _offset);
    if (r >= 0) {
      buffer.copy(0, r, scratch);
      *result = Slice(scratch, r);
      _offset += r;
      s = Status::OK();
    } else {
      s = err_to_status(r);
      if (s == Status::IOError()) {
        *result = Slice();
        s = Status::OK();
      }
    }
    LOG_DEBUG("[OUT]%s, %i, %s\n", s.ToString().c_str(), (int)r, buffer.c_str());
    return s;
  }

  /**
   * @brief skip "n" bytes from the file
   * @details
   *  Skip "n" bytes from the file. This is guaranteed to be no
   *  slower that reading the same data, but may be faster.
   *
   *  If end of file is reached, skipping will stop at the end of the
   *  file, and Skip will return OK.
   *
   *  REQUIRES: External synchronization
   *
   * @param n [description]
   * @return [description]
   */
  Status Skip(uint64_t n) {
    _offset += n;
    return Status::OK();
  }

  /**
   * @brief noop
   * @details
   *  rocksdb has it's own caching capabilities that we should be able to use,
   *  without relying on a cache here. This can safely be a no-op.
   *
   * @param offset [description]
   * @param length [description]
   *
   * @return [description]
   */
  Status InvalidateCache(size_t offset, size_t length) {
    return Status::OK();
  }
};

// A file abstraction for randomly reading the contents of a file.
class LibradosRandomAccessFile : public RandomAccessFile {
  librados::IoCtx * _io_ctx;
  std::string _fid;
  std::string _hint;
public:
  LibradosRandomAccessFile(librados::IoCtx * io_ctx, std::string fid, std::string hint):
    _io_ctx(io_ctx), _fid(fid), _hint(hint) {}

  ~LibradosRandomAccessFile() {}

  /**
   * @brief read file
   * @details similar to LibradosSequentialFile::Read
   *
   * @param offset [description]
   * @param n [description]
   * @param result [description]
   * @param scratch [description]
   * @return [description]
   */
  Status Read(uint64_t offset, size_t n, Slice* result,
              char* scratch) const {
    LOG_DEBUG("[IN]%i\n", (int)n);
    librados::bufferlist buffer;
    Status s;
    int r = _io_ctx->read(_fid, buffer, n, offset);
    if (r >= 0) {
      buffer.copy(0, r, scratch);
      *result = Slice(scratch, r);
      s = Status::OK();
    } else {
      s = err_to_status(r);
      if (s == Status::IOError()) {
        *result = Slice();
        s = Status::OK();
      }
    }
    LOG_DEBUG("[OUT]%s, %i, %s\n", s.ToString().c_str(), (int)r, buffer.c_str());
    return s;
  }

  /**
   * @brief [brief description]
   * @details Get unique id for each file and guarantee this id is different for each file
   *
   * @param id [description]
   * @param max_size max size of id, it shoud be larger than 16
   *
   * @return [description]
   */
  size_t GetUniqueId(char* id, size_t max_size) const {
    // All fid has the same db_id prefix, so we need to ignore db_id prefix
    size_t s = std::min(max_size, _fid.size());
    strncpy(id, _fid.c_str() + (_fid.size() - s), s);
    id[s - 1] = '\0';
    return s;
  };

  //enum AccessPattern { NORMAL, RANDOM, SEQUENTIAL, WILLNEED, DONTNEED };
  void Hint(AccessPattern pattern) {
    /* Do nothing */
  }

  /**
   * @brief noop
   * @details [long description]
   *
   * @param offset [description]
   * @param length [description]
   *
   * @return [description]
   */
  Status InvalidateCache(size_t offset, size_t length) {
    return Status::OK();
  }
};


// A file abstraction for sequential writing.  The implementation
// must provide buffering since callers may append small fragments
// at a time to the file.
class LibradosWritableFile : public WritableFile {
  librados::IoCtx * _io_ctx;
  std::string _fid;
  std::string _hint;
  const EnvLibrados * const _env;

  std::mutex _mutex;                 // used to protect modification of all following variables
  librados::bufferlist _buffer;      // write buffer
  uint64_t _buffer_size;             // write buffer size
  uint64_t _file_size;               // this file size doesn't include buffer size

  /**
   * @brief assuming caller holds lock
   * @details [long description]
   * @return [description]
   */
  int _SyncLocked() {
    // 1. sync append data to RADOS
    int r = _io_ctx->append(_fid, _buffer, _buffer_size);
    assert(r >= 0);

    // 2. update local variables
    if (0 == r) {
      _buffer.clear();
      _file_size += _buffer_size;
      _buffer_size = 0;
    }

    return r;
  }

public:
  LibradosWritableFile(librados::IoCtx * io_ctx,
                       std::string fid,
                       std::string hint,
                       const EnvLibrados * const env)
    : _io_ctx(io_ctx), _fid(fid), _hint(hint), _env(env), _buffer(), _buffer_size(0), _file_size(0) {
    int ret = _io_ctx->stat(_fid, &_file_size, nullptr);

    // if file not exist
    if (ret < 0) {
      _file_size = 0;
    }
  }

  ~LibradosWritableFile() {
    // sync before closeing writable file
    Sync();
  }

  /**
   * @brief append data to file
   * @details
   *  Append will save all written data in buffer util buffer size
   *  reaches buffer max size. Then, it will write buffer into rados
   *
   * @param data [description]
   * @return [description]
   */
  Status Append(const Slice& data) {
    // append buffer
    LOG_DEBUG("[IN] %i | %s\n", (int)data.size(), data.data());
    int r = 0;

    std::lock_guard<std::mutex> lock(_mutex);
    _buffer.append(data.data(), data.size());
    _buffer_size += data.size();

    if (_buffer_size > _env->_write_buffer_size) {
      r = _SyncLocked();
    }

    LOG_DEBUG("[OUT] %i\n", r);
    return err_to_status(r);
  }

  /**
   * @brief not supported
   * @details [long description]
   * @return [description]
   */
  Status PositionedAppend(
    const Slice& /* data */,
    uint64_t /* offset */) {
    return Status::NotSupported();
  }

  /**
   * @brief truncate file to assigned size
   * @details [long description]
   *
   * @param size [description]
   * @return [description]
   */
  Status Truncate(uint64_t size) {
    LOG_DEBUG("[IN]%lld|%lld|%lld\n", (long long)size, (long long)_file_size, (long long)_buffer_size);
    int r = 0;

    std::lock_guard<std::mutex> lock(_mutex);
    if (_file_size > size) {
      r = _io_ctx->trunc(_fid, size);

      if (r == 0) {
        _buffer.clear();
        _buffer_size = 0;
        _file_size = size;
      }
    } else if (_file_size == size) {
      _buffer.clear();
      _buffer_size = 0;
    } else {
      librados::bufferlist tmp;
      tmp.claim(_buffer);
      _buffer.substr_of(tmp, 0, size - _file_size);
      _buffer_size = size - _file_size;
    }

    LOG_DEBUG("[OUT] %i\n", r);
    return err_to_status(r);
  }

  /**
   * @brief close file
   * @details [long description]
   * @return [description]
   */
  Status Close() {
    LOG_DEBUG("%s | %lld | %lld\n", _hint.c_str(), (long long)_buffer_size, (long long)_file_size);
    return Sync();
  }

  /**
   * @brief flush file,
   * @details initiate an aio write and not wait
   *
   * @return [description]
   */
  Status Flush() {
    librados::AioCompletion *write_completion = librados::Rados::aio_create_completion();
    int r = 0;

    std::lock_guard<std::mutex> lock(_mutex);
    r = _io_ctx->aio_append(_fid, write_completion, _buffer, _buffer_size);

    if (0 == r) {
      _file_size += _buffer_size;
      _buffer.clear();
      _buffer_size = 0;
    }

    write_completion->release();

    return err_to_status(r);
  }

  /**
   * @brief write buffer data to rados
   * @details initiate an aio write and wait for result
   * @return [description]
   */
  Status Sync() { // sync data
    int r = 0;

    std::lock_guard<std::mutex> lock(_mutex);
    if (_buffer_size > 0) {
      r = _SyncLocked();
    }

    return err_to_status(r);
  }

  /**
   * @brief [brief description]
   * @details [long description]
   * @return true if Sync() and Fsync() are safe to call concurrently with Append()and Flush().
   */
  bool IsSyncThreadSafe() const {
    return true;
  }

  /**
   * @brief Indicates the upper layers if the current WritableFile implementation uses direct IO.
   * @details [long description]
   * @return [description]
   */
  bool use_direct_io() const {
    return false;
  }

  /**
   * @brief Get file size
   * @details
   *  This API will use cached file_size.
   * @return [description]
   */
  uint64_t GetFileSize() {
    LOG_DEBUG("%lld|%lld\n", (long long)_buffer_size, (long long)_file_size);

    std::lock_guard<std::mutex> lock(_mutex);
    int file_size = _file_size + _buffer_size;

    return file_size;
  }

  /**
   * @brief For documentation, refer to RandomAccessFile::GetUniqueId()
   * @details [long description]
   *
   * @param id [description]
   * @param max_size [description]
   *
   * @return [description]
   */
  size_t GetUniqueId(char* id, size_t max_size) const {
    // All fid has the same db_id prefix, so we need to ignore db_id prefix
    size_t s = std::min(max_size, _fid.size());
    strncpy(id, _fid.c_str() + (_fid.size() - s), s);
    id[s - 1] = '\0';
    return s;
  }

  /**
   * @brief noop
   * @details [long description]
   *
   * @param offset [description]
   * @param length [description]
   *
   * @return [description]
   */
  Status InvalidateCache(size_t offset, size_t length) {
    return Status::OK();
  }

  using WritableFile::RangeSync;
  /**
   * @brief No RangeSync support, just call Sync()
   * @details [long description]
   *
   * @param offset [description]
   * @param nbytes [description]
   *
   * @return [description]
   */
  Status RangeSync(off_t offset, off_t nbytes) {
    return Sync();
  }

protected:
  using WritableFile::Allocate;
  /**
   * @brief noop
   * @details [long description]
   *
   * @param offset [description]
   * @param len [description]
   *
   * @return [description]
   */
  Status Allocate(off_t offset, off_t len) {
    return Status::OK();
  }
};


// Directory object represents collection of files and implements
// filesystem operations that can be executed on directories.
class LibradosDirectory : public Directory {
  librados::IoCtx * _io_ctx;
  std::string _fid;
public:
  explicit LibradosDirectory(librados::IoCtx * io_ctx, std::string fid):
    _io_ctx(io_ctx), _fid(fid) {}

  // Fsync directory. Can be called concurrently from multiple threads.
  Status Fsync() {
    return Status::OK();
  }
};

// Identifies a locked file.
// This is exclusive lock and can't nested lock by same thread
class LibradosFileLock : public FileLock {
  librados::IoCtx * _io_ctx;
  const std::string _obj_name;
  const std::string _lock_name;
  const std::string _cookie;
  int lock_state;
public:
  LibradosFileLock(
    librados::IoCtx * io_ctx,
    const std::string obj_name):
    _io_ctx(io_ctx),
    _obj_name(obj_name),
    _lock_name("lock_name"),
    _cookie("cookie") {

    // TODO: the lock will never expire. It may cause problem if the process crash or abnormally exit.
    while (!_io_ctx->lock_exclusive(
             _obj_name,
             _lock_name,
             _cookie,
             "description", nullptr, 0));
  }

  ~LibradosFileLock() {
    _io_ctx->unlock(_obj_name, _lock_name, _cookie);
  }
};


// --------------------
// --- EnvLibrados ----
// --------------------
/**
 * @brief EnvLibrados ctor
 * @details [long description]
 *
 * @param db_name unique database name
 * @param config_path the configure file path for rados
 */
EnvLibrados::EnvLibrados(const std::string& db_name,
                         const std::string& config_path,
                         const std::string& db_pool)
  : EnvLibrados("client.admin",
                "ceph",
                0,
                db_name,
                config_path,
                db_pool,
                "/wal",
                db_pool,
                1 << 20) {}

/**
 * @brief EnvLibrados ctor
 * @details [long description]
 *
 * @param client_name       first 3 parameters is for RADOS client init
 * @param cluster_name
 * @param flags
 * @param db_name           unique database name, used as db_id key
 * @param config_path the   configure file path for rados
 * @param db_pool the pool  for db data
 * @param wal_pool the pool for WAL data
 * @param write_buffer_size WritableFile buffer max size
 */
EnvLibrados::EnvLibrados(const std::string& client_name,
                         const std::string& cluster_name,
                         const uint64_t flags,
                         const std::string& db_name,
                         const std::string& config_path,
                         const std::string& db_pool,
                         const std::string& wal_dir,
                         const std::string& wal_pool,
                         const uint64_t write_buffer_size)
  : EnvWrapper(Env::Default()),
    _client_name(client_name),
    _cluster_name(cluster_name),
    _flags(flags),
    _db_name(db_name),
    _config_path(config_path),
    _db_pool_name(db_pool),
    _wal_dir(wal_dir),
    _wal_pool_name(wal_pool),
    _write_buffer_size(write_buffer_size) {
  int ret = 0;

  // 1. create a Rados object and initialize it
  ret = _rados.init2(_client_name.c_str(), _cluster_name.c_str(), _flags); // just use the client.admin keyring
  if (ret < 0) { // let's handle any error that might have come back
    std::cerr << "couldn't initialize rados! error " << ret << std::endl;
    ret = EXIT_FAILURE;
    goto out;
  }

  // 2. read configure file
  ret = _rados.conf_read_file(_config_path.c_str());
  if (ret < 0) {
    // This could fail if the config file is malformed, but it'd be hard.
    std::cerr << "failed to parse config file " << _config_path
              << "! error" << ret << std::endl;
    ret = EXIT_FAILURE;
    goto out;
  }

  // 3. we actually connect to the cluster
  ret = _rados.connect();
  if (ret < 0) {
    std::cerr << "couldn't connect to cluster! error " << ret << std::endl;
    ret = EXIT_FAILURE;
    goto out;
  }

  // 4. create db_pool if not exist
  ret = _rados.pool_create(_db_pool_name.c_str());
  if (ret < 0 && ret != -EEXIST && ret !=  -EPERM) {
    std::cerr << "couldn't create pool! error " << ret << std::endl;
    goto out;
  }

  // 5. create db_pool_ioctx
  ret = _rados.ioctx_create(_db_pool_name.c_str(), _db_pool_ioctx);
  if (ret < 0) {
    std::cerr << "couldn't set up ioctx! error " << ret << std::endl;
    ret = EXIT_FAILURE;
    goto out;
  }

  // 6. create wal_pool if not exist
  ret = _rados.pool_create(_wal_pool_name.c_str());
  if (ret < 0 && ret != -EEXIST && ret !=  -EPERM) {
    std::cerr << "couldn't create pool! error " << ret << std::endl;
    goto out;
  }

  // 7. create wal_pool_ioctx
  ret = _rados.ioctx_create(_wal_pool_name.c_str(), _wal_pool_ioctx);
  if (ret < 0) {
    std::cerr << "couldn't set up ioctx! error " << ret << std::endl;
    ret = EXIT_FAILURE;
    goto out;
  }

  // 8. add root dir
  _AddFid(ROOT_DIR_KEY, DIR_ID_VALUE);

out:
  LOG_DEBUG("rados connect result code : %i\n", ret);
}

/****************************************************
  private functions to handle fid operation.
  Dir also have fid, but the value is DIR_ID_VALUE
****************************************************/

/**
 * @brief generate a new fid
 * @details [long description]
 * @return [description]
 */
std::string EnvLibrados::_CreateFid() {
  return _db_name + "." + GenerateUniqueId();
}

/**
 * @brief get fid
 * @details [long description]
 *
 * @param fname [description]
 * @param fid [description]
 *
 * @return
 *  Status::OK()
 *  Status::NotFound()
 */
Status EnvLibrados::_GetFid(
  const std::string &fname,
  std::string& fid) {
  std::set<std::string> keys;
  std::map<std::string, librados::bufferlist> kvs;
  keys.insert(fname);
  int r = _db_pool_ioctx.omap_get_vals_by_keys(_db_name, keys, &kvs);

  if (0 == r && 0 == kvs.size()) {
    return Status::NotFound();
  } else if (0 == r && 0 != kvs.size()) {
    fid.assign(kvs[fname].c_str(), kvs[fname].length());
    return Status::OK();
  } else {
    return err_to_status(r);
  }
}

/**
 * @brief rename fid
 * @details Only modify object in rados once,
 * so this rename operation is atomic in term of rados
 *
 * @param old_fname [description]
 * @param new_fname [description]
 *
 * @return [description]
 */
Status EnvLibrados::_RenameFid(const std::string& old_fname,
                               const std::string& new_fname) {
  std::string fid;
  Status s = _GetFid(old_fname, fid);

  if (Status::OK() != s) {
    return s;
  }

  librados::bufferlist bl;
  std::set<std::string> keys;
  std::map<std::string, librados::bufferlist> kvs;
  librados::ObjectWriteOperation o;
  bl.append(fid);
  keys.insert(old_fname);
  kvs[new_fname] = bl;
  o.omap_rm_keys(keys);
  o.omap_set(kvs);
  int r = _db_pool_ioctx.operate(_db_name, &o);
  return err_to_status(r);
}

/**
 * @brief add <file path, fid> to metadata object. It may overwrite exist key.
 * @details [long description]
 *
 * @param fname [description]
 * @param fid [description]
 *
 * @return [description]
 */
Status EnvLibrados::_AddFid(
  const std::string& fname,
  const std::string& fid) {
  std::map<std::string, librados::bufferlist> kvs;
  librados::bufferlist value;
  value.append(fid);
  kvs[fname] = value;
  int r = _db_pool_ioctx.omap_set(_db_name, kvs);
  return err_to_status(r);
}

/**
 * @brief return subfile names of dir.
 * @details
 *  RocksDB has a 2-level structure, so all keys
 *  that have dir as prefix are subfiles of dir.
 *  So we can just return these files' name.
 *
 * @param dir [description]
 * @param result [description]
 *
 * @return [description]
 */
Status EnvLibrados::_GetSubFnames(
  const std::string& dir,
  std::vector<std::string> * result
) {
  std::string start_after(dir);
  std::string filter_prefix(dir);
  std::map<std::string, librados::bufferlist> kvs;
  _db_pool_ioctx.omap_get_vals(_db_name,
                               start_after, filter_prefix,
                               MAX_ITEMS_IN_FS, &kvs);

  result->clear();
  for (auto i = kvs.begin(); i != kvs.end(); i++) {
    result->push_back(i->first.substr(dir.size() + 1));
  }
  return Status::OK();
}

/**
 * @brief delete key fname from metadata object
 * @details [long description]
 *
 * @param fname [description]
 * @return [description]
 */
Status EnvLibrados::_DelFid(
  const std::string& fname) {
  std::set<std::string> keys;
  keys.insert(fname);
  int r = _db_pool_ioctx.omap_rm_keys(_db_name, keys);
  return err_to_status(r);
}

/**
 * @brief get match IoCtx from _prefix_pool_map
 * @details [long description]
 *
 * @param prefix [description]
 * @return [description]
 *
 */
librados::IoCtx* EnvLibrados::_GetIoctx(const std::string& fpath) {
  auto is_prefix = [](const std::string & s1, const std::string & s2) {
    auto it1 = s1.begin(), it2 = s2.begin();
    while (it1 != s1.end() && it2 != s2.end() && *it1 == *it2) ++it1, ++it2;
    return it1 == s1.end();
  };

  if (is_prefix(_wal_dir, fpath)) {
    return &_wal_pool_ioctx;
  } else {
    return &_db_pool_ioctx;
  }
}

/************************************************************
                public functions
************************************************************/
/**
 * @brief generate unique id
 * @details Combine system time and random number.
 * @return [description]
 */
std::string EnvLibrados::GenerateUniqueId() {
  Random64 r(time(nullptr));
  uint64_t random_uuid_portion =
    r.Uniform(std::numeric_limits<uint64_t>::max());
  uint64_t nanos_uuid_portion = NowNanos();
  char uuid2[200];
  snprintf(uuid2,
           200,
           "%16lx-%16lx",
           (unsigned long)nanos_uuid_portion,
           (unsigned long)random_uuid_portion);
  return uuid2;
}

/**
 * @brief create a new sequential read file handler
 * @details it will check the existence of fname
 *
 * @param fname [description]
 * @param result [description]
 * @param options [description]
 * @return [description]
 */
Status EnvLibrados::NewSequentialFile(
  const std::string& fname,
  std::unique_ptr<SequentialFile>* result,
  const EnvOptions& options)
{
  LOG_DEBUG("[IN]%s\n", fname.c_str());
  std::string dir, file, fid;
  split(fname, &dir, &file);
  Status s;
  std::string fpath = dir + "/" + file;
  do {
    s = _GetFid(dir, fid);

    if (!s.ok() || fid != DIR_ID_VALUE) {
      if (fid != DIR_ID_VALUE) s = Status::IOError();
      break;
    }

    s = _GetFid(fpath, fid);

    if (Status::NotFound() == s) {
      s = Status::IOError();
      errno = ENOENT;
      break;
    }

    result->reset(new LibradosSequentialFile(_GetIoctx(fpath), fid, fpath));
    s = Status::OK();
  } while (0);

  LOG_DEBUG("[OUT]%s\n", s.ToString().c_str());
  return s;
}

/**
 * @brief create a new random access file handler
 * @details it will check the existence of fname
 *
 * @param fname [description]
 * @param result [description]
 * @param options [description]
 * @return [description]
 */
Status EnvLibrados::NewRandomAccessFile(
  const std::string& fname,
  std::unique_ptr<RandomAccessFile>* result,
  const EnvOptions& options)
{
  LOG_DEBUG("[IN]%s\n", fname.c_str());
  std::string dir, file, fid;
  split(fname, &dir, &file);
  Status s;
  std::string fpath = dir + "/" + file;
  do {
    s = _GetFid(dir, fid);

    if (!s.ok() || fid != DIR_ID_VALUE) {
      s = Status::IOError();
      break;
    }

    s = _GetFid(fpath, fid);

    if (Status::NotFound() == s) {
      s = Status::IOError();
      errno = ENOENT;
      break;
    }

    result->reset(new LibradosRandomAccessFile(_GetIoctx(fpath), fid, fpath));
    s = Status::OK();
  } while (0);

  LOG_DEBUG("[OUT]%s\n", s.ToString().c_str());
  return s;
}

/**
 * @brief create a new write file handler
 * @details it will check the existence of fname
 *
 * @param fname [description]
 * @param result [description]
 * @param options [description]
 * @return [description]
 */
Status EnvLibrados::NewWritableFile(
  const std::string& fname,
  std::unique_ptr<WritableFile>* result,
  const EnvOptions& options)
{
  LOG_DEBUG("[IN]%s\n", fname.c_str());
  std::string dir, file, fid;
  split(fname, &dir, &file);
  Status s;
  std::string fpath = dir + "/" + file;

  do {
    // 1. check if dir exist
    s = _GetFid(dir, fid);
    if (!s.ok()) {
      break;
    }

    if (fid != DIR_ID_VALUE) {
      s = Status::IOError();
      break;
    }

    // 2. check if file exist.
    // 2.1 exist, use it
    // 2.2 not exist, create it
    s = _GetFid(fpath, fid);
    if (Status::NotFound() == s) {
      fid = _CreateFid();
      _AddFid(fpath, fid);
    }

    result->reset(new LibradosWritableFile(_GetIoctx(fpath), fid, fpath, this));
    s = Status::OK();
  } while (0);

  LOG_DEBUG("[OUT]%s\n", s.ToString().c_str());
  return s;
}

/**
 * @brief reuse write file handler
 * @details
 *  This function will rename old_fname to new_fname,
 *  then return the handler of new_fname
 *
 * @param new_fname [description]
 * @param old_fname [description]
 * @param result [description]
 * @param options [description]
 * @return [description]
 */
Status EnvLibrados::ReuseWritableFile(
  const std::string& new_fname,
  const std::string& old_fname,
  std::unique_ptr<WritableFile>* result,
  const EnvOptions& options)
{
  LOG_DEBUG("[IN]%s => %s\n", old_fname.c_str(), new_fname.c_str());
  std::string src_fid, tmp_fid, src_dir, src_file, dst_dir, dst_file;
  split(old_fname, &src_dir, &src_file);
  split(new_fname, &dst_dir, &dst_file);

  std::string src_fpath = src_dir + "/" + src_file;
  std::string dst_fpath = dst_dir + "/" + dst_file;
  Status r = Status::OK();
  do {
    r = _RenameFid(src_fpath,
                   dst_fpath);
    if (!r.ok()) {
      break;
    }

    result->reset(new LibradosWritableFile(_GetIoctx(dst_fpath), src_fid, dst_fpath, this));
  } while (0);

  LOG_DEBUG("[OUT]%s\n", r.ToString().c_str());
  return r;
}

/**
 * @brief create a new directory handler
 * @details [long description]
 *
 * @param name [description]
 * @param result [description]
 *
 * @return [description]
 */
Status EnvLibrados::NewDirectory(
  const std::string& name,
  std::unique_ptr<Directory>* result)
{
  LOG_DEBUG("[IN]%s\n", name.c_str());
  std::string fid, dir, file;
  /* just want to get dir name */
  split(name + "/tmp", &dir, &file);
  Status s;

  do {
    s = _GetFid(dir, fid);

    if (!s.ok() || DIR_ID_VALUE != fid) {
      s = Status::IOError(name, strerror(-ENOENT));
      break;
    }

    if (Status::NotFound() == s) {
      s = _AddFid(dir, DIR_ID_VALUE);
      if (!s.ok()) break;
    } else if (!s.ok()) {
      break;
    }

    result->reset(new LibradosDirectory(_GetIoctx(dir), dir));
    s = Status::OK();
  } while (0);

  LOG_DEBUG("[OUT]%s\n", s.ToString().c_str());
  return s;
}

/**
 * @brief check if fname is exist
 * @details [long description]
 *
 * @param fname [description]
 * @return [description]
 */
Status EnvLibrados::FileExists(const std::string& fname)
{
  LOG_DEBUG("[IN]%s\n", fname.c_str());
  std::string fid, dir, file;
  split(fname, &dir, &file);
  Status s = _GetFid(dir + "/" + file, fid);

  if (s.ok() && fid != DIR_ID_VALUE) {
    s = Status::OK();
  }

  LOG_DEBUG("[OUT]%s\n", s.ToString().c_str());
  return s;
}

/**
 * @brief get subfile name of dir_in
 * @details [long description]
 *
 * @param dir_in [description]
 * @param result [description]
 *
 * @return [description]
 */
Status EnvLibrados::GetChildren(
  const std::string& dir_in,
  std::vector<std::string>* result)
{
  LOG_DEBUG("[IN]%s\n", dir_in.c_str());
  std::string fid, dir, file;
  split(dir_in + "/temp", &dir, &file);
  Status s;

  do {
    s = _GetFid(dir, fid);
    if (!s.ok()) {
      break;
    }

    if (fid != DIR_ID_VALUE) {
      s = Status::IOError();
      break;
    }

    s = _GetSubFnames(dir, result);
  } while (0);

  LOG_DEBUG("[OUT]%s\n", s.ToString().c_str());
  return s;
}

/**
 * @brief delete fname
 * @details [long description]
 *
 * @param fname [description]
 * @return [description]
 */
Status EnvLibrados::DeleteFile(const std::string& fname)
{
  LOG_DEBUG("[IN]%s\n", fname.c_str());
  std::string fid, dir, file;
  split(fname, &dir, &file);
  Status s = _GetFid(dir + "/" + file, fid);

  if (s.ok() && DIR_ID_VALUE != fid) {
    s = _DelFid(dir + "/" + file);
  } else {
    s = Status::NotFound();
  }
  LOG_DEBUG("[OUT]%s\n", s.ToString().c_str());
  return s;
}

/**
 * @brief create new dir
 * @details [long description]
 *
 * @param dirname [description]
 * @return [description]
 */
Status EnvLibrados::CreateDir(const std::string& dirname)
{
  LOG_DEBUG("[IN]%s\n", dirname.c_str());
  std::string fid, dir, file;
  split(dirname + "/temp", &dir, &file);
  Status s = _GetFid(dir + "/" + file, fid);

  do {
    if (Status::NotFound() != s && fid != DIR_ID_VALUE) {
      break;
    } else if (Status::OK() == s && fid == DIR_ID_VALUE) {
      break;
    }

    s = _AddFid(dir, DIR_ID_VALUE);
  } while (0);

  LOG_DEBUG("[OUT]%s\n", s.ToString().c_str());
  return s;
}

/**
 * @brief create dir if missing
 * @details [long description]
 *
 * @param dirname [description]
 * @return [description]
 */
Status EnvLibrados::CreateDirIfMissing(const std::string& dirname)
{
  LOG_DEBUG("[IN]%s\n", dirname.c_str());
  std::string fid, dir, file;
  split(dirname + "/temp", &dir, &file);
  Status s = Status::OK();

  do {
    s = _GetFid(dir, fid);
    if (Status::NotFound() != s) {
      break;
    }

    s = _AddFid(dir, DIR_ID_VALUE);
  } while (0);

  LOG_DEBUG("[OUT]%s\n", s.ToString().c_str());
  return s;
}

/**
 * @brief delete dir
 * @details
 *
 * @param dirname [description]
 * @return [description]
 */
Status EnvLibrados::DeleteDir(const std::string& dirname)
{
  LOG_DEBUG("[IN]%s\n", dirname.c_str());
  std::string fid, dir, file;
  split(dirname + "/temp", &dir, &file);
  Status s = Status::OK();

  s = _GetFid(dir, fid);

  if (s.ok() && DIR_ID_VALUE == fid) {
    std::vector<std::string> subs;
    s = _GetSubFnames(dir, &subs);
    // if subfiles exist, can't delete dir
    if (subs.size() > 0) {
      s = Status::IOError();
    } else {
      s = _DelFid(dir);
    }
  } else {
    s = Status::NotFound();
  }

  LOG_DEBUG("[OUT]%s\n", s.ToString().c_str());
  return s;
}

/**
 * @brief return file size
 * @details [long description]
 *
 * @param fname [description]
 * @param file_size [description]
 *
 * @return [description]
 */
Status EnvLibrados::GetFileSize(
  const std::string& fname,
  uint64_t* file_size)
{
  LOG_DEBUG("[IN]%s\n", fname.c_str());
  std::string fid, dir, file;
  split(fname, &dir, &file);
  time_t mtime;
  Status s;

  do {
    std::string fpath = dir + "/" + file;
    s = _GetFid(fpath, fid);

    if (!s.ok()) {
      break;
    }

    int ret = _GetIoctx(fpath)->stat(fid, file_size, &mtime);
    if (ret < 0) {
      LOG_DEBUG("%i\n", ret);
      if (-ENOENT == ret) {
        *file_size = 0;
        s = Status::OK();
      } else {
        s = err_to_status(ret);
      }
    } else {
      s = Status::OK();
    }
  } while (0);

  LOG_DEBUG("[OUT]%s|%lld\n", s.ToString().c_str(), (long long)*file_size);
  return s;
}

/**
 * @brief get file modification time
 * @details [long description]
 *
 * @param fname [description]
 * @param file_mtime [description]
 *
 * @return [description]
 */
Status EnvLibrados::GetFileModificationTime(const std::string& fname,
    uint64_t* file_mtime)
{
  LOG_DEBUG("[IN]%s\n", fname.c_str());
  std::string fid, dir, file;
  split(fname, &dir, &file);
  time_t mtime;
  uint64_t file_size;
  Status s = Status::OK();
  do {
    std::string fpath = dir + "/" + file;
    s = _GetFid(dir + "/" + file, fid);

    if (!s.ok()) {
      break;
    }

    int ret = _GetIoctx(fpath)->stat(fid, &file_size, &mtime);
    if (ret < 0) {
      if (Status::NotFound() == err_to_status(ret)) {
        *file_mtime = static_cast<uint64_t>(mtime);
        s = Status::OK();
      } else {
        s = err_to_status(ret);
      }
    } else {
      s = Status::OK();
    }
  } while (0);

  LOG_DEBUG("[OUT]%s\n", s.ToString().c_str());
  return s;
}

/**
 * @brief rename file
 * @details
 *
 * @param src [description]
 * @param target_in [description]
 *
 * @return [description]
 */
Status EnvLibrados::RenameFile(
  const std::string& src,
  const std::string& target_in)
{
  LOG_DEBUG("[IN]%s => %s\n", src.c_str(), target_in.c_str());
  std::string src_fid, tmp_fid, src_dir, src_file, dst_dir, dst_file;
  split(src, &src_dir, &src_file);
  split(target_in, &dst_dir, &dst_file);

  auto s = _RenameFid(src_dir + "/" + src_file,
                      dst_dir + "/" + dst_file);
  LOG_DEBUG("[OUT]%s\n", s.ToString().c_str());
  return s;
}

/**
 * @brief not support
 * @details [long description]
 *
 * @param src [description]
 * @param target_in [description]
 *
 * @return [description]
 */
Status EnvLibrados::LinkFile(
  const std::string& src,
  const std::string& target_in)
{
  LOG_DEBUG("[IO]%s => %s\n", src.c_str(), target_in.c_str());
  return Status::NotSupported();
}

/**
 * @brief lock file. create if missing.
 * @details [long description]
 *
 * It seems that LockFile is used for preventing other instance of RocksDB
 * from opening up the database at the same time. From RocksDB source code,
 * the invokes of LockFile are at following locations:
 *
 *  ./db/db_impl.cc:1159:    s = env_->LockFile(LockFileName(dbname_), &db_lock_);    // DBImpl::Recover
 *  ./db/db_impl.cc:5839:  Status result = env->LockFile(lockname, &lock);            // Status DestroyDB
 *
 * When db recovery and db destroy, RocksDB will call LockFile
 *
 * @param fname [description]
 * @param lock [description]
 *
 * @return [description]
 */
Status EnvLibrados::LockFile(
  const std::string& fname,
  FileLock** lock)
{
  LOG_DEBUG("[IN]%s\n", fname.c_str());
  std::string fid, dir, file;
  split(fname, &dir, &file);
  Status s = Status::OK();

  do {
    std::string fpath = dir + "/" + file;
    s = _GetFid(fpath, fid);

    if (Status::OK() != s &&
        Status::NotFound() != s) {
      break;
    } else if (Status::NotFound() == s) {
      s = _AddFid(fpath, _CreateFid());
      if (!s.ok()) {
        break;
      }
    } else if (Status::OK() == s && DIR_ID_VALUE == fid) {
      s = Status::IOError();
      break;
    }

    *lock = new LibradosFileLock(_GetIoctx(fpath), fpath);
  } while (0);

  LOG_DEBUG("[OUT]%s\n", s.ToString().c_str());
  return s;
}

/**
 * @brief unlock file
 * @details [long description]
 *
 * @param lock [description]
 * @return [description]
 */
Status EnvLibrados::UnlockFile(FileLock* lock)
{
  LOG_DEBUG("[IO]%p\n", lock);
  if (nullptr != lock) {
    delete lock;
  }
  return Status::OK();
}


/**
 * @brief not support
 * @details [long description]
 *
 * @param db_path [description]
 * @param output_path [description]
 *
 * @return [description]
 */
Status EnvLibrados::GetAbsolutePath(
  const std::string& db_path,
  std::string* output_path)
{
  LOG_DEBUG("[IO]%s\n", db_path.c_str());
  return Status::NotSupported();
}

/**
 * @brief Get default EnvLibrados
 * @details [long description]
 * @return [description]
 */
EnvLibrados* EnvLibrados::Default() {
  static EnvLibrados default_env(default_db_name,
                                 std::getenv(default_config_path),
                                 default_pool_name);
  return &default_env;
}
// @lint-ignore TXT4 T25377293 Grandfathered in
}
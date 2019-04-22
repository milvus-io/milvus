//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
#include "db/db_test_util.h"
#include "port/stack_trace.h"
#include "rocksdb/perf_context.h"
#if !defined(ROCKSDB_LITE)
#include "util/sync_point.h"
#endif
#include <iostream>
#include <string>

namespace rocksdb {

class DBEncryptionTest : public DBTestBase {
 public:
  DBEncryptionTest() : DBTestBase("/db_encryption_test") {}
};

#ifndef ROCKSDB_LITE

TEST_F(DBEncryptionTest, CheckEncrypted) {
  ASSERT_OK(Put("foo567", "v1.fetdq"));
  ASSERT_OK(Put("bar123", "v2.dfgkjdfghsd"));
  Close();

  // Open all files and look for the values we've put in there.
  // They should not be found if encrypted, otherwise
  // they should be found.
  std::vector<std::string> fileNames;
  auto status = env_->GetChildren(dbname_, &fileNames);
  ASSERT_OK(status);

  auto defaultEnv = Env::Default();
  int hits = 0;
  for (auto it = fileNames.begin() ; it != fileNames.end(); ++it) {
    if ((*it == "..") || (*it == ".")) {
      continue;
    }
    auto filePath = dbname_ + "/" + *it;
    std::unique_ptr<SequentialFile> seqFile;
    auto envOptions = EnvOptions(CurrentOptions());
    status = defaultEnv->NewSequentialFile(filePath, &seqFile, envOptions);
    ASSERT_OK(status);

    uint64_t fileSize;
    status = defaultEnv->GetFileSize(filePath, &fileSize);
    ASSERT_OK(status);

    std::string scratch;
    scratch.reserve(fileSize);
    Slice data;
    status = seqFile->Read(fileSize, &data, (char*)scratch.data());
    ASSERT_OK(status);

    if (data.ToString().find("foo567") != std::string::npos) {
      hits++; 
      //std::cout << "Hit in " << filePath << "\n";
    }
    if (data.ToString().find("v1.fetdq") != std::string::npos) {
      hits++; 
      //std::cout << "Hit in " << filePath << "\n";
    }
    if (data.ToString().find("bar123") != std::string::npos) {
      hits++; 
      //std::cout << "Hit in " << filePath << "\n";
    }
    if (data.ToString().find("v2.dfgkjdfghsd") != std::string::npos) {
      hits++; 
      //std::cout << "Hit in " << filePath << "\n";
    }
    if (data.ToString().find("dfgk") != std::string::npos) {
      hits++; 
      //std::cout << "Hit in " << filePath << "\n";
    }
  }
  if (encrypted_env_) {
    ASSERT_EQ(hits, 0);
  } else {
    ASSERT_GE(hits, 4);
  }
}

#endif // ROCKSDB_LITE

}  // namespace rocksdb

int main(int argc, char** argv) {
  rocksdb::port::InstallStackTraceHandler();
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}

//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//

#ifndef ROCKSDB_LITE

#include "util/auto_roll_logger.h"
#include <errno.h>
#include <sys/stat.h>
#include <algorithm>
#include <cmath>
#include <fstream>
#include <iostream>
#include <iterator>
#include <string>
#include <thread>
#include <vector>
#include "port/port.h"
#include "rocksdb/db.h"
#include "util/logging.h"
#include "util/sync_point.h"
#include "util/testharness.h"

namespace rocksdb {
namespace {
class NoSleepEnv : public EnvWrapper {
 public:
  NoSleepEnv(Env* base) : EnvWrapper(base) {}
  void SleepForMicroseconds(int micros) override {
    fake_time_ += static_cast<uint64_t>(micros);
  }

  uint64_t NowNanos() override { return fake_time_ * 1000; }

  uint64_t NowMicros() override { return fake_time_; }

 private:
  uint64_t fake_time_ = 6666666666;
};
}  // namespace

class AutoRollLoggerTest : public testing::Test {
 public:
  static void InitTestDb() {
#ifdef OS_WIN
    // Replace all slashes in the path so windows CompSpec does not
    // become confused
    std::string testDir(kTestDir);
    std::replace_if(testDir.begin(), testDir.end(),
                    [](char ch) { return ch == '/'; }, '\\');
    std::string deleteCmd = "if exist " + testDir + " rd /s /q " + testDir;
#else
    std::string deleteCmd = "rm -rf " + kTestDir;
#endif
    ASSERT_TRUE(system(deleteCmd.c_str()) == 0);
    Env::Default()->CreateDir(kTestDir);
  }

  void RollLogFileBySizeTest(AutoRollLogger* logger, size_t log_max_size,
                             const std::string& log_message);
  void RollLogFileByTimeTest(Env*, AutoRollLogger* logger, size_t time,
                             const std::string& log_message);

  static const std::string kSampleMessage;
  static const std::string kTestDir;
  static const std::string kLogFile;
  static Env* default_env;
};

const std::string AutoRollLoggerTest::kSampleMessage(
    "this is the message to be written to the log file!!");
const std::string AutoRollLoggerTest::kTestDir(
    test::PerThreadDBPath("db_log_test"));
const std::string AutoRollLoggerTest::kLogFile(
    test::PerThreadDBPath("db_log_test") + "/LOG");
Env* AutoRollLoggerTest::default_env = Env::Default();

// In this test we only want to Log some simple log message with
// no format. LogMessage() provides such a simple interface and
// avoids the [format-security] warning which occurs when you
// call ROCKS_LOG_INFO(logger, log_message) directly.
namespace {
void LogMessage(Logger* logger, const char* message) {
  ROCKS_LOG_INFO(logger, "%s", message);
}

void LogMessage(const InfoLogLevel log_level, Logger* logger,
                const char* message) {
  Log(log_level, logger, "%s", message);
}
}  // namespace

void AutoRollLoggerTest::RollLogFileBySizeTest(AutoRollLogger* logger,
                                               size_t log_max_size,
                                               const std::string& log_message) {
  logger->SetInfoLogLevel(InfoLogLevel::INFO_LEVEL);
  // measure the size of each message, which is supposed
  // to be equal or greater than log_message.size()
  LogMessage(logger, log_message.c_str());
  size_t message_size = logger->GetLogFileSize();
  size_t current_log_size = message_size;

  // Test the cases when the log file will not be rolled.
  while (current_log_size + message_size < log_max_size) {
    LogMessage(logger, log_message.c_str());
    current_log_size += message_size;
    ASSERT_EQ(current_log_size, logger->GetLogFileSize());
  }

  // Now the log file will be rolled
  LogMessage(logger, log_message.c_str());
  // Since rotation is checked before actual logging, we need to
  // trigger the rotation by logging another message.
  LogMessage(logger, log_message.c_str());

  ASSERT_TRUE(message_size == logger->GetLogFileSize());
}

void AutoRollLoggerTest::RollLogFileByTimeTest(Env* env, AutoRollLogger* logger,
                                               size_t time,
                                               const std::string& log_message) {
  uint64_t expected_ctime;
  uint64_t actual_ctime;

  uint64_t total_log_size;
  EXPECT_OK(env->GetFileSize(kLogFile, &total_log_size));
  expected_ctime = logger->TEST_ctime();
  logger->SetCallNowMicrosEveryNRecords(0);

  // -- Write to the log for several times, which is supposed
  // to be finished before time.
  for (int i = 0; i < 10; ++i) {
    env->SleepForMicroseconds(50000);
    LogMessage(logger, log_message.c_str());
    EXPECT_OK(logger->GetStatus());
    // Make sure we always write to the same log file (by
    // checking the create time);

    actual_ctime = logger->TEST_ctime();

    // Also make sure the log size is increasing.
    EXPECT_EQ(expected_ctime, actual_ctime);
    EXPECT_GT(logger->GetLogFileSize(), total_log_size);
    total_log_size = logger->GetLogFileSize();
  }

  // -- Make the log file expire
  env->SleepForMicroseconds(static_cast<int>(time * 1000000));
  LogMessage(logger, log_message.c_str());

  // At this time, the new log file should be created.
  actual_ctime = logger->TEST_ctime();
  EXPECT_LT(expected_ctime, actual_ctime);
  EXPECT_LT(logger->GetLogFileSize(), total_log_size);
}

TEST_F(AutoRollLoggerTest, RollLogFileBySize) {
    InitTestDb();
    size_t log_max_size = 1024 * 5;

    AutoRollLogger logger(Env::Default(), kTestDir, "", log_max_size, 0);

    RollLogFileBySizeTest(&logger, log_max_size,
                          kSampleMessage + ":RollLogFileBySize");
}

TEST_F(AutoRollLoggerTest, RollLogFileByTime) {
  NoSleepEnv nse(Env::Default());

  size_t time = 2;
  size_t log_size = 1024 * 5;

  InitTestDb();
  // -- Test the existence of file during the server restart.
  ASSERT_EQ(Status::NotFound(), default_env->FileExists(kLogFile));
  AutoRollLogger logger(&nse, kTestDir, "", log_size, time);
  ASSERT_OK(default_env->FileExists(kLogFile));

  RollLogFileByTimeTest(&nse, &logger, time,
                        kSampleMessage + ":RollLogFileByTime");
}

TEST_F(AutoRollLoggerTest, OpenLogFilesMultipleTimesWithOptionLog_max_size) {
  // If only 'log_max_size' options is specified, then every time
  // when rocksdb is restarted, a new empty log file will be created.
  InitTestDb();
  // WORKAROUND:
  // avoid complier's complaint of "comparison between signed
  // and unsigned integer expressions" because literal 0 is
  // treated as "singed".
  size_t kZero = 0;
  size_t log_size = 1024;

  AutoRollLogger* logger = new AutoRollLogger(
    Env::Default(), kTestDir, "", log_size, 0);

  LogMessage(logger, kSampleMessage.c_str());
  ASSERT_GT(logger->GetLogFileSize(), kZero);
  delete logger;

  // reopens the log file and an empty log file will be created.
  logger = new AutoRollLogger(
    Env::Default(), kTestDir, "", log_size, 0);
  ASSERT_EQ(logger->GetLogFileSize(), kZero);
  delete logger;
}

TEST_F(AutoRollLoggerTest, CompositeRollByTimeAndSizeLogger) {
  size_t time = 2, log_max_size = 1024 * 5;

  InitTestDb();

  NoSleepEnv nse(Env::Default());
  AutoRollLogger logger(&nse, kTestDir, "", log_max_size, time);

  // Test the ability to roll by size
  RollLogFileBySizeTest(&logger, log_max_size,
                        kSampleMessage + ":CompositeRollByTimeAndSizeLogger");

  // Test the ability to roll by Time
  RollLogFileByTimeTest(&nse, &logger, time,
                        kSampleMessage + ":CompositeRollByTimeAndSizeLogger");
}

#ifndef OS_WIN
// TODO: does not build for Windows because of PosixLogger use below. Need to
// port
TEST_F(AutoRollLoggerTest, CreateLoggerFromOptions) {
  DBOptions options;
  NoSleepEnv nse(Env::Default());
  std::shared_ptr<Logger> logger;

  // Normal logger
  ASSERT_OK(CreateLoggerFromOptions(kTestDir, options, &logger));
  ASSERT_TRUE(dynamic_cast<PosixLogger*>(logger.get()));

  // Only roll by size
  InitTestDb();
  options.max_log_file_size = 1024;
  ASSERT_OK(CreateLoggerFromOptions(kTestDir, options, &logger));
  AutoRollLogger* auto_roll_logger =
    dynamic_cast<AutoRollLogger*>(logger.get());
  ASSERT_TRUE(auto_roll_logger);
  RollLogFileBySizeTest(
      auto_roll_logger, options.max_log_file_size,
      kSampleMessage + ":CreateLoggerFromOptions - size");

  // Only roll by Time
  options.env = &nse;
  InitTestDb();
  options.max_log_file_size = 0;
  options.log_file_time_to_roll = 2;
  ASSERT_OK(CreateLoggerFromOptions(kTestDir, options, &logger));
  auto_roll_logger =
    dynamic_cast<AutoRollLogger*>(logger.get());
  RollLogFileByTimeTest(&nse, auto_roll_logger, options.log_file_time_to_roll,
                        kSampleMessage + ":CreateLoggerFromOptions - time");

  // roll by both Time and size
  InitTestDb();
  options.max_log_file_size = 1024 * 5;
  options.log_file_time_to_roll = 2;
  ASSERT_OK(CreateLoggerFromOptions(kTestDir, options, &logger));
  auto_roll_logger =
    dynamic_cast<AutoRollLogger*>(logger.get());
  RollLogFileBySizeTest(auto_roll_logger, options.max_log_file_size,
                        kSampleMessage + ":CreateLoggerFromOptions - both");
  RollLogFileByTimeTest(&nse, auto_roll_logger, options.log_file_time_to_roll,
                        kSampleMessage + ":CreateLoggerFromOptions - both");
}

TEST_F(AutoRollLoggerTest, LogFlushWhileRolling) {
  DBOptions options;
  std::shared_ptr<Logger> logger;

  InitTestDb();
  options.max_log_file_size = 1024 * 5;
  ASSERT_OK(CreateLoggerFromOptions(kTestDir, options, &logger));
  AutoRollLogger* auto_roll_logger =
      dynamic_cast<AutoRollLogger*>(logger.get());
  ASSERT_TRUE(auto_roll_logger);
  rocksdb::port::Thread flush_thread;

  // Notes:
  // (1) Need to pin the old logger before beginning the roll, as rolling grabs
  //     the mutex, which would prevent us from accessing the old logger. This
  //     also marks flush_thread with AutoRollLogger::Flush:PinnedLogger.
  // (2) Need to reset logger during PosixLogger::Flush() to exercise a race
  //     condition case, which is executing the flush with the pinned (old)
  //     logger after auto-roll logger has cut over to a new logger.
  // (3) PosixLogger::Flush() happens in both threads but its SyncPoints only
  //     are enabled in flush_thread (the one pinning the old logger).
  rocksdb::SyncPoint::GetInstance()->LoadDependencyAndMarkers(
      {{"AutoRollLogger::Flush:PinnedLogger",
        "AutoRollLoggerTest::LogFlushWhileRolling:PreRollAndPostThreadInit"},
       {"PosixLogger::Flush:Begin1",
        "AutoRollLogger::ResetLogger:BeforeNewLogger"},
       {"AutoRollLogger::ResetLogger:AfterNewLogger",
        "PosixLogger::Flush:Begin2"}},
      {{"AutoRollLogger::Flush:PinnedLogger", "PosixLogger::Flush:Begin1"},
       {"AutoRollLogger::Flush:PinnedLogger", "PosixLogger::Flush:Begin2"}});
  rocksdb::SyncPoint::GetInstance()->EnableProcessing();

  flush_thread = port::Thread ([&]() { auto_roll_logger->Flush(); });
  TEST_SYNC_POINT(
      "AutoRollLoggerTest::LogFlushWhileRolling:PreRollAndPostThreadInit");
  RollLogFileBySizeTest(auto_roll_logger, options.max_log_file_size,
                        kSampleMessage + ":LogFlushWhileRolling");
  flush_thread.join();
  rocksdb::SyncPoint::GetInstance()->DisableProcessing();
}

#endif  // OS_WIN

TEST_F(AutoRollLoggerTest, InfoLogLevel) {
  InitTestDb();

  size_t log_size = 8192;
  size_t log_lines = 0;
  // an extra-scope to force the AutoRollLogger to flush the log file when it
  // becomes out of scope.
  {
    AutoRollLogger logger(Env::Default(), kTestDir, "", log_size, 0);
    for (int log_level = InfoLogLevel::HEADER_LEVEL;
         log_level >= InfoLogLevel::DEBUG_LEVEL; log_level--) {
      logger.SetInfoLogLevel((InfoLogLevel)log_level);
      for (int log_type = InfoLogLevel::DEBUG_LEVEL;
           log_type <= InfoLogLevel::HEADER_LEVEL; log_type++) {
        // log messages with log level smaller than log_level will not be
        // logged.
        LogMessage((InfoLogLevel)log_type, &logger, kSampleMessage.c_str());
      }
      log_lines += InfoLogLevel::HEADER_LEVEL - log_level + 1;
    }
    for (int log_level = InfoLogLevel::HEADER_LEVEL;
         log_level >= InfoLogLevel::DEBUG_LEVEL; log_level--) {
      logger.SetInfoLogLevel((InfoLogLevel)log_level);

      // again, messages with level smaller than log_level will not be logged.
      ROCKS_LOG_HEADER(&logger, "%s", kSampleMessage.c_str());
      ROCKS_LOG_DEBUG(&logger, "%s", kSampleMessage.c_str());
      ROCKS_LOG_INFO(&logger, "%s", kSampleMessage.c_str());
      ROCKS_LOG_WARN(&logger, "%s", kSampleMessage.c_str());
      ROCKS_LOG_ERROR(&logger, "%s", kSampleMessage.c_str());
      ROCKS_LOG_FATAL(&logger, "%s", kSampleMessage.c_str());
      log_lines += InfoLogLevel::HEADER_LEVEL - log_level + 1;
    }
  }
  std::ifstream inFile(AutoRollLoggerTest::kLogFile.c_str());
  size_t lines = std::count(std::istreambuf_iterator<char>(inFile),
                         std::istreambuf_iterator<char>(), '\n');
  ASSERT_EQ(log_lines, lines);
  inFile.close();
}

TEST_F(AutoRollLoggerTest, Close) {
  InitTestDb();

  size_t log_size = 8192;
  size_t log_lines = 0;
  AutoRollLogger logger(Env::Default(), kTestDir, "", log_size, 0);
  for (int log_level = InfoLogLevel::HEADER_LEVEL;
       log_level >= InfoLogLevel::DEBUG_LEVEL; log_level--) {
    logger.SetInfoLogLevel((InfoLogLevel)log_level);
    for (int log_type = InfoLogLevel::DEBUG_LEVEL;
         log_type <= InfoLogLevel::HEADER_LEVEL; log_type++) {
      // log messages with log level smaller than log_level will not be
      // logged.
      LogMessage((InfoLogLevel)log_type, &logger, kSampleMessage.c_str());
    }
    log_lines += InfoLogLevel::HEADER_LEVEL - log_level + 1;
  }
  for (int log_level = InfoLogLevel::HEADER_LEVEL;
       log_level >= InfoLogLevel::DEBUG_LEVEL; log_level--) {
    logger.SetInfoLogLevel((InfoLogLevel)log_level);

    // again, messages with level smaller than log_level will not be logged.
    ROCKS_LOG_HEADER(&logger, "%s", kSampleMessage.c_str());
    ROCKS_LOG_DEBUG(&logger, "%s", kSampleMessage.c_str());
    ROCKS_LOG_INFO(&logger, "%s", kSampleMessage.c_str());
    ROCKS_LOG_WARN(&logger, "%s", kSampleMessage.c_str());
    ROCKS_LOG_ERROR(&logger, "%s", kSampleMessage.c_str());
    ROCKS_LOG_FATAL(&logger, "%s", kSampleMessage.c_str());
    log_lines += InfoLogLevel::HEADER_LEVEL - log_level + 1;
  }
  ASSERT_EQ(logger.Close(), Status::OK());

  std::ifstream inFile(AutoRollLoggerTest::kLogFile.c_str());
  size_t lines = std::count(std::istreambuf_iterator<char>(inFile),
                         std::istreambuf_iterator<char>(), '\n');
  ASSERT_EQ(log_lines, lines);
  inFile.close();
}

// Test the logger Header function for roll over logs
// We expect the new logs creates as roll over to carry the headers specified
static std::vector<std::string> GetOldFileNames(const std::string& path) {
  std::vector<std::string> ret;

  const std::string dirname = path.substr(/*start=*/0, path.find_last_of("/"));
  const std::string fname = path.substr(path.find_last_of("/") + 1);

  std::vector<std::string> children;
  Env::Default()->GetChildren(dirname, &children);

  // We know that the old log files are named [path]<something>
  // Return all entities that match the pattern
  for (auto& child : children) {
    if (fname != child && child.find(fname) == 0) {
      ret.push_back(dirname + "/" + child);
    }
  }

  return ret;
}

// Return the number of lines where a given pattern was found in the file
static size_t GetLinesCount(const std::string& fname,
                            const std::string& pattern) {
  std::stringstream ssbuf;
  std::string line;
  size_t count = 0;

  std::ifstream inFile(fname.c_str());
  ssbuf << inFile.rdbuf();

  while (getline(ssbuf, line)) {
    if (line.find(pattern) != std::string::npos) {
      count++;
    }
  }

  return count;
}

TEST_F(AutoRollLoggerTest, LogHeaderTest) {
  static const size_t MAX_HEADERS = 10;
  static const size_t LOG_MAX_SIZE = 1024 * 5;
  static const std::string HEADER_STR = "Log header line";

  // test_num == 0 -> standard call to Header()
  // test_num == 1 -> call to Log() with InfoLogLevel::HEADER_LEVEL
  for (int test_num = 0; test_num < 2; test_num++) {

    InitTestDb();

    AutoRollLogger logger(Env::Default(), kTestDir, /*db_log_dir=*/ "",
                          LOG_MAX_SIZE, /*log_file_time_to_roll=*/ 0);

    if (test_num == 0) {
      // Log some headers explicitly using Header()
      for (size_t i = 0; i < MAX_HEADERS; i++) {
        Header(&logger, "%s %d", HEADER_STR.c_str(), i);
      }
    } else if (test_num == 1) {
      // HEADER_LEVEL should make this behave like calling Header()
      for (size_t i = 0; i < MAX_HEADERS; i++) {
        ROCKS_LOG_HEADER(&logger, "%s %d", HEADER_STR.c_str(), i);
      }
    }

    const std::string newfname = logger.TEST_log_fname();

    // Log enough data to cause a roll over
    int i = 0;
    for (size_t iter = 0; iter < 2; iter++) {
      while (logger.GetLogFileSize() < LOG_MAX_SIZE) {
        Info(&logger, (kSampleMessage + ":LogHeaderTest line %d").c_str(), i);
        ++i;
      }

      Info(&logger, "Rollover");
    }

    // Flush the log for the latest file
    LogFlush(&logger);

    const auto oldfiles = GetOldFileNames(newfname);

    ASSERT_EQ(oldfiles.size(), (size_t) 2);

    for (auto& oldfname : oldfiles) {
      // verify that the files rolled over
      ASSERT_NE(oldfname, newfname);
      // verify that the old log contains all the header logs
      ASSERT_EQ(GetLinesCount(oldfname, HEADER_STR), MAX_HEADERS);
    }
  }
}

TEST_F(AutoRollLoggerTest, LogFileExistence) {
  rocksdb::DB* db;
  rocksdb::Options options;
#ifdef OS_WIN
  // Replace all slashes in the path so windows CompSpec does not
  // become confused
  std::string testDir(kTestDir);
  std::replace_if(testDir.begin(), testDir.end(),
    [](char ch) { return ch == '/'; }, '\\');
  std::string deleteCmd = "if exist " + testDir + " rd /s /q " + testDir;
#else
  std::string deleteCmd = "rm -rf " + kTestDir;
#endif
  ASSERT_EQ(system(deleteCmd.c_str()), 0);
  options.max_log_file_size = 100 * 1024 * 1024;
  options.create_if_missing = true;
  ASSERT_OK(rocksdb::DB::Open(options, kTestDir, &db));
  ASSERT_OK(default_env->FileExists(kLogFile));
  delete db;
}

}  // namespace rocksdb

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}

#else
#include <stdio.h>

int main(int /*argc*/, char** /*argv*/) {
  fprintf(stderr,
          "SKIPPED as AutoRollLogger is not supported in ROCKSDB_LITE\n");
  return 0;
}

#endif  // !ROCKSDB_LITE

//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#ifndef GFLAGS
#include <cstdio>
int main() {
  fprintf(stderr, "Please install gflags to run rocksdb tools\n");
  return 1;
}
#else

#include "monitoring/histogram.h"
#include "rocksdb/env.h"
#include "util/file_reader_writer.h"
#include "util/gflags_compat.h"
#include "util/testharness.h"
#include "util/testutil.h"

using GFLAGS_NAMESPACE::ParseCommandLineFlags;
using GFLAGS_NAMESPACE::SetUsageMessage;

// A simple benchmark to simulate transactional logs

DEFINE_int32(num_records, 6000, "Number of records.");
DEFINE_int32(record_size, 249, "Size of each record.");
DEFINE_int32(record_interval, 10000, "Interval between records (microSec)");
DEFINE_int32(bytes_per_sync, 0, "bytes_per_sync parameter in EnvOptions");
DEFINE_bool(enable_sync, false, "sync after each write.");

namespace rocksdb {
void RunBenchmark() {
  std::string file_name = test::PerThreadDBPath("log_write_benchmark.log");
  Env* env = Env::Default();
  EnvOptions env_options = env->OptimizeForLogWrite(EnvOptions());
  env_options.bytes_per_sync = FLAGS_bytes_per_sync;
  std::unique_ptr<WritableFile> file;
  env->NewWritableFile(file_name, &file, env_options);
  std::unique_ptr<WritableFileWriter> writer;
  writer.reset(new WritableFileWriter(std::move(file), env_options));

  std::string record;
  record.assign(FLAGS_record_size, 'X');

  HistogramImpl hist;

  uint64_t start_time = env->NowMicros();
  for (int i = 0; i < FLAGS_num_records; i++) {
    uint64_t start_nanos = env->NowNanos();
    writer->Append(record);
    writer->Flush();
    if (FLAGS_enable_sync) {
      writer->Sync(false);
    }
    hist.Add(env->NowNanos() - start_nanos);

    if (i % 1000 == 1) {
      fprintf(stderr, "Wrote %d records...\n", i);
    }

    int time_to_sleep =
        (i + 1) * FLAGS_record_interval - (env->NowMicros() - start_time);
    if (time_to_sleep > 0) {
      env->SleepForMicroseconds(time_to_sleep);
    }
  }

  fprintf(stderr, "Distribution of latency of append+flush: \n%s",
          hist.ToString().c_str());
}
}  // namespace rocksdb

int main(int argc, char** argv) {
  SetUsageMessage(std::string("\nUSAGE:\n") + std::string(argv[0]) +
                  " [OPTIONS]...");
  ParseCommandLineFlags(&argc, &argv, true);

  rocksdb::RunBenchmark();
  return 0;
}

#endif  // GFLAGS

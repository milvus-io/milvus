# Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
#  This source code is licensed under both the GPLv2 (found in the
#  COPYING file in the root directory) and Apache 2.0 License
#  (found in the LICENSE.Apache file in the root directory).

from advisor.db_bench_runner import DBBenchRunner
from advisor.db_log_parser import NO_COL_FAMILY, DataSource
from advisor.db_options_parser import DatabaseOptions
import os
import unittest


class TestDBBenchRunnerMethods(unittest.TestCase):
    def setUp(self):
        self.pos_args = [
            './../../db_bench',
            'overwrite',
            'use_existing_db=true',
            'duration=10'
        ]
        self.bench_runner = DBBenchRunner(self.pos_args)
        this_path = os.path.abspath(os.path.dirname(__file__))
        options_path = os.path.join(this_path, 'input_files/OPTIONS-000005')
        self.db_options = DatabaseOptions(options_path)

    def test_setup(self):
        self.assertEqual(self.bench_runner.db_bench_binary, self.pos_args[0])
        self.assertEqual(self.bench_runner.benchmark, self.pos_args[1])
        self.assertSetEqual(
            set(self.bench_runner.db_bench_args), set(self.pos_args[2:])
        )

    def test_get_info_log_file_name(self):
        log_file_name = DBBenchRunner.get_info_log_file_name(
            None, 'random_path'
        )
        self.assertEqual(log_file_name, 'LOG')

        log_file_name = DBBenchRunner.get_info_log_file_name(
            '/dev/shm/', '/tmp/rocksdbtest-155919/dbbench/'
        )
        self.assertEqual(log_file_name, 'tmp_rocksdbtest-155919_dbbench_LOG')

    def test_get_opt_args_str(self):
        misc_opt_dict = {'bloom_bits': 2, 'empty_opt': None, 'rate_limiter': 3}
        optional_args_str = DBBenchRunner.get_opt_args_str(misc_opt_dict)
        self.assertEqual(optional_args_str, ' --bloom_bits=2 --rate_limiter=3')

    def test_get_log_options(self):
        db_path = '/tmp/rocksdb-155919/dbbench'
        # when db_log_dir is present in the db_options
        update_dict = {
            'DBOptions.db_log_dir': {NO_COL_FAMILY: '/dev/shm'},
            'DBOptions.stats_dump_period_sec': {NO_COL_FAMILY: '20'}
        }
        self.db_options.update_options(update_dict)
        log_file_prefix, stats_freq = self.bench_runner.get_log_options(
            self.db_options, db_path
        )
        self.assertEqual(
            log_file_prefix, '/dev/shm/tmp_rocksdb-155919_dbbench_LOG'
        )
        self.assertEqual(stats_freq, 20)

        update_dict = {
            'DBOptions.db_log_dir': {NO_COL_FAMILY: None},
            'DBOptions.stats_dump_period_sec': {NO_COL_FAMILY: '30'}
        }
        self.db_options.update_options(update_dict)
        log_file_prefix, stats_freq = self.bench_runner.get_log_options(
            self.db_options, db_path
        )
        self.assertEqual(log_file_prefix, '/tmp/rocksdb-155919/dbbench/LOG')
        self.assertEqual(stats_freq, 30)

    def test_build_experiment_command(self):
        # add some misc_options to db_options
        update_dict = {
            'bloom_bits': {NO_COL_FAMILY: 2},
            'rate_limiter_bytes_per_sec': {NO_COL_FAMILY: 128000000}
        }
        self.db_options.update_options(update_dict)
        db_path = '/dev/shm'
        experiment_command = self.bench_runner._build_experiment_command(
            self.db_options, db_path
        )
        opt_args_str = DBBenchRunner.get_opt_args_str(
            self.db_options.get_misc_options()
        )
        opt_args_str += (
            ' --options_file=' +
            self.db_options.generate_options_config('12345')
        )
        for arg in self.pos_args[2:]:
            opt_args_str += (' --' + arg)
        expected_command = (
            self.pos_args[0] + ' --benchmarks=' + self.pos_args[1] +
            ' --statistics --perf_level=3 --db=' + db_path + opt_args_str
        )
        self.assertEqual(experiment_command, expected_command)


class TestDBBenchRunner(unittest.TestCase):
    def setUp(self):
        # Note: the db_bench binary should be present in the rocksdb/ directory
        self.pos_args = [
            './../../db_bench',
            'overwrite',
            'use_existing_db=true',
            'duration=20'
        ]
        self.bench_runner = DBBenchRunner(self.pos_args)
        this_path = os.path.abspath(os.path.dirname(__file__))
        options_path = os.path.join(this_path, 'input_files/OPTIONS-000005')
        self.db_options = DatabaseOptions(options_path)

    def test_experiment_output(self):
        update_dict = {'bloom_bits': {NO_COL_FAMILY: 2}}
        self.db_options.update_options(update_dict)
        db_path = '/dev/shm'
        data_sources, throughput = self.bench_runner.run_experiment(
            self.db_options, db_path
        )
        self.assertEqual(
            data_sources[DataSource.Type.DB_OPTIONS][0].type,
            DataSource.Type.DB_OPTIONS
        )
        self.assertEqual(
            data_sources[DataSource.Type.LOG][0].type,
            DataSource.Type.LOG
        )
        self.assertEqual(len(data_sources[DataSource.Type.TIME_SERIES]), 2)
        self.assertEqual(
            data_sources[DataSource.Type.TIME_SERIES][0].type,
            DataSource.Type.TIME_SERIES
        )
        self.assertEqual(
            data_sources[DataSource.Type.TIME_SERIES][1].type,
            DataSource.Type.TIME_SERIES
        )
        self.assertEqual(
            data_sources[DataSource.Type.TIME_SERIES][1].stats_freq_sec, 0
        )


if __name__ == '__main__':
    unittest.main()

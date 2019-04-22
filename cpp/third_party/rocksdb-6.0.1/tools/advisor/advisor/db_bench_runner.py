# Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
#  This source code is licensed under both the GPLv2 (found in the
#  COPYING file in the root directory) and Apache 2.0 License
#  (found in the LICENSE.Apache file in the root directory).

from advisor.bench_runner import BenchmarkRunner
from advisor.db_log_parser import DataSource, DatabaseLogs, NO_COL_FAMILY
from advisor.db_stats_fetcher import (
    LogStatsParser, OdsStatsFetcher, DatabasePerfContext
)
import shutil
import subprocess
import time


'''
NOTE: This is not thread-safe, because the output file is simply overwritten.
'''


class DBBenchRunner(BenchmarkRunner):
    OUTPUT_FILE = "temp/dbbench_out.tmp"
    ERROR_FILE = "temp/dbbench_err.tmp"
    DB_PATH = "DB path"
    THROUGHPUT = "ops/sec"
    PERF_CON = " PERF_CONTEXT:"

    @staticmethod
    def is_metric_better(new_metric, old_metric):
        # for db_bench 'throughput' is the metric returned by run_experiment
        return new_metric >= old_metric

    @staticmethod
    def get_opt_args_str(misc_options_dict):
        # given a dictionary of options and their values, return a string
        # that can be appended as command-line arguments
        optional_args_str = ""
        for option_name, option_value in misc_options_dict.items():
            if option_value:
                optional_args_str += (
                    " --" + option_name + "=" + str(option_value)
                )
        return optional_args_str

    def __init__(self, positional_args, ods_args=None):
        # parse positional_args list appropriately
        self.db_bench_binary = positional_args[0]
        self.benchmark = positional_args[1]
        self.db_bench_args = None
        if len(positional_args) > 2:
            # options list with each option given as "<option>=<value>"
            self.db_bench_args = positional_args[2:]
        # save ods_args, if provided
        self.ods_args = ods_args

    def _parse_output(self, get_perf_context=False):
        '''
        Sample db_bench output after running 'readwhilewriting' benchmark:
        DB path: [/tmp/rocksdbtest-155919/dbbench]\n
        readwhilewriting : 16.582 micros/op 60305 ops/sec; 4.2 MB/s (3433828\
        of 5427999 found)\n
        PERF_CONTEXT:\n
        user_key_comparison_count = 500466712, block_cache_hit_count = ...\n
        '''
        output = {
            self.THROUGHPUT: None, self.DB_PATH: None, self.PERF_CON: None
        }
        perf_context_begins = False
        with open(self.OUTPUT_FILE, 'r') as fp:
            for line in fp:
                if line.startswith(self.benchmark):
                    # line from sample output:
                    # readwhilewriting : 16.582 micros/op 60305 ops/sec; \
                    # 4.2 MB/s (3433828 of 5427999 found)\n
                    print(line)  # print output of the benchmark run
                    token_list = line.strip().split()
                    for ix, token in enumerate(token_list):
                        if token.startswith(self.THROUGHPUT):
                            # in above example, throughput = 60305 ops/sec
                            output[self.THROUGHPUT] = (
                                float(token_list[ix - 1])
                            )
                            break
                elif get_perf_context and line.startswith(self.PERF_CON):
                    # the following lines in the output contain perf context
                    # statistics (refer example above)
                    perf_context_begins = True
                elif get_perf_context and perf_context_begins:
                    # Sample perf_context output:
                    # user_key_comparison_count = 500, block_cache_hit_count =\
                    # 468, block_read_count = 580, block_read_byte = 445, ...
                    token_list = line.strip().split(',')
                    # token_list = ['user_key_comparison_count = 500',
                    # 'block_cache_hit_count = 468','block_read_count = 580'...
                    perf_context = {
                        tk.split('=')[0].strip(): tk.split('=')[1].strip()
                        for tk in token_list
                        if tk
                    }
                    # TODO(poojam23): this is a hack and should be replaced
                    # with the timestamp that db_bench will provide per printed
                    # perf_context
                    timestamp = int(time.time())
                    perf_context_ts = {}
                    for stat in perf_context.keys():
                        perf_context_ts[stat] = {
                            timestamp: int(perf_context[stat])
                        }
                    output[self.PERF_CON] = perf_context_ts
                    perf_context_begins = False
                elif line.startswith(self.DB_PATH):
                    # line from sample output:
                    # DB path: [/tmp/rocksdbtest-155919/dbbench]\n
                    output[self.DB_PATH] = (
                        line.split('[')[1].split(']')[0]
                    )
        return output

    def get_log_options(self, db_options, db_path):
        # get the location of the LOG file and the frequency at which stats are
        # dumped in the LOG file
        log_dir_path = None
        stats_freq_sec = None
        logs_file_prefix = None

        # fetch frequency at which the stats are dumped in the Rocksdb logs
        dump_period = 'DBOptions.stats_dump_period_sec'
        # fetch the directory, if specified, in which the Rocksdb logs are
        # dumped, by default logs are dumped in same location as database
        log_dir = 'DBOptions.db_log_dir'
        log_options = db_options.get_options([dump_period, log_dir])
        if dump_period in log_options:
            stats_freq_sec = int(log_options[dump_period][NO_COL_FAMILY])
        if log_dir in log_options:
            log_dir_path = log_options[log_dir][NO_COL_FAMILY]

        log_file_name = DBBenchRunner.get_info_log_file_name(
            log_dir_path, db_path
        )

        if not log_dir_path:
            log_dir_path = db_path
        if not log_dir_path.endswith('/'):
            log_dir_path += '/'

        logs_file_prefix = log_dir_path + log_file_name
        return (logs_file_prefix, stats_freq_sec)

    def _get_options_command_line_args_str(self, curr_options):
        '''
        This method uses the provided Rocksdb OPTIONS to create a string of
        command-line arguments for db_bench.
        The --options_file argument is always given and the options that are
        not supported by the OPTIONS file are given as separate arguments.
        '''
        optional_args_str = DBBenchRunner.get_opt_args_str(
            curr_options.get_misc_options()
        )
        # generate an options configuration file
        options_file = curr_options.generate_options_config(nonce='12345')
        optional_args_str += " --options_file=" + options_file
        return optional_args_str

    def _setup_db_before_experiment(self, curr_options, db_path):
        # remove destination directory if it already exists
        try:
            shutil.rmtree(db_path, ignore_errors=True)
        except OSError as e:
            print('Error: rmdir ' + e.filename + ' ' + e.strerror)
        # setup database with a million keys using the fillrandom benchmark
        command = "%s --benchmarks=fillrandom --db=%s --num=1000000" % (
            self.db_bench_binary, db_path
        )
        args_str = self._get_options_command_line_args_str(curr_options)
        command += args_str
        self._run_command(command)

    def _build_experiment_command(self, curr_options, db_path):
        command = "%s --benchmarks=%s --statistics --perf_level=3 --db=%s" % (
            self.db_bench_binary, self.benchmark, db_path
        )
        # fetch the command-line arguments string for providing Rocksdb options
        args_str = self._get_options_command_line_args_str(curr_options)
        # handle the command-line args passed in the constructor, these
        # arguments are specific to db_bench
        for cmd_line_arg in self.db_bench_args:
            args_str += (" --" + cmd_line_arg)
        command += args_str
        return command

    def _run_command(self, command):
        out_file = open(self.OUTPUT_FILE, "w+")
        err_file = open(self.ERROR_FILE, "w+")
        print('executing... - ' + command)
        subprocess.call(command, shell=True, stdout=out_file, stderr=err_file)
        out_file.close()
        err_file.close()

    def run_experiment(self, db_options, db_path):
        # setup the Rocksdb database before running experiment
        self._setup_db_before_experiment(db_options, db_path)
        # get the command to run the experiment
        command = self._build_experiment_command(db_options, db_path)
        experiment_start_time = int(time.time())
        # run experiment
        self._run_command(command)
        experiment_end_time = int(time.time())
        # parse the db_bench experiment output
        parsed_output = self._parse_output(get_perf_context=True)

        # get the log files path prefix and frequency at which Rocksdb stats
        # are dumped in the logs
        logs_file_prefix, stats_freq_sec = self.get_log_options(
            db_options, parsed_output[self.DB_PATH]
        )
        # create the Rocksbd LOGS object
        db_logs = DatabaseLogs(
            logs_file_prefix, db_options.get_column_families()
        )
        # Create the Log STATS object
        db_log_stats = LogStatsParser(logs_file_prefix, stats_freq_sec)
        # Create the PerfContext STATS object
        db_perf_context = DatabasePerfContext(
            parsed_output[self.PERF_CON], 0, False
        )
        # create the data-sources dictionary
        data_sources = {
            DataSource.Type.DB_OPTIONS: [db_options],
            DataSource.Type.LOG: [db_logs],
            DataSource.Type.TIME_SERIES: [db_log_stats, db_perf_context]
        }
        # Create the ODS STATS object
        if self.ods_args:
            key_prefix = ''
            if 'key_prefix' in self.ods_args:
                key_prefix = self.ods_args['key_prefix']
            data_sources[DataSource.Type.TIME_SERIES].append(OdsStatsFetcher(
                self.ods_args['client_script'],
                self.ods_args['entity'],
                experiment_start_time,
                experiment_end_time,
                key_prefix
            ))
        # return the experiment's data-sources and throughput
        return data_sources, parsed_output[self.THROUGHPUT]

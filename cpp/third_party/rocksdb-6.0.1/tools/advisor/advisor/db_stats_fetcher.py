# Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
#  This source code is licensed under both the GPLv2 (found in the
#  COPYING file in the root directory) and Apache 2.0 License
#  (found in the LICENSE.Apache file in the root directory).

from advisor.db_log_parser import Log
from advisor.db_timeseries_parser import TimeSeriesData, NO_ENTITY
import copy
import glob
import re
import subprocess
import time


class LogStatsParser(TimeSeriesData):
    STATS = 'STATISTICS:'

    @staticmethod
    def parse_log_line_for_stats(log_line):
        # Example stat line (from LOG file):
        # "rocksdb.db.get.micros P50 : 8.4 P95 : 21.8 P99 : 33.9 P100 : 92.0\n"
        token_list = log_line.strip().split()
        # token_list = ['rocksdb.db.get.micros', 'P50', ':', '8.4', 'P95', ':',
        # '21.8', 'P99', ':', '33.9', 'P100', ':', '92.0']
        stat_prefix = token_list[0] + '.'  # 'rocksdb.db.get.micros.'
        stat_values = [
            token
            for token in token_list[1:]
            if token != ':'
        ]
        # stat_values = ['P50', '8.4', 'P95', '21.8', 'P99', '33.9', 'P100',
        # '92.0']
        stat_dict = {}
        for ix, metric in enumerate(stat_values):
            if ix % 2 == 0:
                stat_name = stat_prefix + metric
                stat_name = stat_name.lower()  # Note: case insensitive names
            else:
                stat_dict[stat_name] = float(metric)
        # stat_dict = {'rocksdb.db.get.micros.p50': 8.4,
        # 'rocksdb.db.get.micros.p95': 21.8, 'rocksdb.db.get.micros.p99': 33.9,
        # 'rocksdb.db.get.micros.p100': 92.0}
        return stat_dict

    def __init__(self, logs_path_prefix, stats_freq_sec):
        super().__init__()
        self.logs_file_prefix = logs_path_prefix
        self.stats_freq_sec = stats_freq_sec
        self.duration_sec = 60

    def get_keys_from_conditions(self, conditions):
        # Note: case insensitive stat names
        reqd_stats = []
        for cond in conditions:
            for key in cond.keys:
                key = key.lower()
                # some keys are prepended with '[]' for OdsStatsFetcher to
                # replace this with the appropriate key_prefix, remove these
                # characters here since the LogStatsParser does not need
                # a prefix
                if key.startswith('[]'):
                    reqd_stats.append(key[2:])
                else:
                    reqd_stats.append(key)
        return reqd_stats

    def add_to_timeseries(self, log, reqd_stats):
        # this method takes in the Log object that contains the Rocksdb stats
        # and a list of required stats, then it parses the stats line by line
        # to fetch required stats and add them to the keys_ts object
        # Example: reqd_stats = ['rocksdb.block.cache.hit.count',
        # 'rocksdb.db.get.micros.p99']
        # Let log.get_message() returns following string:
        # "[WARN] [db/db_impl.cc:485] STATISTICS:\n
        # rocksdb.block.cache.miss COUNT : 1459\n
        # rocksdb.block.cache.hit COUNT : 37\n
        # ...
        # rocksdb.db.get.micros P50 : 15.6 P95 : 39.7 P99 : 62.6 P100 : 148.0\n
        # ..."
        new_lines = log.get_message().split('\n')
        # let log_ts = 1532518219
        log_ts = log.get_timestamp()
        # example updates to keys_ts:
        # keys_ts[NO_ENTITY]['rocksdb.db.get.micros.p99'][1532518219] = 62.6
        # keys_ts[NO_ENTITY]['rocksdb.block.cache.hit.count'][1532518219] = 37
        for line in new_lines[1:]:  # new_lines[0] does not contain any stats
            stats_on_line = self.parse_log_line_for_stats(line)
            for stat in stats_on_line:
                if stat in reqd_stats:
                    if stat not in self.keys_ts[NO_ENTITY]:
                        self.keys_ts[NO_ENTITY][stat] = {}
                    self.keys_ts[NO_ENTITY][stat][log_ts] = stats_on_line[stat]

    def fetch_timeseries(self, reqd_stats):
        # this method parses the Rocksdb LOG file and generates timeseries for
        # each of the statistic in the list reqd_stats
        self.keys_ts = {NO_ENTITY: {}}
        for file_name in glob.glob(self.logs_file_prefix + '*'):
            # TODO(poojam23): find a way to distinguish between 'old' log files
            # from current and previous experiments, present in the same
            # directory
            if re.search('old', file_name, re.IGNORECASE):
                continue
            with open(file_name, 'r') as db_logs:
                new_log = None
                for line in db_logs:
                    if Log.is_new_log(line):
                        if (
                            new_log and
                            re.search(self.STATS, new_log.get_message())
                        ):
                            self.add_to_timeseries(new_log, reqd_stats)
                        new_log = Log(line, column_families=[])
                    else:
                        # To account for logs split into multiple lines
                        new_log.append_message(line)
            # Check for the last log in the file.
            if new_log and re.search(self.STATS, new_log.get_message()):
                self.add_to_timeseries(new_log, reqd_stats)


class DatabasePerfContext(TimeSeriesData):
    # TODO(poojam23): check if any benchrunner provides PerfContext sampled at
    # regular intervals
    def __init__(self, perf_context_ts, stats_freq_sec, cumulative):
        '''
        perf_context_ts is expected to be in the following format:
        Dict[metric, Dict[timestamp, value]], where for
        each (metric, timestamp) pair, the value is database-wide (i.e.
        summed over all the threads involved)
        if stats_freq_sec == 0, per-metric only one value is reported
        '''
        super().__init__()
        self.stats_freq_sec = stats_freq_sec
        self.keys_ts = {NO_ENTITY: perf_context_ts}
        if cumulative:
            self.unaccumulate_metrics()

    def unaccumulate_metrics(self):
        # if the perf context metrics provided are cumulative in nature, this
        # method can be used to convert them to a disjoint format
        epoch_ts = copy.deepcopy(self.keys_ts)
        for stat in self.keys_ts[NO_ENTITY]:
            timeseries = sorted(
                list(self.keys_ts[NO_ENTITY][stat].keys()), reverse=True
            )
            if len(timeseries) < 2:
                continue
            for ix, ts in enumerate(timeseries[:-1]):
                epoch_ts[NO_ENTITY][stat][ts] = (
                    epoch_ts[NO_ENTITY][stat][ts] -
                    epoch_ts[NO_ENTITY][stat][timeseries[ix+1]]
                )
                if epoch_ts[NO_ENTITY][stat][ts] < 0:
                    raise ValueError('DBPerfContext: really cumulative?')
            # drop the smallest timestamp in the timeseries for this metric
            epoch_ts[NO_ENTITY][stat].pop(timeseries[-1])
        self.keys_ts = epoch_ts

    def get_keys_from_conditions(self, conditions):
        reqd_stats = []
        for cond in conditions:
            reqd_stats.extend([key.lower() for key in cond.keys])
        return reqd_stats

    def fetch_timeseries(self, statistics):
        # this method is redundant for DatabasePerfContext because the __init__
        # does the job of populating 'keys_ts'
        pass


class OdsStatsFetcher(TimeSeriesData):
    # class constants
    OUTPUT_FILE = 'temp/stats_out.tmp'
    ERROR_FILE = 'temp/stats_err.tmp'
    RAPIDO_COMMAND = "%s --entity=%s --key=%s --tstart=%s --tend=%s --showtime"

    # static methods
    @staticmethod
    def _get_string_in_quotes(value):
        return '"' + str(value) + '"'

    @staticmethod
    def _get_time_value_pair(pair_string):
        # example pair_string: '[1532544591, 97.3653601828]'
        pair_string = pair_string.replace('[', '')
        pair_string = pair_string.replace(']', '')
        pair = pair_string.split(',')
        first = int(pair[0].strip())
        second = float(pair[1].strip())
        return [first, second]

    @staticmethod
    def _get_ods_cli_stime(start_time):
        diff = int(time.time() - int(start_time))
        stime = str(diff) + '_s'
        return stime

    def __init__(
        self, client, entities, start_time, end_time, key_prefix=None
    ):
        super().__init__()
        self.client = client
        self.entities = entities
        self.start_time = start_time
        self.end_time = end_time
        self.key_prefix = key_prefix
        self.stats_freq_sec = 60
        self.duration_sec = 60

    def execute_script(self, command):
        print('executing...')
        print(command)
        out_file = open(self.OUTPUT_FILE, "w+")
        err_file = open(self.ERROR_FILE, "w+")
        subprocess.call(command, shell=True, stdout=out_file, stderr=err_file)
        out_file.close()
        err_file.close()

    def parse_rapido_output(self):
        # Output looks like the following:
        # <entity_name>\t<key_name>\t[[ts, value], [ts, value], ...]
        # ts = timestamp; value = value of key_name in entity_name at time ts
        self.keys_ts = {}
        with open(self.OUTPUT_FILE, 'r') as fp:
            for line in fp:
                token_list = line.strip().split('\t')
                entity = token_list[0]
                key = token_list[1]
                if entity not in self.keys_ts:
                    self.keys_ts[entity] = {}
                if key not in self.keys_ts[entity]:
                    self.keys_ts[entity][key] = {}
                list_of_lists = [
                    self._get_time_value_pair(pair_string)
                    for pair_string in token_list[2].split('],')
                ]
                value = {pair[0]: pair[1] for pair in list_of_lists}
                self.keys_ts[entity][key] = value

    def parse_ods_output(self):
        # Output looks like the following:
        # <entity_name>\t<key_name>\t<timestamp>\t<value>
        # there is one line per (entity_name, key_name, timestamp)
        self.keys_ts = {}
        with open(self.OUTPUT_FILE, 'r') as fp:
            for line in fp:
                token_list = line.split()
                entity = token_list[0]
                if entity not in self.keys_ts:
                    self.keys_ts[entity] = {}
                key = token_list[1]
                if key not in self.keys_ts[entity]:
                    self.keys_ts[entity][key] = {}
                self.keys_ts[entity][key][token_list[2]] = token_list[3]

    def fetch_timeseries(self, statistics):
        # this method fetches the timeseries of required stats from the ODS
        # service and populates the 'keys_ts' object appropriately
        print('OdsStatsFetcher: fetching ' + str(statistics))
        if re.search('rapido', self.client, re.IGNORECASE):
            command = self.RAPIDO_COMMAND % (
                self.client,
                self._get_string_in_quotes(self.entities),
                self._get_string_in_quotes(','.join(statistics)),
                self._get_string_in_quotes(self.start_time),
                self._get_string_in_quotes(self.end_time)
            )
            # Run the tool and fetch the time-series data
            self.execute_script(command)
            # Parse output and populate the 'keys_ts' map
            self.parse_rapido_output()
        elif re.search('ods', self.client, re.IGNORECASE):
            command = (
                self.client + ' ' +
                '--stime=' + self._get_ods_cli_stime(self.start_time) + ' ' +
                self._get_string_in_quotes(self.entities) + ' ' +
                self._get_string_in_quotes(','.join(statistics))
            )
            # Run the tool and fetch the time-series data
            self.execute_script(command)
            # Parse output and populate the 'keys_ts' map
            self.parse_ods_output()

    def get_keys_from_conditions(self, conditions):
        reqd_stats = []
        for cond in conditions:
            for key in cond.keys:
                use_prefix = False
                if key.startswith('[]'):
                    use_prefix = True
                    key = key[2:]
                # TODO(poojam23): this is very hacky and needs to be improved
                if key.startswith("rocksdb"):
                    key += ".60"
                if use_prefix:
                    if not self.key_prefix:
                        print('Warning: OdsStatsFetcher might need key prefix')
                        print('for the key: ' + key)
                    else:
                        key = self.key_prefix + "." + key
                reqd_stats.append(key)
        return reqd_stats

    def fetch_rate_url(self, entities, keys, window_len, percent, display):
        # type: (List[str], List[str], str, str, bool) -> str
        transform_desc = (
            "rate(" + str(window_len) + ",duration=" + str(self.duration_sec)
        )
        if percent:
            transform_desc = transform_desc + ",%)"
        else:
            transform_desc = transform_desc + ")"
        if re.search('rapido', self.client, re.IGNORECASE):
            command = self.RAPIDO_COMMAND + " --transform=%s --url=%s"
            command = command % (
                self.client,
                self._get_string_in_quotes(','.join(entities)),
                self._get_string_in_quotes(','.join(keys)),
                self._get_string_in_quotes(self.start_time),
                self._get_string_in_quotes(self.end_time),
                self._get_string_in_quotes(transform_desc),
                self._get_string_in_quotes(display)
            )
        elif re.search('ods', self.client, re.IGNORECASE):
            command = (
                self.client + ' ' +
                '--stime=' + self._get_ods_cli_stime(self.start_time) + ' ' +
                '--fburlonly ' +
                self._get_string_in_quotes(entities) + ' ' +
                self._get_string_in_quotes(','.join(keys)) + ' ' +
                self._get_string_in_quotes(transform_desc)
            )
        self.execute_script(command)
        url = ""
        with open(self.OUTPUT_FILE, 'r') as fp:
            url = fp.readline()
        return url

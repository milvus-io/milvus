# Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
#  This source code is licensed under both the GPLv2 (found in the
#  COPYING file in the root directory) and Apache 2.0 License
#  (found in the LICENSE.Apache file in the root directory).

from advisor.db_stats_fetcher import LogStatsParser, DatabasePerfContext
from advisor.db_timeseries_parser import NO_ENTITY
from advisor.rule_parser import Condition, TimeSeriesCondition
import os
import time
import unittest
from unittest.mock import MagicMock


class TestLogStatsParser(unittest.TestCase):
    def setUp(self):
        this_path = os.path.abspath(os.path.dirname(__file__))
        stats_file = os.path.join(
            this_path, 'input_files/log_stats_parser_keys_ts'
        )
        # populate the keys_ts dictionary of LogStatsParser
        self.stats_dict = {NO_ENTITY: {}}
        with open(stats_file, 'r') as fp:
            for line in fp:
                stat_name = line.split(':')[0].strip()
                self.stats_dict[NO_ENTITY][stat_name] = {}
                token_list = line.split(':')[1].strip().split(',')
                for token in token_list:
                    timestamp = int(token.split()[0])
                    value = float(token.split()[1])
                    self.stats_dict[NO_ENTITY][stat_name][timestamp] = value
        self.log_stats_parser = LogStatsParser('dummy_log_file', 20)
        self.log_stats_parser.keys_ts = self.stats_dict

    def test_check_and_trigger_conditions_bursty(self):
        # mock fetch_timeseries() because 'keys_ts' has been pre-populated
        self.log_stats_parser.fetch_timeseries = MagicMock()
        # condition: bursty
        cond1 = Condition('cond-1')
        cond1 = TimeSeriesCondition.create(cond1)
        cond1.set_parameter('keys', 'rocksdb.db.get.micros.p50')
        cond1.set_parameter('behavior', 'bursty')
        cond1.set_parameter('window_sec', 40)
        cond1.set_parameter('rate_threshold', 0)
        self.log_stats_parser.check_and_trigger_conditions([cond1])
        expected_cond_trigger = {
            NO_ENTITY: {1530896440: 0.9767546362322214}
        }
        self.assertDictEqual(expected_cond_trigger, cond1.get_trigger())
        # ensure that fetch_timeseries() was called once
        self.log_stats_parser.fetch_timeseries.assert_called_once()

    def test_check_and_trigger_conditions_eval_agg(self):
        # mock fetch_timeseries() because 'keys_ts' has been pre-populated
        self.log_stats_parser.fetch_timeseries = MagicMock()
        # condition: evaluate_expression
        cond1 = Condition('cond-1')
        cond1 = TimeSeriesCondition.create(cond1)
        cond1.set_parameter('keys', 'rocksdb.db.get.micros.p50')
        cond1.set_parameter('behavior', 'evaluate_expression')
        keys = [
            'rocksdb.manifest.file.sync.micros.p99',
            'rocksdb.db.get.micros.p50'
        ]
        cond1.set_parameter('keys', keys)
        cond1.set_parameter('aggregation_op', 'latest')
        # condition evaluates to FALSE
        cond1.set_parameter('evaluate', 'keys[0]-(keys[1]*100)>200')
        self.log_stats_parser.check_and_trigger_conditions([cond1])
        expected_cond_trigger = {NO_ENTITY: [1792.0, 15.9638]}
        self.assertIsNone(cond1.get_trigger())
        # condition evaluates to TRUE
        cond1.set_parameter('evaluate', 'keys[0]-(keys[1]*100)<200')
        self.log_stats_parser.check_and_trigger_conditions([cond1])
        expected_cond_trigger = {NO_ENTITY: [1792.0, 15.9638]}
        self.assertDictEqual(expected_cond_trigger, cond1.get_trigger())
        # ensure that fetch_timeseries() was called
        self.log_stats_parser.fetch_timeseries.assert_called()

    def test_check_and_trigger_conditions_eval(self):
        # mock fetch_timeseries() because 'keys_ts' has been pre-populated
        self.log_stats_parser.fetch_timeseries = MagicMock()
        # condition: evaluate_expression
        cond1 = Condition('cond-1')
        cond1 = TimeSeriesCondition.create(cond1)
        cond1.set_parameter('keys', 'rocksdb.db.get.micros.p50')
        cond1.set_parameter('behavior', 'evaluate_expression')
        keys = [
            'rocksdb.manifest.file.sync.micros.p99',
            'rocksdb.db.get.micros.p50'
        ]
        cond1.set_parameter('keys', keys)
        cond1.set_parameter('evaluate', 'keys[0]-(keys[1]*100)>500')
        self.log_stats_parser.check_and_trigger_conditions([cond1])
        expected_trigger = {NO_ENTITY: {
            1530896414: [9938.0, 16.31508],
            1530896440: [9938.0, 16.346602],
            1530896466: [9938.0, 16.284669],
            1530896492: [9938.0, 16.16005]
        }}
        self.assertDictEqual(expected_trigger, cond1.get_trigger())
        self.log_stats_parser.fetch_timeseries.assert_called_once()


class TestDatabasePerfContext(unittest.TestCase):
    def test_unaccumulate_metrics(self):
        perf_dict = {
            "user_key_comparison_count": 675903942,
            "block_cache_hit_count": 830086,
        }
        timestamp = int(time.time())
        perf_ts = {}
        for key in perf_dict:
            perf_ts[key] = {}
            start_val = perf_dict[key]
            for ix in range(5):
                perf_ts[key][timestamp+(ix*10)] = start_val + (2 * ix * ix)
        db_perf_context = DatabasePerfContext(perf_ts, 10, True)
        timestamps = [timestamp+(ix*10) for ix in range(1, 5, 1)]
        values = [val for val in range(2, 15, 4)]
        inner_dict = {timestamps[ix]: values[ix] for ix in range(4)}
        expected_keys_ts = {NO_ENTITY: {
            'user_key_comparison_count': inner_dict,
            'block_cache_hit_count': inner_dict
        }}
        self.assertDictEqual(expected_keys_ts, db_perf_context.keys_ts)

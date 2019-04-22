# Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
#  This source code is licensed under both the GPLv2 (found in the
#  COPYING file in the root directory) and Apache 2.0 License
#  (found in the LICENSE.Apache file in the root directory).

from advisor.rule_parser import RulesSpec
from advisor.db_log_parser import DatabaseLogs, DataSource
from advisor.db_options_parser import DatabaseOptions
from advisor.db_stats_fetcher import LogStatsParser, OdsStatsFetcher
import argparse


def main(args):
    # initialise the RulesSpec parser
    rule_spec_parser = RulesSpec(args.rules_spec)
    rule_spec_parser.load_rules_from_spec()
    rule_spec_parser.perform_section_checks()
    # initialize the DatabaseOptions object
    db_options = DatabaseOptions(args.rocksdb_options)
    # Create DatabaseLogs object
    db_logs = DatabaseLogs(
        args.log_files_path_prefix, db_options.get_column_families()
    )
    # Create the Log STATS object
    db_log_stats = LogStatsParser(
        args.log_files_path_prefix, args.stats_dump_period_sec
    )
    data_sources = {
        DataSource.Type.DB_OPTIONS: [db_options],
        DataSource.Type.LOG: [db_logs],
        DataSource.Type.TIME_SERIES: [db_log_stats]
    }
    if args.ods_client:
        data_sources[DataSource.Type.TIME_SERIES].append(OdsStatsFetcher(
            args.ods_client,
            args.ods_entity,
            args.ods_tstart,
            args.ods_tend,
            args.ods_key_prefix
        ))
    triggered_rules = rule_spec_parser.get_triggered_rules(
        data_sources, db_options.get_column_families()
    )
    rule_spec_parser.print_rules(triggered_rules)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Use this script to get\
        suggestions for improving Rocksdb performance.')
    parser.add_argument(
        '--rules_spec', required=True, type=str,
        help='path of the file containing the expert-specified Rules'
    )
    parser.add_argument(
        '--rocksdb_options', required=True, type=str,
        help='path of the starting Rocksdb OPTIONS file'
    )
    parser.add_argument(
        '--log_files_path_prefix', required=True, type=str,
        help='path prefix of the Rocksdb LOG files'
    )
    parser.add_argument(
        '--stats_dump_period_sec', required=True, type=int,
        help='the frequency (in seconds) at which STATISTICS are printed to ' +
        'the Rocksdb LOG file'
    )
    # ODS arguments
    parser.add_argument(
        '--ods_client', type=str, help='the ODS client binary'
    )
    parser.add_argument(
        '--ods_entity', type=str,
        help='the servers for which the ODS stats need to be fetched'
    )
    parser.add_argument(
        '--ods_key_prefix', type=str,
        help='the prefix that needs to be attached to the keys of time ' +
        'series to be fetched from ODS'
    )
    parser.add_argument(
        '--ods_tstart', type=int,
        help='start time of timeseries to be fetched from ODS'
    )
    parser.add_argument(
        '--ods_tend', type=int,
        help='end time of timeseries to be fetched from ODS'
    )
    args = parser.parse_args()
    main(args)

# Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
#  This source code is licensed under both the GPLv2 (found in the
#  COPYING file in the root directory) and Apache 2.0 License
#  (found in the LICENSE.Apache file in the root directory).

import argparse
from advisor.db_config_optimizer import ConfigOptimizer
from advisor.db_log_parser import NO_COL_FAMILY
from advisor.db_options_parser import DatabaseOptions
from advisor.rule_parser import RulesSpec


CONFIG_OPT_NUM_ITER = 10


def main(args):
    # initialise the RulesSpec parser
    rule_spec_parser = RulesSpec(args.rules_spec)
    # initialise the benchmark runner
    bench_runner_module = __import__(
        args.benchrunner_module, fromlist=[args.benchrunner_class]
    )
    bench_runner_class = getattr(bench_runner_module, args.benchrunner_class)
    ods_args = {}
    if args.ods_client and args.ods_entity:
        ods_args['client_script'] = args.ods_client
        ods_args['entity'] = args.ods_entity
        if args.ods_key_prefix:
            ods_args['key_prefix'] = args.ods_key_prefix
    db_bench_runner = bench_runner_class(args.benchrunner_pos_args, ods_args)
    # initialise the database configuration
    db_options = DatabaseOptions(args.rocksdb_options, args.misc_options)
    # set the frequency at which stats are dumped in the LOG file and the
    # location of the LOG file.
    db_log_dump_settings = {
        "DBOptions.stats_dump_period_sec": {
            NO_COL_FAMILY: args.stats_dump_period_sec
        }
    }
    db_options.update_options(db_log_dump_settings)
    # initialise the configuration optimizer
    config_optimizer = ConfigOptimizer(
        db_bench_runner,
        db_options,
        rule_spec_parser,
        args.base_db_path
    )
    # run the optimiser to improve the database configuration for given
    # benchmarks, with the help of expert-specified rules
    final_db_options = config_optimizer.run()
    # generate the final rocksdb options file
    print(
        'Final configuration in: ' +
        final_db_options.generate_options_config('final')
    )
    print(
        'Final miscellaneous options: ' +
        repr(final_db_options.get_misc_options())
    )


if __name__ == '__main__':
    '''
    An example run of this tool from the command-line would look like:
    python3 -m advisor.config_optimizer_example
    --base_db_path=/tmp/rocksdbtest-155919/dbbench
    --rocksdb_options=temp/OPTIONS_boot.tmp --misc_options bloom_bits=2
    --rules_spec=advisor/rules.ini --stats_dump_period_sec=20
    --benchrunner_module=advisor.db_bench_runner
    --benchrunner_class=DBBenchRunner --benchrunner_pos_args ./../../db_bench
    readwhilewriting use_existing_db=true duration=90
    '''
    parser = argparse.ArgumentParser(description='This script is used for\
        searching for a better database configuration')
    parser.add_argument(
        '--rocksdb_options', required=True, type=str,
        help='path of the starting Rocksdb OPTIONS file'
    )
    # these are options that are column-family agnostic and are not yet
    # supported by the Rocksdb Options file: eg. bloom_bits=2
    parser.add_argument(
        '--misc_options', nargs='*',
        help='whitespace-separated list of options that are not supported ' +
        'by the Rocksdb OPTIONS file, given in the ' +
        '<option_name>=<option_value> format eg. "bloom_bits=2 ' +
        'rate_limiter_bytes_per_sec=128000000"')
    parser.add_argument(
        '--base_db_path', required=True, type=str,
        help='path for the Rocksdb database'
    )
    parser.add_argument(
        '--rules_spec', required=True, type=str,
        help='path of the file containing the expert-specified Rules'
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
    # benchrunner_module example: advisor.db_benchmark_client
    parser.add_argument(
        '--benchrunner_module', required=True, type=str,
        help='the module containing the BenchmarkRunner class to be used by ' +
        'the Optimizer, example: advisor.db_bench_runner'
    )
    # benchrunner_class example: DBBenchRunner
    parser.add_argument(
        '--benchrunner_class', required=True, type=str,
        help='the name of the BenchmarkRunner class to be used by the ' +
        'Optimizer, should be present in the module provided in the ' +
        'benchrunner_module argument, example: DBBenchRunner'
    )
    parser.add_argument(
        '--benchrunner_pos_args', nargs='*',
        help='whitespace-separated positional arguments that are passed on ' +
        'to the constructor of the BenchmarkRunner class provided in the ' +
        'benchrunner_class argument, example: "use_existing_db=true ' +
        'duration=900"'
    )
    args = parser.parse_args()
    main(args)

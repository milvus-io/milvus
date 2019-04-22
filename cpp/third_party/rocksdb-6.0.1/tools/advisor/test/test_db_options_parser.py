# Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
#  This source code is licensed under both the GPLv2 (found in the
#  COPYING file in the root directory) and Apache 2.0 License
#  (found in the LICENSE.Apache file in the root directory).

from advisor.db_log_parser import NO_COL_FAMILY
from advisor.db_options_parser import DatabaseOptions
from advisor.rule_parser import Condition, OptionCondition
import os
import unittest


class TestDatabaseOptions(unittest.TestCase):
    def setUp(self):
        self.this_path = os.path.abspath(os.path.dirname(__file__))
        self.og_options = os.path.join(
            self.this_path, 'input_files/OPTIONS-000005'
        )
        misc_options = [
            'bloom_bits = 4', 'rate_limiter_bytes_per_sec = 1024000'
        ]
        # create the options object
        self.db_options = DatabaseOptions(self.og_options, misc_options)
        # perform clean-up before running tests
        self.generated_options = os.path.join(
            self.this_path, '../temp/OPTIONS_testing.tmp'
        )
        if os.path.isfile(self.generated_options):
            os.remove(self.generated_options)

    def test_get_options_diff(self):
        old_opt = {
            'DBOptions.stats_dump_freq_sec': {NO_COL_FAMILY: '20'},
            'CFOptions.write_buffer_size': {
                'default': '1024000',
                'col_fam_A': '128000',
                'col_fam_B': '128000000'
            },
            'DBOptions.use_fsync': {NO_COL_FAMILY: 'true'},
            'DBOptions.max_log_file_size': {NO_COL_FAMILY: '128000000'}
        }
        new_opt = {
            'bloom_bits': {NO_COL_FAMILY: '4'},
            'CFOptions.write_buffer_size': {
                'default': '128000000',
                'col_fam_A': '128000',
                'col_fam_C': '128000000'
            },
            'DBOptions.use_fsync': {NO_COL_FAMILY: 'true'},
            'DBOptions.max_log_file_size': {NO_COL_FAMILY: '0'}
        }
        diff = DatabaseOptions.get_options_diff(old_opt, new_opt)

        expected_diff = {
            'DBOptions.stats_dump_freq_sec': {NO_COL_FAMILY: ('20', None)},
            'bloom_bits': {NO_COL_FAMILY: (None, '4')},
            'CFOptions.write_buffer_size': {
                'default': ('1024000', '128000000'),
                'col_fam_B': ('128000000', None),
                'col_fam_C': (None, '128000000')
            },
            'DBOptions.max_log_file_size': {NO_COL_FAMILY: ('128000000', '0')}
        }
        self.assertDictEqual(diff, expected_diff)

    def test_is_misc_option(self):
        self.assertTrue(DatabaseOptions.is_misc_option('bloom_bits'))
        self.assertFalse(
            DatabaseOptions.is_misc_option('DBOptions.stats_dump_freq_sec')
        )

    def test_set_up(self):
        options = self.db_options.get_all_options()
        self.assertEqual(22, len(options.keys()))
        expected_misc_options = {
            'bloom_bits': '4', 'rate_limiter_bytes_per_sec': '1024000'
        }
        self.assertDictEqual(
            expected_misc_options, self.db_options.get_misc_options()
        )
        self.assertListEqual(
            ['default', 'col_fam_A'], self.db_options.get_column_families()
        )

    def test_get_options(self):
        opt_to_get = [
            'DBOptions.manual_wal_flush', 'DBOptions.db_write_buffer_size',
            'bloom_bits', 'CFOptions.compaction_filter_factory',
            'CFOptions.num_levels', 'rate_limiter_bytes_per_sec',
            'TableOptions.BlockBasedTable.block_align', 'random_option'
        ]
        options = self.db_options.get_options(opt_to_get)
        expected_options = {
            'DBOptions.manual_wal_flush': {NO_COL_FAMILY: 'false'},
            'DBOptions.db_write_buffer_size': {NO_COL_FAMILY: '0'},
            'bloom_bits': {NO_COL_FAMILY: '4'},
            'CFOptions.compaction_filter_factory': {
                'default': 'nullptr', 'col_fam_A': 'nullptr'
            },
            'CFOptions.num_levels': {'default': '7', 'col_fam_A': '5'},
            'rate_limiter_bytes_per_sec': {NO_COL_FAMILY: '1024000'},
            'TableOptions.BlockBasedTable.block_align': {
                'default': 'false', 'col_fam_A': 'true'
            }
        }
        self.assertDictEqual(expected_options, options)

    def test_update_options(self):
        # add new, update old, set old
        # before updating
        expected_old_opts = {
            'DBOptions.db_log_dir': {NO_COL_FAMILY: None},
            'DBOptions.manual_wal_flush': {NO_COL_FAMILY: 'false'},
            'bloom_bits': {NO_COL_FAMILY: '4'},
            'CFOptions.num_levels': {'default': '7', 'col_fam_A': '5'},
            'TableOptions.BlockBasedTable.block_restart_interval': {
                'col_fam_A': '16'
            }
        }
        get_opts = list(expected_old_opts.keys())
        options = self.db_options.get_options(get_opts)
        self.assertEqual(expected_old_opts, options)
        # after updating options
        update_opts = {
            'DBOptions.db_log_dir': {NO_COL_FAMILY: '/dev/shm'},
            'DBOptions.manual_wal_flush': {NO_COL_FAMILY: 'true'},
            'bloom_bits': {NO_COL_FAMILY: '2'},
            'CFOptions.num_levels': {'col_fam_A': '7'},
            'TableOptions.BlockBasedTable.block_restart_interval': {
                'default': '32'
            },
            'random_misc_option': {NO_COL_FAMILY: 'something'}
        }
        self.db_options.update_options(update_opts)
        update_opts['CFOptions.num_levels']['default'] = '7'
        update_opts['TableOptions.BlockBasedTable.block_restart_interval'] = {
            'default': '32', 'col_fam_A': '16'
        }
        get_opts.append('random_misc_option')
        options = self.db_options.get_options(get_opts)
        self.assertDictEqual(update_opts, options)
        expected_misc_options = {
            'bloom_bits': '2',
            'rate_limiter_bytes_per_sec': '1024000',
            'random_misc_option': 'something'
        }
        self.assertDictEqual(
            expected_misc_options, self.db_options.get_misc_options()
        )

    def test_generate_options_config(self):
        # make sure file does not exist from before
        self.assertFalse(os.path.isfile(self.generated_options))
        self.db_options.generate_options_config('testing')
        self.assertTrue(os.path.isfile(self.generated_options))

    def test_check_and_trigger_conditions(self):
        # options only from CFOptions
        # setup the OptionCondition objects to check and trigger
        update_dict = {
            'CFOptions.level0_file_num_compaction_trigger': {'col_fam_A': '4'},
            'CFOptions.max_bytes_for_level_base': {'col_fam_A': '10'}
        }
        self.db_options.update_options(update_dict)
        cond1 = Condition('opt-cond-1')
        cond1 = OptionCondition.create(cond1)
        cond1.set_parameter(
            'options', [
                'CFOptions.level0_file_num_compaction_trigger',
                'TableOptions.BlockBasedTable.block_restart_interval',
                'CFOptions.max_bytes_for_level_base'
            ]
        )
        cond1.set_parameter(
            'evaluate',
            'int(options[0])*int(options[1])-int(options[2])>=0'
        )
        # only DBOptions
        cond2 = Condition('opt-cond-2')
        cond2 = OptionCondition.create(cond2)
        cond2.set_parameter(
            'options', [
                'DBOptions.db_write_buffer_size',
                'bloom_bits',
                'rate_limiter_bytes_per_sec'
            ]
        )
        cond2.set_parameter(
            'evaluate',
            '(int(options[2]) * int(options[1]) * int(options[0]))==0'
        )
        # mix of CFOptions and DBOptions
        cond3 = Condition('opt-cond-3')
        cond3 = OptionCondition.create(cond3)
        cond3.set_parameter(
            'options', [
                'DBOptions.db_write_buffer_size',  # 0
                'CFOptions.num_levels',  # 5, 7
                'bloom_bits'  # 4
            ]
        )
        cond3.set_parameter(
            'evaluate', 'int(options[2])*int(options[0])+int(options[1])>6'
        )
        self.db_options.check_and_trigger_conditions([cond1, cond2, cond3])

        cond1_trigger = {'col_fam_A': ['4', '16', '10']}
        self.assertDictEqual(cond1_trigger, cond1.get_trigger())
        cond2_trigger = {NO_COL_FAMILY: ['0', '4', '1024000']}
        self.assertDictEqual(cond2_trigger, cond2.get_trigger())
        cond3_trigger = {'default': ['0', '7', '4']}
        self.assertDictEqual(cond3_trigger, cond3.get_trigger())


if __name__ == '__main__':
    unittest.main()

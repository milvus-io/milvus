# Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
#  This source code is licensed under both the GPLv2 (found in the
#  COPYING file in the root directory) and Apache 2.0 License
#  (found in the LICENSE.Apache file in the root directory).

from advisor.db_log_parser import DatabaseLogs, Log, NO_COL_FAMILY
from advisor.rule_parser import Condition, LogCondition
import os
import unittest


class TestLog(unittest.TestCase):
    def setUp(self):
        self.column_families = ['default', 'col_fam_A']

    def test_get_column_family(self):
        test_log = (
            "2018/05/25-14:34:21.047233 7f82ba72e700 [db/flush_job.cc:371] " +
            "[col_fam_A] [JOB 44] Level-0 flush table #84: 1890780 bytes OK"
        )
        db_log = Log(test_log, self.column_families)
        self.assertEqual('col_fam_A', db_log.get_column_family())

        test_log = (
            "2018/05/25-14:34:21.047233 7f82ba72e700 [db/flush_job.cc:371] " +
            "[JOB 44] Level-0 flush table #84: 1890780 bytes OK"
        )
        db_log = Log(test_log, self.column_families)
        db_log.append_message('[default] some remaining part of log')
        self.assertEqual(NO_COL_FAMILY, db_log.get_column_family())

    def test_get_methods(self):
        hr_time = "2018/05/25-14:30:25.491635"
        context = "7f82ba72e700"
        message = (
            "[db/flush_job.cc:331] [default] [JOB 10] Level-0 flush table " +
            "#23: started"
        )
        test_log = hr_time + " " + context + " " + message
        db_log = Log(test_log, self.column_families)
        self.assertEqual(db_log.get_message(), message)
        remaining_message = "[col_fam_A] some more logs"
        db_log.append_message(remaining_message)
        self.assertEqual(
            db_log.get_human_readable_time(), "2018/05/25-14:30:25.491635"
        )
        self.assertEqual(db_log.get_context(), "7f82ba72e700")
        self.assertEqual(db_log.get_timestamp(), 1527258625)
        self.assertEqual(
            db_log.get_message(), str(message + '\n' + remaining_message)
        )

    def test_is_new_log(self):
        new_log = "2018/05/25-14:34:21.047233 context random new log"
        remaining_log = "2018/05/25 not really a new log"
        self.assertTrue(Log.is_new_log(new_log))
        self.assertFalse(Log.is_new_log(remaining_log))


class TestDatabaseLogs(unittest.TestCase):
    def test_check_and_trigger_conditions(self):
        this_path = os.path.abspath(os.path.dirname(__file__))
        logs_path_prefix = os.path.join(this_path, 'input_files/LOG-0')
        column_families = ['default', 'col-fam-A', 'col-fam-B']
        db_logs = DatabaseLogs(logs_path_prefix, column_families)
        # matches, has 2 col_fams
        condition1 = LogCondition.create(Condition('cond-A'))
        condition1.set_parameter('regex', 'random log message')
        # matches, multiple lines message
        condition2 = LogCondition.create(Condition('cond-B'))
        condition2.set_parameter('regex', 'continuing on next line')
        # does not match
        condition3 = LogCondition.create(Condition('cond-C'))
        condition3.set_parameter('regex', 'this should match no log')
        db_logs.check_and_trigger_conditions(
            [condition1, condition2, condition3]
        )
        cond1_trigger = condition1.get_trigger()
        self.assertEqual(2, len(cond1_trigger.keys()))
        self.assertSetEqual(
            {'col-fam-A', NO_COL_FAMILY}, set(cond1_trigger.keys())
        )
        self.assertEqual(2, len(cond1_trigger['col-fam-A']))
        messages = [
            "[db/db_impl.cc:563] [col-fam-A] random log message for testing",
            "[db/db_impl.cc:653] [col-fam-A] another random log message"
        ]
        self.assertIn(cond1_trigger['col-fam-A'][0].get_message(), messages)
        self.assertIn(cond1_trigger['col-fam-A'][1].get_message(), messages)
        self.assertEqual(1, len(cond1_trigger[NO_COL_FAMILY]))
        self.assertEqual(
            cond1_trigger[NO_COL_FAMILY][0].get_message(),
            "[db/db_impl.cc:331] [unknown] random log message no column family"
        )
        cond2_trigger = condition2.get_trigger()
        self.assertEqual(['col-fam-B'], list(cond2_trigger.keys()))
        self.assertEqual(1, len(cond2_trigger['col-fam-B']))
        self.assertEqual(
            cond2_trigger['col-fam-B'][0].get_message(),
            "[db/db_impl.cc:234] [col-fam-B] log continuing on next line\n" +
            "remaining part of the log"
        )
        self.assertIsNone(condition3.get_trigger())

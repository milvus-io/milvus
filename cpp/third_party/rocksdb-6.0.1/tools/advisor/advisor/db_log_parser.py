# Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
#  This source code is licensed under both the GPLv2 (found in the
#  COPYING file in the root directory) and Apache 2.0 License
#  (found in the LICENSE.Apache file in the root directory).

from abc import ABC, abstractmethod
from calendar import timegm
from enum import Enum
import glob
import re
import time


NO_COL_FAMILY = 'DB_WIDE'


class DataSource(ABC):
    class Type(Enum):
        LOG = 1
        DB_OPTIONS = 2
        TIME_SERIES = 3

    def __init__(self, type):
        self.type = type

    @abstractmethod
    def check_and_trigger_conditions(self, conditions):
        pass


class Log:
    @staticmethod
    def is_new_log(log_line):
        # The assumption is that a new log will start with a date printed in
        # the below regex format.
        date_regex = '\d{4}/\d{2}/\d{2}-\d{2}:\d{2}:\d{2}\.\d{6}'
        return re.match(date_regex, log_line)

    def __init__(self, log_line, column_families):
        token_list = log_line.strip().split()
        self.time = token_list[0]
        self.context = token_list[1]
        self.message = " ".join(token_list[2:])
        self.column_family = None
        # example log for 'default' column family:
        # "2018/07/25-17:29:05.176080 7f969de68700 [db/compaction_job.cc:1634]
        # [default] [JOB 3] Compacting 24@0 + 16@1 files to L1, score 6.00\n"
        for col_fam in column_families:
            search_for_str = '\[' + col_fam + '\]'
            if re.search(search_for_str, self.message):
                self.column_family = col_fam
                break
        if not self.column_family:
            self.column_family = NO_COL_FAMILY

    def get_human_readable_time(self):
        # example from a log line: '2018/07/25-11:25:45.782710'
        return self.time

    def get_column_family(self):
        return self.column_family

    def get_context(self):
        return self.context

    def get_message(self):
        return self.message

    def append_message(self, remaining_log):
        self.message = self.message + '\n' + remaining_log.strip()

    def get_timestamp(self):
        # example: '2018/07/25-11:25:45.782710' will be converted to the GMT
        # Unix timestamp 1532517945 (note: this method assumes that self.time
        # is in GMT)
        hr_time = self.time + 'GMT'
        timestamp = timegm(time.strptime(hr_time, "%Y/%m/%d-%H:%M:%S.%f%Z"))
        return timestamp

    def __repr__(self):
        return (
            'time: ' + self.time + '; context: ' + self.context +
            '; col_fam: ' + self.column_family +
            '; message: ' + self.message
        )


class DatabaseLogs(DataSource):
    def __init__(self, logs_path_prefix, column_families):
        super().__init__(DataSource.Type.LOG)
        self.logs_path_prefix = logs_path_prefix
        self.column_families = column_families

    def trigger_conditions_for_log(self, conditions, log):
        # For a LogCondition object, trigger is:
        # Dict[column_family_name, List[Log]]. This explains why the condition
        # was triggered and for which column families.
        for cond in conditions:
            if re.search(cond.regex, log.get_message(), re.IGNORECASE):
                trigger = cond.get_trigger()
                if not trigger:
                    trigger = {}
                if log.get_column_family() not in trigger:
                    trigger[log.get_column_family()] = []
                trigger[log.get_column_family()].append(log)
                cond.set_trigger(trigger)

    def check_and_trigger_conditions(self, conditions):
        for file_name in glob.glob(self.logs_path_prefix + '*'):
            # TODO(poojam23): find a way to distinguish between log files
            # - generated in the current experiment but are labeled 'old'
            # because they LOGs exceeded the file size limit  AND
            # - generated in some previous experiment that are also labeled
            # 'old' and were not deleted for some reason
            if re.search('old', file_name, re.IGNORECASE):
                continue
            with open(file_name, 'r') as db_logs:
                new_log = None
                for line in db_logs:
                    if Log.is_new_log(line):
                        if new_log:
                            self.trigger_conditions_for_log(
                                conditions, new_log
                            )
                        new_log = Log(line, self.column_families)
                    else:
                        # To account for logs split into multiple lines
                        new_log.append_message(line)
            # Check for the last log in the file.
            if new_log:
                self.trigger_conditions_for_log(conditions, new_log)

# Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
#  This source code is licensed under both the GPLv2 (found in the
#  COPYING file in the root directory) and Apache 2.0 License
#  (found in the LICENSE.Apache file in the root directory).

from abc import ABC, abstractmethod
import re


class BenchmarkRunner(ABC):
    @staticmethod
    @abstractmethod
    def is_metric_better(new_metric, old_metric):
        pass

    @abstractmethod
    def run_experiment(self):
        # should return a list of DataSource objects
        pass

    @staticmethod
    def get_info_log_file_name(log_dir, db_path):
        # Example: DB Path = /dev/shm and OPTIONS file has option
        # db_log_dir=/tmp/rocks/, then the name of the log file will be
        # 'dev_shm_LOG' and its location will be /tmp/rocks. If db_log_dir is
        # not specified in the OPTIONS file, then the location of the log file
        # will be /dev/shm and the name of the file will be 'LOG'
        file_name = ''
        if log_dir:
            # refer GetInfoLogPrefix() in rocksdb/util/filename.cc
            # example db_path: /dev/shm/dbbench
            file_name = db_path[1:]  # to ignore the leading '/' character
            to_be_replaced = re.compile('[^0-9a-zA-Z\-_\.]')
            for character in to_be_replaced.findall(db_path):
                file_name = file_name.replace(character, '_')
            if not file_name.endswith('_'):
                file_name += '_'
        file_name += 'LOG'
        return file_name

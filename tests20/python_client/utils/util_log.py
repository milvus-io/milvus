import logging
import sys

from config.test_info import test_info


class TestLog:
    def __init__(self, logger, log_debug, log_file, log_err):
        self.logger = logger
        self.log_debug = log_debug
        self.log_file = log_file
        self.log_err = log_err

        self.log = logging.getLogger(self.logger)
        self.log.setLevel(logging.DEBUG)

        try:
            formatter = logging.Formatter("[%(asctime)s - %(levelname)s - %(name)s]: %(message)s (%(filename)s:%(lineno)s)")
            dh = logging.FileHandler(self.log_debug)
            dh.setLevel(logging.DEBUG)
            dh.setFormatter(formatter)
            self.log.addHandler(dh)

            fh = logging.FileHandler(self.log_file)
            fh.setLevel(logging.INFO)
            fh.setFormatter(formatter)
            self.log.addHandler(fh)

            eh = logging.FileHandler(self.log_err)
            eh.setLevel(logging.ERROR)
            eh.setFormatter(formatter)
            self.log.addHandler(eh)

        except Exception as e:
            print("Can not use %s or %s to log." % (log_file, log_err))


"""All modules share this unified log"""
log_debug = test_info.log_debug
log_info = test_info.log_info
log_err = test_info.log_err
test_log = TestLog('refactor_test', log_debug, log_info, log_err).log

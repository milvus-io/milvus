import logging
import sys

from config.test_info import test_info


class TestLog:
    def __init__(self, logger, log_file, log_err):
        self.logger = logger
        self.log_file = log_file
        self.log_err = log_err

        self.log = logging.getLogger(self.logger)
        self.log.setLevel(logging.DEBUG)

        try:
            formatter = logging.Formatter("[%(asctime)s - %(levelname)s - %(name)s]: %(message)s (%(filename)s:%(lineno)s)")
            fh = logging.FileHandler(log_file)
            fh.setLevel(logging.DEBUG)
            # formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
            fh.setFormatter(formatter)
            self.log.addHandler(fh)

            eh = logging.FileHandler(log_err)
            eh.setLevel(logging.ERROR)
            # formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
            eh.setFormatter(formatter)
            self.log.addHandler(eh)

            ch = logging.StreamHandler(sys.stdout)
            ch.setLevel(logging.DEBUG)
            ch.setFormatter(formatter)
            self.log.addHandler(ch)

        except Exception as e:
            print("Can not use %s or %s to log." % (log_file, log_err))


"""All modules share this unified log"""
log_info = test_info.log_info
log_err = test_info.log_err
test_log = TestLog('refactor_test', log_info, log_err).log

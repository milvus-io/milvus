import logging

from config.my_info import my_info


class MyLog:
    def __init__(self, logger, log_file, log_err):
        self.logger = logger
        self.log_file = log_file
        self.log_err = log_err

        self.log = logging.getLogger(self.logger)
        self.log.setLevel(logging.DEBUG)

        try:
            fh = logging.FileHandler(log_file)
            fh.setLevel(logging.DEBUG)
            formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
            fh.setFormatter(formatter)
            self.log.addHandler(fh)

            eh = logging.FileHandler(log_err)
            eh.setLevel(logging.ERROR)
            formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
            eh.setFormatter(formatter)
            self.log.addHandler(eh)

        except Exception as e:
            print("Can not use %s or %s to log." % (log_file, log_err))


"""All modules share this unified log"""
test_log = my_info.test_log
test_err = my_info.test_err
my_log = MyLog('refactor_test', test_log, test_err).log

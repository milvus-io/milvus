import logging
import sys

from config.log_config import log_config


class TestLog:
    def __init__(self, logger, log_debug, log_file, log_err, log_worker):
        self.logger = logger
        self.log_debug = log_debug
        self.log_file = log_file
        self.log_err = log_err
        self.log_worker = log_worker

        self.log = logging.getLogger(self.logger)
        self.log.setLevel(logging.DEBUG)

        try:
            formatter = logging.Formatter("[%(asctime)s - %(levelname)s - %(name)s]: "
                                          "%(message)s (%(filename)s:%(lineno)s)")
            # [%(process)s] process NO.
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

            if self.log_worker != "":
                wh = logging.FileHandler(self.log_worker)
                wh.setLevel(logging.DEBUG)
                wh.setFormatter(formatter)
                self.log.addHandler(wh)

            ch = logging.StreamHandler(sys.stdout)
            ch.setLevel(logging.DEBUG)
            ch.setFormatter(formatter)
            # self.log.addHandler(ch)

        except Exception as e:
            print("Can not use %s or %s or %s to log. error : %s" % (log_debug, log_file, log_err, str(e)))


"""All modules share this unified log"""
log_debug = log_config.log_debug
log_info = log_config.log_info
log_err = log_config.log_err
log_worker = log_config.log_worker
test_log = TestLog('ci_test', log_debug, log_info, log_err, log_worker).log

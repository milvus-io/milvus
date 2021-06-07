import json


class TestInfo:
    def __init__(self):
        self.get_default_config()

    def get_default_config(self):
        """ Make sure the path exists """
        self.home_dir = "/tmp/"
        self.log_dir = self.home_dir + "log/"
        self.log_debug = "%s/ci_test_log.debug" % self.log_dir
        self.log_info = "%s/ci_test_log.log" % self.log_dir
        self.log_err = "%s/ci_test_log.err" % self.log_dir


test_info = TestInfo()

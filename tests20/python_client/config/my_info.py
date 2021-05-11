import json


class MyInfo:
    def __init__(self):
        self.get_default_config()

    def get_default_config(self):
        """ Make sure the path exists """
        self.home_dir = "/tmp/"
        self.log_dir = self.home_dir + "log/"
        self.test_log = "%s/refactor_test.log" % self.log_dir
        self.test_err = "%s/refactor_test.err" % self.log_dir


my_info = MyInfo()

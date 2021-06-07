import os


class TestInfo:
    def __init__(self):
        self.get_default_config()

    @staticmethod
    def get_env_variable(var="CI_LOG_PATH"):
        """ get log path of testing """
        try:
            log_path = os.environ[var]
            return str(log_path)
        except Exception as e:
            log_path = "/tmp/ci_logs/"
            print("Failed to get environment variables : %s, Use default path : %s" % (str(e), log_path))
            return log_path

    @staticmethod
    def create_path(log_path):
        if not os.path.isdir(str(log_path)):
            print("[modify_file] folder(%s) is not exist." % log_path)
            print("[modify_file] create path now...")
            os.makedirs(log_path)

    def get_default_config(self):
        """ Make sure the path exists """
        self.log_dir = self.get_env_variable()
        self.log_debug = "%s/ci_test_log.debug" % self.log_dir
        self.log_info = "%s/ci_test_log.log" % self.log_dir
        self.log_err = "%s/ci_test_log.err" % self.log_dir

        self.create_path(self.log_dir)


test_info = TestInfo()

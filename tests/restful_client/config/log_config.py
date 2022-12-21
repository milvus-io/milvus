import os


class LogConfig:
    def __init__(self):
        self.log_debug = ""
        self.log_err = ""
        self.log_info = ""
        self.log_worker = ""
        self.get_default_config()

    @staticmethod
    def get_env_variable(var="CI_LOG_PATH"):
        """ get log path for testing """
        try:
            log_path = os.environ[var]
            return str(log_path)
        except Exception as e:
            # now = datetime.datetime.now().strftime("%Y-%m-%d-%H-%M-%S")
            log_path = f"/tmp/ci_logs"
            print("[get_env_variable] failed to get environment variables : %s, use default path : %s" % (str(e), log_path))
            return log_path

    @staticmethod
    def create_path(log_path):
        if not os.path.isdir(str(log_path)):
            print("[create_path] folder(%s) is not exist." % log_path)
            print("[create_path] create path now...")
            os.makedirs(log_path)

    def get_default_config(self):
        """ Make sure the path exists """
        log_dir = self.get_env_variable()
        self.log_debug = "%s/ci_test_log.debug" % log_dir
        self.log_info = "%s/ci_test_log.log" % log_dir
        self.log_err = "%s/ci_test_log.err" % log_dir
        work_log = os.environ.get('PYTEST_XDIST_WORKER')
        if work_log is not None:
            self.log_worker = f'{log_dir}/{work_log}.log'

        self.create_path(log_dir)


log_config = LogConfig()

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
        """get log path for testing"""
        try:
            log_path = os.environ[var]
            return str(log_path)
        except Exception as e:
            # now = datetime.datetime.now().strftime("%Y-%m-%d-%H-%M-%S")
            log_path = "/tmp/ci_logs"
            print(f"[get_env_variable] failed to get environment variables : {str(e)}, use default path : {log_path}")
            return log_path

    @staticmethod
    def create_path(log_path):
        if not os.path.isdir(str(log_path)):
            print(f"[create_path] folder({log_path}) is not exist.")
            print("[create_path] create path now...")
            os.makedirs(log_path)

    def get_default_config(self):
        """Make sure the path exists"""
        log_dir = self.get_env_variable()
        self.log_debug = f"{log_dir}/ci_test_log.debug"
        self.log_info = f"{log_dir}/ci_test_log.log"
        self.log_err = f"{log_dir}/ci_test_log.err"
        work_log = os.environ.get("PYTEST_XDIST_WORKER")
        if work_log is not None:
            self.log_worker = f"{log_dir}/{work_log}.log"

        self.create_path(log_dir)


log_config = LogConfig()

import os
from pathlib import Path

class LogConfig:
    def __init__(self):
        self.log_path = ""
        self.log_report_json = ""
        self.log_report_html = ""
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
        print("[create_path] folder(%s) is not exist." % log_path)
        print("[create_path] create path now...")
        folder_path = Path(str(log_path))
        folder_path.mkdir(parents=True, exist_ok=True)

    def get_default_config(self):
        """ Make sure the path exists """
        log_dir = self.get_env_variable()
        self.log_path = log_dir
        self.log_report_json = "%s/test_report.json" % log_dir
        self.log_report_html = "%s/test_report.html" % log_dir

        self.create_path(log_dir)


log_config = LogConfig()

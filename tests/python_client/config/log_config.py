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
        print(f"[create_path] folder({log_path}) is not exist.")
        print("[create_path] create path now...")
        folder_path = Path(str(log_path))
        folder_path.mkdir(parents=True, exist_ok=True)

    def get_default_config(self):
        """Make sure the path exists"""
        log_dir = self.get_env_variable()
        self.log_path = log_dir
        self.log_report_json = f"{log_dir}/test_report.json"
        self.log_report_html = f"{log_dir}/test_report.html"

        self.create_path(log_dir)


log_config = LogConfig()

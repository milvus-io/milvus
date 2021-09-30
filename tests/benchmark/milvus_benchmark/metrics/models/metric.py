import time
import datetime
import json
import hashlib
from .env import Env
from .server import Server
from .hardware import Hardware


class Metric(object):
    def __init__(self):
        # format of report data
        self._version = '0.1'
        self._type = 'metric'
        self.run_id = None
        self.mode = None
        self.server = Server()
        self.hardware = Hardware()
        self.env = Env()
        self.status = "INIT"
        self.err_message = ""
        self.collection = {}
        self.index = {}
        self.search = {}
        self.run_params = {}
        self.metrics = {
            "type": "",
            "value": None,
        }
        self.datetime = str(datetime.datetime.now())

    def set_run_id(self):
        # Get current time as run id, which uniquely identifies this test
        self.run_id = int(time.time())

    def set_mode(self, mode):
        # Set the deployment mode of milvus
        self.mode = mode

    # including: metric, suite_metric
    def set_case_metric_type(self):
        # The current test types on argo are all case
        self._type = "case"

    def json_md5(self):
        json_str = json.dumps(vars(self), sort_keys=True)
        return hashlib.md5(json_str.encode('utf-8')).hexdigest()

    def update_status(self, status):
        # Set the final result of the test run: RUN_SUCC or RUN_FAILED
        self.status = status

    def update_result(self, result):
        # Customized test result update, different test types have different results
        self.metrics["value"].update(result)

    def update_message(self, err_message):
        # If there is an error message in the test result, record the error message and report it
        self.err_message = err_message
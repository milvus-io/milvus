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
        self.run_id = int(time.time())

    def set_mode(self, mode):
        self.mode = mode

    # including: metric, suite_metric
    def set_case_metric_type(self):
        self._type = "case"

    def json_md5(self):
        json_str = json.dumps(vars(self), sort_keys=True)
        return hashlib.md5(json_str.encode('utf-8')).hexdigest()

    def update_status(self, status):
        self.status = status

    def update_result(self, result):
        self.metrics["value"].update(result)

    def update_message(self, err_message):
        self.err_message = err_message
import time
import datetime
import json
import hashlib
from .env import Env
from .server import Server
from .hardware import Hardware


class Metric(object):
    """
    A template for reporting data:

    {
        "_id" : ObjectId("6126865855aba6fb8e742f05"),
        "_version" : "0.1",
        "_type" : "case",
        "run_id" : NumberInt(1629914593),
        "mode" : "local",
        "server" : {
            "id" : ObjectId("6126865855aba6fb8e742f04"),
            "value" : {
                "_version" : "0.1",
                "_type" : "server",
                "version" : "2.0.0-RC5",
                "mode" : "single",
                "build_commit" : null,
                "deploy_opology" : {
                    "server" : {
                        "server_tag" : "8c16m"
                    },
                    "milvus" : {
                        "deploy_mode" : "single"
                    }
                }
            }
        },
        "hardware" : {
            "id" : ObjectId("60f078c5d8aad7192f9baf80"),
            "value" : {
                "_version" : "0.1",
                "_type" : "hardware",
                "name" : "server_tag",
                "cpus" : 0.0
            }
        },
        "env" : {
            "id" : ObjectId("604b54df90fbee981a6ed81d"),
            "value" : {
                "_version" : "0.1",
                "_type" : "env",
                "server_config" : null,
                "OMP_NUM_THREADS" : null
            }
        },
        "status" : "RUN_SUCC",
        "err_message" : "",
        "collection" : {
            "dimension" : NumberInt(128),
            "metric_type" : "l2",
            "dataset_name" : "sift_128_euclidean"
        },
        "index" : {
            "index_type" : "ivf_sq8",
            "index_param" : {
                "nlist" : NumberInt(1024)
            }
        },
        "search" : {
            "nq" : NumberInt(10000),
            "topk" : NumberInt(10),
            "search_param" : {
                "nprobe" : NumberInt(1)
            },
            "filter" : [

            ]
        },
        "run_params" : null,
        "metrics" : {
            "type" : "ann_accuracy",
            "value" : {
                "acc" : 0.377
            }
        },
        "datetime" : "2021-08-25 18:03:13.820593",
        "type" : "metric"
    }
    """
    def __init__(self):
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
import json
import hashlib


class Env:
    """
    {
        "_version": "0.1",
        "_type": "env",
        "server_config": dict,
        "OMP_NUM_THREADS": string,
    }
    """

    def __init__(self, server_config=None, omp_num_threads=None):
        self._version = '0.1'
        self._type = 'env'
        self.server_config = server_config
        self.OMP_NUM_THREADS = omp_num_threads

    def json_md5(self):
        json_str = json.dumps(vars(self), sort_keys=True)
        return hashlib.md5(json_str.encode('utf-8')).hexdigest()

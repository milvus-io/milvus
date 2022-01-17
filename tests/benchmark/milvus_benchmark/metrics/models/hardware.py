import json
import hashlib


class Hardware:
    """
    {
        "_version": "0.1",
        "_type": "hardware",
        "name": string,
        "cpus": float
    }

    """

    def __init__(self, name=None, cpus=0.0):
        self._version = '0.1'
        self._type = 'hardware'
        self.name = name
        self.cpus = cpus

    def json_md5(self):
        json_str = json.dumps(vars(self), sort_keys=True)
        return hashlib.md5(json_str.encode('utf-8')).hexdigest()

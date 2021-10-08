import json
import hashlib


class Server:
    """
    {
        "_version": "0.1",
        "_type": "server",
        "version": string,
        "build_commit": string,
        # "md5": string,
    }
    """

    def __init__(self, version=None, mode=None, build_commit=None, deploy_opology=None):
        self._version = '0.1'
        self._type = 'server'
        self.version = version
        self.mode = mode
        self.build_commit = build_commit
        self.deploy_opology = deploy_opology
        # self.md5 = md5

    def json_md5(self):
        json_str = json.dumps(vars(self), sort_keys=True)
        return hashlib.md5(json_str.encode('utf-8')).hexdigest()

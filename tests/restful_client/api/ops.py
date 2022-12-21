from decorest import GET, POST, DELETE
from decorest import HttpStatus, RestClient
from decorest import accept, body, content, endpoint, form
from decorest import header, multipart, on, query, stream, timeout

class Ops(RestClient):

    def manual_compaction():
        pass

    def get_compaction_plans():
        pass

    def get_compaction_state():
        pass

    def load_balance():
        pass

    def get_replicas():
        pass
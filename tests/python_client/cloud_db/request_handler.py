import requests

TIMEOUT = 60


def api_request_catch():
    def wrapper(func):
        def inner_wrapper(*args, **kwargs):
            try:
                res = func(*args, **kwargs)
                return res
            except Exception as e:
                return e
        return inner_wrapper
    return wrapper


class Request:

    def __init__(self, headers=None):
        self.headers = {'Content-type': "application/json"} if headers is None else headers

    @api_request_catch()
    def post(self, url, data=None, headers=None, timeout=TIMEOUT):
        res = requests.post(url, json=data, headers=headers, timeout=timeout, verify=False)
        return res

    @api_request_catch()
    def get(self, url, data=None, headers=None, timeout=TIMEOUT):
        headers = self.headers if headers is None else headers

        res = requests.get(url, json=data, headers=headers, timeout=timeout, verify=False)
        return res






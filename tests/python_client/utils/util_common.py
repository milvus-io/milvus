from yaml import full_load
import json
import requests
import unittest
from utils.util_log import test_log as log

def gen_experiment_config(yaml):
    """load the yaml file of chaos experiment"""
    with open(yaml) as f:
        _config = full_load(f)
        f.close()
    return _config


def findkeys(node, kv):
    # refer to https://stackoverflow.com/questions/9807634/find-all-occurrences-of-a-key-in-nested-dictionaries-and-lists
    if isinstance(node, list):
        for i in node:
            for x in findkeys(i, kv):
               yield x
    elif isinstance(node, dict):
        if kv in node:
            yield node[kv]
        for j in node.values():
            for x in findkeys(j, kv):
                yield x


def find_value_by_key(node, k):
    # refer to https://stackoverflow.com/questions/9807634/find-all-occurrences-of-a-key-in-nested-dictionaries-and-lists
    if isinstance(node, list):
        for i in node:
            for x in find_value_by_key(i, k):
               yield x
    elif isinstance(node, dict):
        if k in node:
            yield node[k]
        for j in node.values():
            for x in find_value_by_key(j, k):
                yield x


def update_key_value(node, modify_k, modify_v):
    # update the value of modify_k to modify_v
    if isinstance(node, list):
        for i in node:
            update_key_value(i, modify_k, modify_v)
    elif isinstance(node, dict):
        if modify_k in node:
            node[modify_k] = modify_v
        for j in node.values():
            update_key_value(j, modify_k, modify_v)
    return node


def update_key_name(node, modify_k, modify_k_new):
    # update the name of modify_k to modify_k_new
    if isinstance(node, list):
        for i in node:
            update_key_name(i, modify_k, modify_k_new)
    elif isinstance(node, dict):
        if modify_k in node:
            value_backup = node[modify_k]
            del node[modify_k]
            node[modify_k_new] = value_backup
        for j in node.values():
            update_key_name(j, modify_k, modify_k_new)
    return node


def get_collections():
    try:
        with open("/tmp/ci_logs/all_collections.json", "r") as f:
            data = json.load(f)
            collections = data["all"]
    except Exception as e:
        log.error(f"get_all_collections error: {e}")
        return []
    return collections


def get_request_success_rate(url, api_key, body):
    headers = {
        'Authorization': "Bearer " + api_key,
        'Content-Type': 'application/json'
    }
    rsp = requests.post(url, headers=headers, data=json.dumps(body))
    result = {}
    if rsp.status_code == 200:
        results = rsp.json()["results"]
        frames = results["A"]["frames"]
        for frame in frames:
            schema = frame["schema"]
            function_name = list(find_value_by_key(schema, "function_name"))[0]
            data = frame["data"]["values"]  # get the success rate value
            result[function_name] = data
    else:
        log.error(f"Failed to get request success rate with status code {rsp.status_code}")
    return result


def analyze_service_breakdown_time(result, chaos_ts, recovery_rate):
    # analyze the service breakdown time
    service_breakdown_time = {}
    for service, value in result.items():
        ts = value[0]
        success_rate = value[1]
        chaos_inject_point = 0
        failed_point = 0
        failed_ts = ts[0]
        recovery_ts = ts[0]
        for i, t in enumerate(ts):
            if t > chaos_ts:
                chaos_inject_point = i - 1
                break
            if t == chaos_ts:
                chaos_inject_point = i
                break
        previous_rate = sum(success_rate[:chaos_inject_point+1]) / (chaos_inject_point+1)
        for i in range(chaos_inject_point, len(ts)):
            if success_rate[i] < recovery_rate * previous_rate:
                failed_point = i
                failed_ts = ts[i]
                break
        for i in range(failed_point, len(ts)):
            if success_rate[i] >= recovery_rate * previous_rate:
                recovery_ts = ts[i]
                break
            else:
                # if the service is still down,
                # set the recovery time to the last timestamp with another interval
                recovery_ts = ts[-1] + (ts[-1] - ts[-2])

        breakdown_time = recovery_ts - failed_ts
        log.info(f"Service {service} breakdown time is {breakdown_time}")
        service_breakdown_time[service] = breakdown_time
    return service_breakdown_time


class TestUtilCommon(unittest.TestCase):

    def test_find_value_by_key(self):
        test_dict = {"id": "abcde",
                     "key1": "blah",
                     "key2": "blah blah",
                     "nestedlist": [
                         {"id": "qwerty",
                          "nestednestedlist": [
                              {"id": "xyz", "keyA": "blah blah blah"},
                              {"id": "fghi", "keyZ": "blah blah blah"}],
                          "anothernestednestedlist": [
                              {"id": "asdf", "keyQ": "blah blah"},
                              {"id": "yuiop", "keyW": "blah"}]}]}
        self.assertEqual(list(find_value_by_key(test_dict, "id")),
                         ['abcde', 'qwerty', 'xyz', 'fghi', 'asdf', 'yuiop'])

    def test_analyze_service_breakdown_time(self):
        result = {
            "service1": [[1, 2, 3, 4, 5], [1, 0, 0, 0, 1]],
            "service2": [[1, 2, 3, 4, 5], [1, 1, 0, 0, 1]],
            "service3": [[1, 2, 3, 4, 5], [1, 1, 1, 0, 1]],
            "service4": [[1, 2, 3, 4, 5], [1, 1, 1, 1, 0]],
        }
        chaos_ts = 2
        recovery_rate = 0.8
        service_breakdown_time = analyze_service_breakdown_time(result, chaos_ts, recovery_rate)
        self.assertEqual(service_breakdown_time, {"service1": 3, "service2": 2, "service3": 1, "service4": 1})

    def test_get_request_success_rate(self):
        url = "https://xxx/api/ds/query"
        api_key = "xxx"
        body = {
            "queries": [
                {
                    "datasource": {
                        "type": "prometheus",
                        "uid": "P1809F7CD0C75ACF3"
                    },
                    "exemplar": True,
                    "expr": "sum(increase(milvus_proxy_req_count{app_kubernetes_io_instance=~\"datacoord-standby-test\", app_kubernetes_io_name=\"milvus\", namespace=\"chaos-testing\", status=\"success\"}[2m])/120) by(function_name, pod, node_id)",
                    "interval": "",
                    "legendFormat": "{{function_name}}-{{pod}}-{{node_id}}",
                    "queryType": "timeSeriesQuery",
                    "refId": "A",
                    "requestId": "123329A",
                    "utcOffsetSec": 28800,
                    "datasourceId": 1,
                    "intervalMs": 15000,
                    "maxDataPoints": 1070
                }
            ],
            "range": {
                "from": "2023-01-06T04:24:48.549Z",
                "to": "2023-01-06T07:24:48.549Z",
                "raw": {
                    "from": "now-3h",
                    "to": "now"
                }
            },
            "from": "1672979088549",
            "to": "1672989888549"
        }

        rsp = get_request_success_rate(url, api_key, body)
        self.assertEqual(isinstance(rsp, dict), True)


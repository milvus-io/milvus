import random
import time
import random
import string
from faker import Faker
import numpy as np
from sklearn import preprocessing
import requests
from loguru import logger
import datetime

fake = Faker()


def random_string(length=8):
    letters = string.ascii_letters
    return ''.join(random.choice(letters) for _ in range(length))


def gen_collection_name(prefix="test_collection", length=8):
    name = f'{prefix}_' + datetime.datetime.now().strftime("%Y_%m_%d_%H_%M_%S_%f") + random_string(length=length)
    return name

def admin_password():
    return "Milvus"


def invalid_cluster_name():
    res = [
        "demo" * 100,
        "demo" + "!",
        "demo" + "@",
    ]
    return res


def wait_cluster_be_ready(cluster_id, client, timeout=120):
    t0 = time.time()
    while True and time.time() - t0 < timeout:
        rsp = client.cluster_describe(cluster_id)
        if rsp['code'] == 200:
            if rsp['data']['status'] == "RUNNING":
                return time.time() - t0
        time.sleep(1)
        logger.debug("wait cluster to be ready, cost time: %s" % (time.time() - t0))
    return -1





def gen_data_by_type(field):
    data_type = field["type"]
    if data_type == "bool":
        return random.choice([True, False])
    if data_type == "int8":
        return random.randint(-128, 127)
    if data_type == "int16":
        return random.randint(-32768, 32767)
    if data_type == "int32":
        return random.randint(-2147483648, 2147483647)
    if data_type == "int64":
        return random.randint(-9223372036854775808, 9223372036854775807)
    if data_type == "float32":
        return np.float64(random.random())  # Object of type float32 is not JSON serializable, so set it as float64
    if data_type == "float64":
        return np.float64(random.random())
    if "varchar" in data_type:
        length = int(data_type.split("(")[1].split(")")[0])
        return "".join([chr(random.randint(97, 122)) for _ in range(length)])
    if "floatVector" in data_type:
        dim = int(data_type.split("(")[1].split(")")[0])
        return preprocessing.normalize([np.array([random.random() for i in range(dim)])])[0].tolist()
    return None


def get_data_by_fields(fields, nb):
    # logger.info(f"fields: {fields}")
    fields_not_auto_id = []
    for field in fields:
        if not field.get("autoId", False):
            fields_not_auto_id.append(field)
    # logger.info(f"fields_not_auto_id: {fields_not_auto_id}")
    data = []
    for i in range(nb):
        tmp = {}
        for field in fields_not_auto_id:
            tmp[field["name"]] = gen_data_by_type(field)
        data.append(tmp)
    return data


def get_random_json_data(uid=None):
    # gen random dict data
    if uid is None:
        uid = 0
    data = {"uid": uid,  "name": fake.name(), "address": fake.address(), "text": fake.text(), "email": fake.email(),
            "phone_number": fake.phone_number(),
            "array_int_dynamic": [random.randint(1, 100_000) for i in range(random.randint(1, 10))],
            "array_varchar_dynamic": [fake.name() for i in range(random.randint(1, 10))],
            "json": {
                "name": fake.name(),
                "address": fake.address()
                }
            }
    for i in range(random.randint(1, 10)):
        data["key" + str(random.randint(1, 100_000))] = "value" + str(random.randint(1, 100_000))
    return data


def get_data_by_payload(payload, nb=100):
    dim = payload.get("dimension", 128)
    vector_field = payload.get("vectorField", "vector")
    data = []
    if nb == 1:
        data = [{
            vector_field: preprocessing.normalize([np.array([random.random() for i in range(dim)])])[0].tolist(),
            **get_random_json_data()

        }]
    else:
        for i in range(nb):
            data.append({
                vector_field: preprocessing.normalize([np.array([random.random() for i in range(dim)])])[0].tolist(),
                **get_random_json_data(uid=i)
            })
    return data


def get_common_fields_by_data(data, exclude_fields=None):
    fields = set()
    if isinstance(data, dict):
        data = [data]
    if not isinstance(data, list):
        raise Exception("data must be list or dict")
    common_fields = set(data[0].keys())
    for d in data:
        keys = set(d.keys())
        common_fields = common_fields.intersection(keys)
    if exclude_fields is not None:
        exclude_fields = set(exclude_fields)
        common_fields = common_fields.difference(exclude_fields)
    return list(common_fields)


def get_all_fields_by_data(data, exclude_fields=None):
    fields = set()
    for d in data:
        keys = list(d.keys())
        fields.union(keys)
    if exclude_fields is not None:
        exclude_fields = set(exclude_fields)
        fields = fields.difference(exclude_fields)
    return list(fields)




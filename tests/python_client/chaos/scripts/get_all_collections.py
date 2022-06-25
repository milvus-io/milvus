from collections import defaultdict
import json
import argparse
from pymilvus import connections, list_collections
TIMEOUT = 120


def save_all_checker_collections(host="127.0.0.1", prefix="Checker"):
    # create connection
    connections.connect(host=host, port="19530")
    all_collections = list_collections()
    if prefix is None:
        all_collections = [c_name for c_name in all_collections]
    else:
        all_collections = [c_name for c_name in all_collections if prefix in c_name]
    m = defaultdict(list)
    for c_name in all_collections:
        prefix = c_name.split("_")[0]
        if len(m[prefix]) <= 10:
            m[prefix].append(c_name)
    selected_collections = []
    for v in m.values():
        selected_collections.extend(v)
    data = {
        "all": selected_collections
    }
    print("selected_collections is")
    print(selected_collections)
    with open("/tmp/ci_logs/all_collections.json", "w") as f:
        f.write(json.dumps(data))


parser = argparse.ArgumentParser(description='host ip')
parser.add_argument('--host', type=str, default='127.0.0.1', help='host ip')
args = parser.parse_args()
save_all_checker_collections(args.host)
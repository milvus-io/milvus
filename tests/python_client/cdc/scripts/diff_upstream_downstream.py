"""
db --> collection --> partition

status:
entities num
load status
index status


then load partition
query all data
compare result

"""
import time

from loguru import logger
import json
import collections.abc
from deepdiff import DeepDiff
from pymilvus import connections, Collection, db, list_collections
import threading


def convert_deepdiff(diff):
    if isinstance(diff, dict):
        return {k: convert_deepdiff(v) for k, v in diff.items()}
    elif isinstance(diff, collections.abc.Set):
        return list(diff)
    return diff


def get_collection_info(info, db_name, c_name):
    info[db_name][c_name] = {}
    c = Collection(c_name)

    info[db_name][c_name]['name'] = c.name
    # logger.info(c.num_entities)
    info[db_name][c_name]['num_entities'] = c.num_entities
    # logger.info(c.schema)
    info[db_name][c_name]['schema'] = len([f.name for f in c.schema.fields])
    # logger.info(c.indexes)
    info[db_name][c_name]['indexes'] = sorted([x.index_name for x in c.indexes])
    # logger.info(c.partitions)
    info[db_name][c_name]['partitions'] = sorted([p.name for p in c.partitions])
    try:
        replicas = len(c.get_replicas().groups)
    except Exception as e:
        logger.warning(e)
        # logger.info(f"no replica for {db_name}.{c_name}")
        replicas = 0
    # logger.info(replicas)
    info[db_name][c_name]['replicas'] = replicas
    if replicas > 0:
        try:
            # logger.info(f"start query {db_name}.{c_name}")
            res = c.query(expr="", output_fields=["count(*)"], timeout=60)
            cnt = res[0]["count(*)"]
            # logger.info(cnt)
            info[db_name][c_name]['cnt'] = cnt
        except Exception as e:
            # logger.warning(f"failed to query {db_name}.{c_name}: {e}")
            info[db_name][c_name]['cnt'] = -1


def get_cluster_info(uri, token):
    try:
        connections.disconnect(alias='default')
    except Exception as e:
        logger.warning(e)
    if token:
        connections.connect(uri=uri, token=token)
    else:
        connections.connect(uri=uri)
    info = {}
    all_db = db.list_database()
    # logger.info(all_db)
    for db_name in all_db:
        info[db_name] = {}
        db.using_database(db_name)
        all_collection = list_collections()
        # logger.info(all_collection)
        threads = []
        for collection_name in all_collection:
            t = threading.Thread(target=get_collection_info, args=(info, db_name, collection_name))
            threads.append(t)
            t.start()
        for t in threads:
            t.join()

    # logger.info(json.dumps(info, indent=2))
    return info


if __name__ == '__main__':
    import argparse

    parser = argparse.ArgumentParser(description='connection info')
    parser.add_argument('--upstream-uri', type=str, default='http://10.100.36.179:19530', help='milvus uri')
    parser.add_argument('--downstream-uri', type=str, default='http://10.100.36.178:19530', help='milvus uri')
    parser.add_argument('--upstream-token', type=str, default='root:Milvus', help='milvus token')
    parser.add_argument('--downstream-token', type=str, default='root:Milvus', help='milvus token')
    args = parser.parse_args()
    diff_cnt = 0
    diff = None
    t0 = time.time()
    while diff_cnt < 10:
        upstream = get_cluster_info(args.upstream_uri, args.upstream_token)
        downstream = get_cluster_info(args.downstream_uri, args.downstream_token)
        diff = DeepDiff(upstream, downstream)
        diff = convert_deepdiff(diff)
        logger.info(f"diff: {diff}")
        logger.info(f"diff: {json.dumps(diff, indent=2)}")
        with open("diff.json", "w") as f:
            json.dump(diff, f, indent=2)
        excludedRegex = [r"root(\[\'\w+\'\])*\['num_entities'\]"]
        diff = DeepDiff(upstream, downstream, exclude_regex_paths=excludedRegex)
        diff = convert_deepdiff(diff)
        logger.info(f"diff exclude num entities: {diff}")
        logger.info(f"diff exclude num entities: {json.dumps(diff, indent=2)}")
        diff_cnt += 1
        if diff:
            logger.info(f"diff exclude num entities found between upstream and downstream {json.dumps(diff, indent=2)}")
            time.sleep(60)

        else:
            logger.info("no diff exclude num entities found between upstream and downstream")
            break
    tt = time.time() - t0
    logger.info(f"total time cost: {tt:.2f} seconds")
    if diff:
        assert False, f"diff found between upstream and downstream {json.dumps(diff, indent=2)}"
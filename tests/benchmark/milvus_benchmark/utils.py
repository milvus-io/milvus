# -*- coding: utf-8 -*-
import time
import logging
import string
import random
from yaml import full_load, dump
import yaml
import tableprint as tp
from pprint import pprint
import config

logger = logging.getLogger("milvus_benchmark.utils")


def timestr_to_int(time_str):
    """ Parse the test time set in the yaml configuration file and convert it to int type """
    time_int = 0
    if isinstance(time_str, int) or time_str.isdigit():
        time_int = int(time_str)
    elif time_str.endswith("s"):
        time_int = int(time_str.split("s")[0])
    elif time_str.endswith("m"):
        time_int = int(time_str.split("m")[0]) * 60
    elif time_str.endswith("h"):
        time_int = int(time_str.split("h")[0]) * 60 * 60
    else:
        raise Exception("%s not support" % time_str)
    return time_int


class literal_str(str): pass


def change_style(style, representer):
    def new_representer(dumper, data):
        scalar = representer(dumper, data)
        scalar.style = style
        return scalar

    return new_representer


from yaml.representer import SafeRepresenter

# represent_str does handle some corner cases, so use that
# instead of calling represent_scalar directly
represent_literal_str = change_style('|', SafeRepresenter.represent_str)

yaml.add_representer(literal_str, represent_literal_str)


def retry(times):
    """
    This decorator prints the execution time for the decorated function.
    """
    def wrapper(func):
        def newfn(*args, **kwargs):
            attempt = 0
            while attempt < times:
                try:
                    result = func(*args, **kwargs)
                    if result:
                        break
                    else:
                        raise Exception("Result false")
                except Exception as e:
                    logger.info(str(e))
                    time.sleep(3)
                    attempt += 1
            return result
        return newfn
    return wrapper


def convert_nested(dct):
    def insert(dct, lst):
        for x in lst[:-2]:
            dct[x] = dct = dct.get(x, dict())
        dct.update({lst[-2]: lst[-1]})

        # empty dict to store the result

    result = dict()

    # create an iterator of lists  
    # representing nested or hierarchial flow 
    lsts = ([*k.split("."), v] for k, v in dct.items())

    # insert each list into the result 
    for lst in lsts:
        insert(result, lst)
    return result


def get_unique_name(prefix=None):
    if prefix is None:
        prefix = "distributed-benchmark-test-"
    return prefix + "".join(random.choice(string.ascii_letters + string.digits) for _ in range(8)).lower()


def get_current_time():
    return time.strftime('%Y-%m-%d %H:%M:%S', time.localtime())


def print_table(headers, columns, data):
    bodys = []
    for index, value in enumerate(columns):
        tmp = [value]
        tmp.extend(data[index])
        bodys.append(tmp)
    tp.table(bodys, headers)


def get_deploy_mode(deploy_params):
    """
    Get the server deployment mode set in the yaml configuration file
    single, cluster, cluster_3rd
    """
    deploy_mode = None
    if deploy_params:
        milvus_params = None
        if "milvus" in deploy_params:
            milvus_params = deploy_params["milvus"]
        if not milvus_params:
            deploy_mode = config.DEFUALT_DEPLOY_MODE
        elif "deploy_mode" in milvus_params:
            deploy_mode = milvus_params["deploy_mode"]
            if deploy_mode not in [config.SINGLE_DEPLOY_MODE, config.CLUSTER_DEPLOY_MODE]:
                raise Exception("Invalid deploy mode: %s" % deploy_mode)
    return deploy_mode


def get_server_tag(deploy_params):
    """
    Get service deployment configuration
    e.g.:
        server:
          server_tag: "8c16m"
    """
    server_tag = ""
    if deploy_params and "server" in deploy_params:
        server = deploy_params["server"]
        # server_name = server["server_name"] if "server_name" in server else ""
        server_tag = server["server_tag"] if "server_tag" in server else ""
    return server_tag


def search_param_analysis(vector_query, filter_query):

    if "vector" in vector_query:
        vector = vector_query["vector"]
    else:
        logger.error("[search_param_analysis] vector not in vector_query")
        return False

    data = []
    anns_field = ""
    param = {}
    limit = 1
    if isinstance(vector, dict) and len(vector) == 1:
        for key in vector:
            anns_field = key
            data = vector[key]["query"]
            param = {"metric_type": vector[key]["metric_type"],
                     "params": vector[key]["params"]}
            limit = vector[key]["topk"]
    else:
        logger.error("[search_param_analysis] vector not dict or len != 1: %s" % str(vector))
        return False

    if isinstance(filter_query, list) and len(filter_query) != 0 and "range" in filter_query[0]:
        filter_range = filter_query[0]["range"]
        if isinstance(filter_range, dict) and len(filter_range) == 1:
            for key in filter_range:
                field_name = filter_range[key]
                expression = None
                if 'GT' in filter_range[key]:
                    exp1 = "%s > %s" % (field_name, str(filter_range[key]['GT']))
                    expression = exp1
                if 'LT' in filter_range[key]:
                    exp2 = "%s < %s" % (field_name, str(filter_range[key]['LT']))
                    if expression:
                        expression = expression + ' && ' + exp2
                    else:
                        expression = exp2

        else:
            logger.error("[search_param_analysis] filter_range not dict or len != 1: %s" % str(filter_range))
            return False
    else:
        # logger.debug("[search_param_analysis] range not in filter_query: %s" % str(filter_query))
        expression = None

    result = {
        "data": data,
        "anns_field": anns_field,
        "param": param,
        "limit": limit,
        "expression": expression
    }
    # logger.debug("[search_param_analysis] search_param_analysis: %s" % str(result))
    return result

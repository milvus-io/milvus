# -*- coding: utf-8 -*-
import os
import sys
import pdb
import time
import json
import datetime
import argparse
import threading
import logging
import string
import random
# import multiprocessing
import numpy as np
# import psutil
import sklearn.preprocessing
# import docker
from yaml import full_load, dump
import yaml
import tableprint as tp
from pprint import pprint
from pymilvus import DataType

logger = logging.getLogger("milvus_benchmark.utils")


def timestr_to_int(time_str):
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


def modify_config(k, v, type=None, file_path="conf/server_config.yaml", db_slave=None):
    if not os.path.isfile(file_path):
        raise Exception('File: %s not found' % file_path)
    with open(file_path) as f:
        config_dict = full_load(f)
        f.close()
    if config_dict:
        if k.find("use_blas_threshold") != -1:
            config_dict['engine_config']['use_blas_threshold'] = int(v)
        elif k.find("use_gpu_threshold") != -1:
            config_dict['engine_config']['gpu_search_threshold'] = int(v)
        elif k.find("cpu_cache_capacity") != -1:
            config_dict['cache_config']['cpu_cache_capacity'] = int(v)
        elif k.find("enable_gpu") != -1:
            config_dict['gpu_resource_config']['enable'] = v
        elif k.find("gpu_cache_capacity") != -1:
            config_dict['gpu_resource_config']['cache_capacity'] = int(v)
        elif k.find("index_build_device") != -1:
            config_dict['gpu_resource_config']['build_index_resources'] = v
        elif k.find("search_resources") != -1:
            config_dict['resource_config']['resources'] = v

        # if db_slave:
        #     config_dict['db_config']['db_slave_path'] = MULTI_DB_SLAVE_PATH
        with open(file_path, 'w') as f:
            dump(config_dict, f, default_flow_style=False)
        f.close()
    else:
        raise Exception('Load file:%s error' % file_path)


# update server_config.yaml
def update_server_config(file_path, server_config):
    if not os.path.isfile(file_path):
        raise Exception('File: %s not found' % file_path)
    with open(file_path) as f:
        values_dict = full_load(f)
        f.close()
        for k, v in server_config.items():
            if k.find("primary_path") != -1:
                values_dict["db_config"]["primary_path"] = v
            elif k.find("use_blas_threshold") != -1:
                values_dict['engine_config']['use_blas_threshold'] = int(v)
            elif k.find("gpu_search_threshold") != -1:
                values_dict['engine_config']['gpu_search_threshold'] = int(v)
            elif k.find("cpu_cache_capacity") != -1:
                values_dict['cache_config']['cpu_cache_capacity'] = int(v)
            elif k.find("cache_insert_data") != -1:
                values_dict['cache_config']['cache_insert_data'] = v
            elif k.find("enable") != -1:
                values_dict['gpu_resource_config']['enable'] = v
            elif k.find("gpu_cache_capacity") != -1:
                values_dict['gpu_resource_config']['cache_capacity'] = int(v)
            elif k.find("build_index_resources") != -1:
                values_dict['gpu_resource_config']['build_index_resources'] = v
            elif k.find("search_resources") != -1:
                values_dict['gpu_resource_config']['search_resources'] = v
            with open(file_path, 'w') as f:
                dump(values_dict, f, default_flow_style=False)
            f.close()

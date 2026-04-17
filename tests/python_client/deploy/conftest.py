import functools
import logging
import socket

import pytest
from pymilvus.orm.types import CONSISTENCY_STRONG

import common.common_func as cf
import common.common_type as ct
from check.param_check import ip_check, number_check
from common.common_func import param_info
from config.log_config import log_config
from utils.util_log import test_log as log
from utils.util_pymilvus import gen_binary_default_fields, gen_default_fields, gen_unique_str, get_milvus

timeout = 60
dimension = 128
delete_timeout = 60


# add a fixture for all index?

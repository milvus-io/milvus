import time
import pdb
import threading
import logging
from multiprocessing import Pool, Process
import pytest
from milvus import IndexType, MetricType
from utils import *


dim = 128
index_file_size = 50
collection_id = "test_loop"
DELETE_TIMEOUT = 60
nprobe = 1
tag = "1970-01-01"
top_k = 5
nb = 6000
tag = "partition_tag"


def init_data(connect):
    # create two collections and add vectors into it
    pass


def get_collections(connect):
    collections = []
    return collections


def insert(connect):
    pass


def search(connect):
    pass


def flush(connect):
    pass


def delete(connect):
    pass


def compact(connect):
    pass


def 
tasks = [insert, search, flush, delete, compact]


class TestLoop:
    """
    ******************************************************************
      This case would be run in loop, until result returned wrong or 
      server connected failed. 
      The steps:
      1. create collection and data initialization
      2. random choice from tasks, including search/insert/delete/
         flush/compact/drop_index/create_index ..., assert result
      3. run step2 in loop
    ******************************************************************
    """

    def test_random_opt_in_loop(self, connect, args):
        init()

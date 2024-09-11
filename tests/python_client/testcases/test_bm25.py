import utils.util_pymilvus as ut
from utils.util_log import test_log as log
from common.common_type import CaseLabel, CheckTasks
from common import common_type as ct
from common import common_func as cf
from common.code_mapping import CollectionErrorMessage as clem
from common.code_mapping import ConnectionErrorMessage as cem
from base.client_base import TestcaseBase
from pymilvus import (
    connections, list_collections,
    FieldSchema, CollectionSchema, DataType, Function, FunctionType,
    Collection
)
from pymilvus.orm.types import CONSISTENCY_STRONG, CONSISTENCY_BOUNDED, CONSISTENCY_EVENTUALLY
import threading
from pymilvus import DefaultConfig
from datetime import datetime
import time
import nltk
from nltk.tokenize import word_tokenize
from collections import Counter
from datasets import load_dataset
import pytest
import random
import numpy as np
import pandas as pd
import ast
pd.set_option("expand_frame_repr", False)




class TestBM25Search(TestcaseBase):
    """
    ******************************************************************
      The following cases are used to test query iterator
    ******************************************************************
    """

    @pytest.mark.tags(CaseLabel.L0)
    def test_bm25_search_normal(self):
        """
        target: test query iterator normal
        method: 1. query iterator
                2. check the result, expect pk
        expected: query successfully
        """
        # 1. initialize with data
        prefix = "test_bm25_search_normal"
        analyzer_params = {
            "tokenizer": "default",
        }
        dim = 128
        default_fields = [
            FieldSchema(name="id", dtype=DataType.INT64, is_primary=True),
            FieldSchema(name="title", dtype=DataType.VARCHAR, max_length=65535, enable_match=True, analyzer_params=analyzer_params),
            FieldSchema(name="overview", dtype=DataType.VARCHAR, max_length=65535, enable_match=True, analyzer_params=analyzer_params),
            FieldSchema(name="genres", dtype=DataType.VARCHAR, max_length=65535, enable_match=True, analyzer_params=analyzer_params),
            FieldSchema(name="producer", dtype=DataType.VARCHAR, max_length=65535, enable_match=True, analyzer_params=analyzer_params),
            FieldSchema(name="cast", dtype=DataType.VARCHAR, max_length=65535, enable_match=True, analyzer_params=analyzer_params),
            FieldSchema(name="overview_sparse", dtype=DataType.SPARSE_FLOAT_VECTOR),
            FieldSchema(name="emb", dtype=DataType.FLOAT_VECTOR, dim=dim)
        ]
        bm25_function = Function(
            name="bm25",
            function_type=FunctionType.BM25,
            inputs=["document"],
            outputs=["sparse"],
            params={"bm25_k1": 1.2, "bm25_b": 0.75},
        )
        default_schema = CollectionSchema(fields=default_fields, description="test collection")
        default_schema.add_function(bm25_function)
        print(f"\nCreate collection for movie dataset...")

        collection_w = self.init_collection_wrap(name=cf.gen_unique_str(prefix), schema=default_schema)
        df = pd.read_parquet("hf://datasets/Cohere/movies/movies.parquet")
        log.info(f"dataframe\n{df}")
        df = df.astype(str)
        log.info(f"dataframe\n{df}")
        data = []
        for i in range(len(df)):
            d = {
                "id": i,
                "title": str(df["title"][i]),
                "overview": str(df["overview"][i]),
                "genres": str(df["genres"][i]),
                "producer": str(df["producer"][i]),
                "cast": str(df["cast"][i]),
                "emb": cf.gen_vectors(1, dim)[0]
            }
            data.append(d)
        log.info(f"data\n{data[:10]}")
        batch_size = 5000
        for i in range(0, len(df), batch_size):
            collection_w.insert(data[i: i + batch_size] if i + batch_size < len(df) else data[i: len(df)])
            collection_w.flush()
        collection_w.create_index("emb", {"index_type": "IVF_SQ8", "metric_type": "L2", "params": {"nlist": 64}})
        index = {
            "index_type": "SPARSE_INVERTED_INDEX",
            "metric_type": "BM25",
            'params': {"bm25_k1": 1.2, "bm25_b": 0.75},
        }
        collection_w.create_index("overview_sparse", index)
        collection_w.load()
        search_params = {
            "metric_type": "BM25",
            "params": {},
        }
        texts_to_search = df["overview"].tolist()[:10]
        res, _ = collection_w.search(texts_to_search, "sparse", search_params, limit=3, output_fields=["overview", "id"],
                                     consistency_level="Strong")
        log.info(f"res: {res}")

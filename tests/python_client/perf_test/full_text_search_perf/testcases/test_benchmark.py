from common.common_type import CaseLabel
from common import common_func as cf
from base.client_base import TestcaseBase
import pytest
import beir.util
import time
import pandas as pd
from faker import Faker
from utils.util_log import test_log as log
import bm25s
from tqdm import tqdm

tqdm.disable = True

from pymilvus import (
    utility,
    FieldSchema, CollectionSchema, Function, DataType, FunctionType,
    Collection,
)

import beir.util
from beir.retrieval.search.lexical import BM25Search
from beir.datasets.data_loader import GenericDataLoader
from beir.retrieval.evaluation import EvaluateRetrieval

fake = Faker()

Faker.seed(19530)
fake_en = Faker("en_US")
fake_zh = Faker("zh_CN")
pd.set_option("expand_frame_repr", False)

prefix = "full_text_search_collection"


def milvus_full_text_search(collection_name, corpus, queries, qrels, top_k=1000, tokenizer="default",
                            index_type="SPARSE_INVERTED_INDEX"):
    corpus_ids, corpus_lst = [], []
    for key, val in corpus.items():
        corpus_ids.append(key)
        doc = val["title"] + " " + val["text"]
        if len(doc) > 25536:
            doc = doc[:25000]
        corpus_lst.append(doc)
    qids, queries_lst = [], []
    for key, val in queries.items():
        qids.append(key)
        queries_lst.append(val)
    corpus_data = [
        {"id": str(corpus_id), "document": doc}
        for corpus_id, doc in zip(corpus_ids, corpus_lst)
    ]
    query_data = [
        {"id": str(qid), "document": query}
        for qid, query in zip(qids, queries_lst)
    ]
    df = pd.DataFrame(corpus_data)
    log.info(f"Corpus data: \n{df}")
    df = pd.DataFrame(query_data)
    log.info(f"Query data: \n{df}")

    has = utility.has_collection(collection_name)
    if has:
        log.info(f"Collection {collection_name} already exists, will drop it")
        utility.drop_collection(collection_name)
    tokenizer_params = {
        "tokenizer": tokenizer,
    }
    fields = [
        FieldSchema(name="id", dtype=DataType.VARCHAR, max_length=10000, is_primary=True),
        FieldSchema(name="document", dtype=DataType.VARCHAR, max_length=25536,
                    enable_tokenizer=True, tokenizer_params=tokenizer_params, ),
        FieldSchema(name="sparse", dtype=DataType.SPARSE_FLOAT_VECTOR),
    ]
    schema = CollectionSchema(fields=fields, description="beir test collection")
    bm25_function = Function(
        name="text_bm25_emb",
        function_type=FunctionType.BM25,
        input_field_names=["document"],
        output_field_names=["sparse"],
        params={},
    )
    schema.add_function(bm25_function)
    hello_bm25 = Collection(collection_name, schema, consistency_level="Strong")
    log.info(f"Collection {collection_name} created successfully, start to insert data")
    batch_size = 5000
    for i in tqdm(range(0, len(corpus_data), batch_size), desc="Inserting data"):
        hello_bm25.insert(corpus_data[i:i + batch_size])
    hello_bm25.flush()
    log.info(f"Data inserted successfully, start to create index")
    hello_bm25.create_index(
        "sparse",
        {
            "index_type": index_type,
            "metric_type": "BM25",
            "params": {
                "bm25_k1": 1.5,
                "bm25_b": 0.75,
            }
        }
    )
    hello_bm25.load()
    log.info(f"Index created successfully, start to search")
    texts_to_search = [q["document"] for q in query_data]
    search_params = {
        "metric_type": "BM25",
        "params": {},
    }
    start_time = time.time()
    result_list = []
    q_batch_size = 1000
    for i in range(0, len(texts_to_search), q_batch_size):
        log.info(f"Searching {i} to {i + q_batch_size}")
        t0 = time.time()
        result = hello_bm25.search(texts_to_search[i:i + q_batch_size], "sparse", search_params, limit=top_k,
                                   output_fields=["id"],
                                   consistency_level="Strong")
        tt = time.time() - t0
        log.info(f"Search time: {tt}")
        result_list.extend(result)
    end_time = time.time()
    log.info(f"Search finished, cost time: {end_time - start_time}")
    result_dict = {}
    for i in range(len(query_data)):
        data = {}
        for hit in result_list[i]:
            data[hit.id] = hit.distance
        result_dict[query_data[i]["id"]] = data

    ndcg, _map, recall, precision = EvaluateRetrieval.evaluate(
        qrels, result_dict, [1, 10, 100, 1000]
    )
    log.info(f"Milvus full text search NDCG: {ndcg}, MAP: {_map}, Recall: {recall}, Precision: {precision}")
    hello_bm25.release()
    return ndcg, _map, recall, precision


def postprocess_results_for_eval(results, scores, query_ids):
    """
    Given the queried results and scores output by BM25S, postprocess them
    to be compatible with BEIR evaluation functions.
    query_ids is a list of query ids in the same order as the results.
    """

    results_record = [
        {"id": qid, "hits": results[i], "scores": list(scores[i])}
        for i, qid in enumerate(query_ids)
    ]

    result_dict_for_eval = {
        res["id"]: {
            docid: float(score) for docid, score in zip(res["hits"], res["scores"])
        }
        for res in results_record
    }

    return result_dict_for_eval


def Lucene_full_text_search(corpus, queries, qrels, top_k=1000):
    corpus_ids, corpus_lst = [], []
    for key, val in corpus.items():
        corpus_ids.append(key)
        corpus_lst.append(val["title"] + " " + val["text"])
    qids, queries_lst = [], []
    for key, val in queries.items():
        qids.append(key)
        queries_lst.append(val)
    corpus_data = [
        {"id": str(corpus_id), "document": doc}
        for corpus_id, doc in zip(corpus_ids, corpus_lst)
    ]
    query_data = [
        {"id": str(qid), "document": query}
        for qid, query in zip(qids, queries_lst)
    ]
    corpus_ids = [corpus_data[i]["id"] for i in range(len(corpus_data))]
    q_ids = [query_data[i]["id"] for i in range(len(query_data))]
    corpus_list = [corpus_data[i]["document"] for i in range(len(corpus_data))]
    queries_list = [query_data[i]["document"] for i in range(len(query_data))]
    corpus_tokens = bm25s.tokenize(corpus_list, leave=False)
    query_tokens = bm25s.tokenize(queries_list, leave=False)
    model = bm25s.BM25(method="lucene", k1=1.5, b=0.75)
    model.index(corpus_tokens, leave_progress=False)
    t0 = time.time()
    queried_results, queried_scores = model.retrieve(
        query_tokens, corpus=corpus_ids, k=top_k, n_threads=4
    )
    tt = time.time() - t0
    log.info(f"BM25s Search time: {tt}")
    lucene_results_dict = postprocess_results_for_eval(queried_results, queried_scores, q_ids)
    ndcg, _map, recall, precision = EvaluateRetrieval.evaluate(
        qrels, lucene_results_dict, [1, 10, 100, 1000]
    )
    log.info(f"Lucene NDCG: {ndcg}, MAP: {_map}, Recall: {recall}, Precision: {precision}")

    return ndcg, _map, recall, precision


def es_full_text_search(corpus, queries, qrels, top_k=1000, index_name="hello", hostname="localhost", k1=1.5, b=0.75):
    num_docs = len(corpus)
    num_queries = len(queries)

    print("=" * 50)
    print(f"Corpus Size: {num_docs:,}")
    print(f"Queries Size: {num_queries:,}")
    model = BM25Search(
        index_name=index_name, hostname=hostname, language="english", number_of_shards=1, initialize=False
    )
    es_bm25_settings = {
        "settings": {
            "index": {
                "similarity": {
                    "default": {
                        "type": "BM25",
                        "k1": k1,
                        "b": b,
                    }
                }
            },
            "analysis": {
                "analyzer": {
                    "custom_analyzer": {
                        "type": "standard",
                        "max_token_length": 1_000_000,
                        "stopwords": "_english_",
                        "filter": ["lowercase", "custom_snowball"]
                    }
                },
                "filter": {
                    "custom_snowball": {
                        "type": "snowball",
                        "language": "English"
                    }
                }
            }
        }
    }
    model.initialise()
    model.index(corpus)
    model.es.es.indices.close(index=index_name)
    model.es.es.indices.put_settings(index=index_name, body=es_bm25_settings)
    model.es.es.indices.open(index=index_name)
    t0 = time.time()
    results = model.search(corpus=corpus, queries=queries, top_k=top_k)
    tt = time.time() - t0
    log.info(f"ES Search time: {tt}")
    ndcg, _map, recall, precision = EvaluateRetrieval.evaluate(
        qrels, results, [1, 10, 100, 1000]
    )
    log.info(f"ES NDCG: {ndcg}, MAP: {_map}, Recall: {recall}, Precision: {precision}")

    return ndcg, _map, recall, precision


class TestSearchWithFullTextSearchBenchmark(TestcaseBase):
    """
    target: test full text search
    method: 1. enable full text search and insert data with varchar
            2. search with text
            3. verify the result
    expected: full text search successfully and result is correct
    """

    @pytest.mark.tags(CaseLabel.L3)
    @pytest.mark.parametrize("index_type", ["SPARSE_INVERTED_INDEX"])
    @pytest.mark.parametrize("dataset",
                             [
                                 "msmarco",
                                 "trec-covid",
                                 "nfcorpus",
                                 "nq",
                                 "hotpotqa",
                                 "fiqa",
                                 "arguana",
                                 "webis-touche2020",
                                 "quora",
                                 "dbpedia-entity",
                                 "scidocs",
                                 "fever",
                                 "climate-fever",
                                 "scifact"
                             ])
    def test_search_with_full_text_search(self, dataset, index_type, es_host, dataset_dir):
        self._connect()
        BASE_URL = f"https://public.ukp.informatik.tu-darmstadt.de/thakur/BEIR/datasets/{dataset}.zip"
        data_path = beir.util.download_and_unzip(BASE_URL, out_dir=dataset_dir)
        split = "test" if dataset != "msmarco" else "dev"
        corpus, queries, qrels = GenericDataLoader(data_folder=data_path).load(split=split)
        collection_name = dataset.replace("-", "_") + "_full_text_search"  # collection name should not contain "-"
        top_k = 1000
        milvus_full_text_search_result = milvus_full_text_search(collection_name, corpus, queries, qrels,
                                                                 top_k=top_k, index_type=index_type)
        es_full_text_search_result = es_full_text_search(corpus, queries, qrels, top_k=top_k,
                                                         index_name=collection_name, hostname=es_host)
        log.info(f"result for dataset {dataset}")
        log.info(f"milvus full text search result {milvus_full_text_search_result}")
        log.info(f"es full text search result {es_full_text_search_result}")

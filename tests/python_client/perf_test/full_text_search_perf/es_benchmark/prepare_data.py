import requests
import json
import pandas as pd
import time
from faker import Faker
import argparse
fake_en = Faker('en_US')


class ElasticsearchBM25Search:
    def __init__(self, host='localhost', port=9200, index_name='my_index'):

        self.base_url = f'http://{host}:{port}'
        self.index_name = index_name
        self.delete_index(index_name)

    def create_index(self, settings=None):
        if settings is None:
            settings = {
                "settings": {
                    "index": {
                        "similarity": {
                            "default": {
                                "type": "BM25",
                                "k1": 1.2,
                                "b": 0.75
                            }
                        }
                    }
                },
                "mappings": {
                    "properties": {
                        "id": {
                            "type": "keyword"
                        },
                        "text": {
                            "type": "text",
                            "similarity": "default",
                            "analyzer": "english"
                        }
                    }
                }
            }

        url = f'{self.base_url}/{self.index_name}'
        response = requests.put(url, json=settings)
        return response.json()

    def search(self, query_text, size=10):
        search_body = {
            "query": {
                "match": {
                    "text": query_text
                }
            },
            "size": size
        }

        url = f'{self.base_url}/{self.index_name}/_search'
        response = requests.post(url, json=search_body)
        return response.json()

    def bulk_index(self, documents):
        bulk_data = []
        for doc in documents:
            bulk_data.append(json.dumps({
                "index": {
                    "_id": doc['id']
                }
            }))
            bulk_data.append(json.dumps({
                "id": doc['id'],
                "text": doc['text']
            }))

        bulk_data = "\n".join(bulk_data) + "\n"
        url = f'{self.base_url}/{self.index_name}/_bulk'
        headers = {'Content-Type': 'application/x-ndjson'}
        response = requests.post(url, data=bulk_data, headers=headers)
        return response.json()

    def delete_index(self, index_name=None):
        if index_name is None:
            index_name = self.index_name

        url = f'{self.base_url}/{index_name}'
        response = requests.delete(url)
        print(response)
        return response.json()


if __name__ == '__main__':
    args = argparse.ArgumentParser()
    args.add_argument('--host', type=str, default='localhost')
    args.add_argument('--port', type=int, default=9200)
    args.add_argument('--index_name', type=str, default='wiki_dataset_benchmark')
    args.add_argument('--data_size', type=int, default=1000_000)
    args = args.parse_args()
    host = args.host
    port = args.port
    index_name = args.index_name
    data_size = args.data_size

    es_search = ElasticsearchBM25Search(host=host, port=port, index_name=index_name)

    es_search.create_index()
    b_z = 100_000
    file_num = data_size // b_z
    data_dir = "/root/dataset/cohere_database/wikipedia-22-12-en-embeddings-parquet"
    batch_files = [f"/dataset/wikipedia-22-12-en-text/parquet-train-{i:05d}-of-00252.parquet" for i in range(file_num)]

    for f in batch_files:
        df = pd.read_parquet(f)
        df = df[['id', 'text']]
        documents = df.to_dict('records')
        batch_size = 10000

        for start in range(0, len(documents), batch_size):
            t0 = time.time()
            batch_documents = documents[start: start + batch_size]
            es_search.bulk_index(batch_documents)
            tt = time.time() - t0
            print(f"insert {batch_size} data costs {tt}s")

    # search for verification
    test_data = pd.read_parquet("/dataset/wikipedia-22-12-en-text/test.parquet")
    results = es_search.search(fake_en.text().lower(), size=100)
    print(results)

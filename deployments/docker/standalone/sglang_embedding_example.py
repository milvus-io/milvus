#!/usr/bin/env python3

# Licensed to the LF AI & Data foundation under one
# or more contributor license agreements. See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership. The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License. You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Create, populate, and search a collection backed by SGLang embeddings."""

from pymilvus import (
    Collection,
    CollectionSchema,
    DataType,
    FieldSchema,
    Function,
    FunctionType,
    connections,
    utility,
)

COLLECTION_NAME = "sglang_embedding_example"
MODEL_NAME = "Qwen/Qwen3-Embedding-0.6B"
EMBEDDING_DIM = 1024


def main() -> None:
    connections.connect(alias="default", host="localhost", port="19530")

    if utility.has_collection(COLLECTION_NAME):
        utility.drop_collection(COLLECTION_NAME)

    fields = [
        FieldSchema(name="id", dtype=DataType.INT64, is_primary=True, auto_id=False),
        FieldSchema(name="text", dtype=DataType.VARCHAR, max_length=65535),
        FieldSchema(name="embedding", dtype=DataType.FLOAT_VECTOR, dim=EMBEDDING_DIM),
    ]
    schema = CollectionSchema(fields=fields)
    schema.add_function(
        Function(
            name="sglang_embedding",
            function_type=FunctionType.TEXTEMBEDDING,
            input_field_names=["text"],
            output_field_names="embedding",
            params={"provider": "openai", "model_name": MODEL_NAME},
        )
    )

    collection = Collection(COLLECTION_NAME, schema=schema)
    collection.insert(
        [
            {"id": 1, "text": "Milvus is a vector database."},
            {"id": 2, "text": "SGLang serves embedding models."},
            {"id": 3, "text": "Vector search finds related text."},
        ]
    )
    collection.flush()
    collection.create_index(
        field_name="embedding",
        index_params={"index_type": "AUTOINDEX", "metric_type": "COSINE", "params": {}},
    )
    collection.load()

    results = collection.search(
        data=["Which system performs vector search?"],
        anns_field="embedding",
        param={"metric_type": "COSINE", "params": {}},
        limit=2,
        output_fields=["text"],
    )
    for hit in results[0]:
        print(f"id={hit.id} distance={hit.distance:.4f} text={hit.entity.get('text')}")


if __name__ == "__main__":
    main()

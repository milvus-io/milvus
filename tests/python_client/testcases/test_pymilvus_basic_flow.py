import uuid

import pytest
from pymilvus import DataType, MilvusClient

NAMESPACE_COUNT = 10


def _client_uri(uri, host, port):
    return uri or f"http://{host}:{port}"


def _namespace_name(index):
    return f"ns_{index:02d}"


def _rows(namespace_index):
    base_id = namespace_index * 100
    return [
        {
            "id": base_id + 1,
            "vector": [0.1, 0.2, 0.3, 0.4],
            "score": namespace_index,
            "tag": "shared",
        },
        {
            "id": base_id + 2,
            "vector": [0.2, 0.3, 0.4, 0.5],
            "score": namespace_index,
            "tag": "shared",
        },
    ]


def _expected_query_rows(namespace_index):
    base_id = namespace_index * 100
    return [
        {"id": base_id + 1, "score": namespace_index, "tag": "shared"},
        {"id": base_id + 2, "score": namespace_index, "tag": "shared"},
    ]


@pytest.mark.tags("L0")
def test_pymilvus_namespace_create_insert_create_index_load_query(host, port, uri, token):
    client = MilvusClient(uri=_client_uri(uri, host, port), token=token)
    collection_name = f"pymilvus_ns_{uuid.uuid4().hex[:8]}"

    try:
        schema = client.create_schema(
            auto_id=False,
            enable_dynamic_field=False,
            enable_namespace=True,
        )
        schema.add_field("id", DataType.INT64, is_primary=True)
        schema.add_field("vector", DataType.FLOAT_VECTOR, dim=4)
        schema.add_field("score", DataType.INT64)
        schema.add_field("tag", DataType.VARCHAR, max_length=64)

        client.create_collection(
            collection_name=collection_name,
            schema=schema,
            properties={"namespace.mode": "partition"},
            consistency_level="Strong",
        )

        namespace_names = [_namespace_name(index) for index in range(NAMESPACE_COUNT)]
        for namespace_name in namespace_names:
            client.create_namespace(
                collection_name=collection_name,
                namespace_name=namespace_name,
            )

        listed_namespaces = client.list_namespaces(collection_name=collection_name)["namespaces"]
        assert set(namespace_names).issubset(set(listed_namespaces))

        for namespace_index, namespace_name in enumerate(namespace_names):
            insert_result = client.insert(
                collection_name=collection_name,
                data=_rows(namespace_index),
                namespace=namespace_name,
            )
            assert insert_result["insert_count"] == 2

        index_params = client.prepare_index_params()
        index_params.add_index(field_name="vector", index_type="AUTOINDEX", metric_type="COSINE")
        client.create_index(collection_name=collection_name, index_params=index_params)
        assert "vector" in client.list_indexes(collection_name=collection_name)

        client.flush(collection_name=collection_name)
        client.load_collection(collection_name=collection_name)

        for namespace_index, namespace_name in enumerate(namespace_names):
            results = client.query(
                collection_name=collection_name,
                filter='tag == "shared"',
                output_fields=["id", "score", "tag"],
                namespace=namespace_name,
            )
            assert sorted(results, key=lambda row: row["id"]) == _expected_query_rows(
                namespace_index
            )
    finally:
        if client.has_collection(collection_name=collection_name):
            client.drop_collection(collection_name=collection_name)
        client.close()

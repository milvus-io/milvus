"""Create or reuse a dedicated collection and write its Search baseline.

Set PROXY_A_URI, MILVUS_TOKEN, and MILVUS_L0_COLLECTION before running this
script. MILVUS_L0_ROWS only applies to new collections. Keep the collection
read-only after setup. Use the same collection, MILVUS_L0_NQ, and optional
MILVUS_L0_BASELINE in setup and pytest.
"""

import argparse
import os
from pathlib import Path
from tempfile import NamedTemporaryFile

VECTOR_DIMENSION = 128
DEFAULT_ROWS = 100_000
BATCH_SIZE = 5_000
DEFAULT_NQ = 16384
TOPK = 10
BASELINE_FORMAT = "v2:default:vector:L2:topk10"


def search(client, collection, vectors, *, timeout=90):
    return client.search(
        collection,
        vectors,
        anns_field="vector",
        limit=TOPK,
        search_params={"metric_type": "L2"},
        timeout=timeout,
    )


def search_hits(response, nq, topk):
    assert len(response) == nq
    assert all(len(hits) == topk for hits in response)
    return [sorted((hit["id"], hit["distance"]) for hit in hits) for hits in response]


def baseline_path(collection):
    return Path(
        os.environ.get(
            "MILVUS_L0_BASELINE",
            Path(__file__).parent / ".baselines" / f"{collection}.npz",
        )
    )


def baseline_identity(client, collection):
    schema = client.describe_collection(collection, timeout=10)
    rows = int(client.get_collection_stats(collection, timeout=10)["row_count"])
    return (collection, str(schema["collection_id"]), rows)


def load_baseline(client, collection):
    import numpy as np

    path = baseline_path(collection)
    if not path.is_file():
        raise ValueError(f"Search baseline missing: {path}. Run setup_running_requests_l0.py first.")
    nq = int(os.environ.get("MILVUS_L0_NQ", DEFAULT_NQ))
    with np.load(path, allow_pickle=False) as data:
        if (
            tuple(data["identity"].tolist()) != tuple(map(str, baseline_identity(client, collection)))
            or data["vectors"].shape != (nq, VECTOR_DIMENSION)
            or data["query"].item() != BASELINE_FORMAT
        ):
            raise ValueError(f"Baseline does not match {collection!r} or its query; regenerate {path}.")
        baselines = {}
        for size in sorted({1, 2, nq}):
            ids, distances = data[f"ids_{size}"], data[f"distances_{size}"]
            if ids.shape != (size, TOPK) or distances.shape != (size, TOPK):
                raise ValueError(f"Invalid nq={size} baseline in {path}; regenerate it.")
            baselines[size] = [list(zip(pk, ds)) for pk, ds in zip(ids.tolist(), distances.tolist())]
        return data["vectors"], baselines


def write_baseline(client, collection):
    import numpy as np

    nq = int(os.environ.get("MILVUS_L0_NQ", DEFAULT_NQ))
    if not 2 <= nq <= 16384:
        raise ValueError("MILVUS_L0_NQ must be in [2, 16384]")
    identity = baseline_identity(client, collection)
    vectors = np.random.default_rng(1113).random((nq, VECTOR_DIMENSION), dtype=np.float32)
    arrays = {}
    for size in sorted({1, 2, nq}):
        hits = search_hits(search(client, collection, vectors[:size]), size, TOPK)
        arrays[f"ids_{size}"] = np.array([[pk for pk, _ in row] for row in hits], dtype=np.int64)
        arrays[f"distances_{size}"] = np.array([[ds for _, ds in row] for row in hits], dtype=np.float64)
    path = baseline_path(collection)
    path.parent.mkdir(parents=True, exist_ok=True)
    with NamedTemporaryFile(dir=path.parent, delete=False) as f:
        np.savez(f, identity=tuple(map(str, identity)), query=BASELINE_FORMAT, vectors=vectors, **arrays)
    Path(f.name).replace(path)
    return path


def create_collection(client, name, rows, *, on_created=None):
    """Create, populate, index, and load a fresh INT64/FLOAT_VECTOR collection."""
    import numpy as np
    from pymilvus import DataType

    if rows < 10:
        raise ValueError("At least 10 rows are required for the L0 Search")
    if client.has_collection(name, timeout=10):
        raise ValueError(f"Collection {name!r} already exists. Use it directly or choose a new name.")

    schema = client.create_schema(auto_id=False, enable_dynamic_field=False)
    schema.add_field(field_name="id", datatype=DataType.INT64, is_primary=True)
    schema.add_field(field_name="vector", datatype=DataType.FLOAT_VECTOR, dim=VECTOR_DIMENSION)
    indexes = client.prepare_index_params()
    indexes.add_index(field_name="vector", index_type="FLAT", metric_type="L2")
    client.create_collection(name, schema=schema, consistency_level="Strong", timeout=120)
    if on_created is not None:
        on_created()

    random = np.random.default_rng()
    for start in range(0, rows, BATCH_SIZE):
        count = min(BATCH_SIZE, rows - start)
        vectors = random.random((count, VECTOR_DIMENSION), dtype=np.float32).tolist()
        client.insert(
            name, [{"id": start + offset, "vector": vector} for offset, vector in enumerate(vectors)], timeout=60
        )
    client.flush(name, timeout=120)
    client.create_index(name, indexes, timeout=120)
    client.load_collection(name, timeout=120)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--reuse-only", action="store_true", help="Fail if the dataset is missing; never insert data")
    args = parser.parse_args()
    required = ("PROXY_A_URI", "MILVUS_TOKEN", "MILVUS_L0_COLLECTION")
    missing = [key for key in required if not os.environ.get(key)]
    if missing:
        raise SystemExit(f"Set the setup inputs: {', '.join(missing)}")

    from pymilvus import MilvusClient

    uri, token, name = (os.environ[key] for key in required)
    client = MilvusClient(uri=uri, token=token, timeout=10)
    try:
        rows = int(os.environ.get("MILVUS_L0_ROWS", DEFAULT_ROWS))
        if not client.has_collection(name, timeout=10):
            if args.reuse_only:
                raise SystemExit(f"Search Collection {name!r} is missing. Prepare it outside CI.")
            create_collection(client, name, rows)
        if client.get_load_state(name, timeout=10)["state"].name != "Loaded":
            client.load_collection(name, timeout=120)
        path = baseline_path(name)
        if path.is_file():
            load_baseline(client, name)
        elif args.reuse_only:
            raise SystemExit(f"Search baseline {path} is missing. Prepare it outside CI.")
        else:
            path = write_baseline(client, name)
    finally:
        client.close()
    print(f"Prepared {name!r}; Search baseline: {path}. Use MILVUS_L0_COLLECTION={name} in pytest.")


if __name__ == "__main__":
    main()

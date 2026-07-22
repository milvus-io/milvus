import hashlib
import io
import json
import os
import random
import threading
import uuid
from concurrent.futures import ThreadPoolExecutor, as_completed

import pytest
from base.client_v2_base import TestMilvusClientV2Base
from common import common_func as cf
from common import common_type as ct
from common.common_type import CaseLabel, CheckTasks
from pymilvus import AnnSearchRequest, DataType, Function, FunctionType, WeightedRanker
from pymilvus.client.types import LoadState
from utils.util_log import test_log as log

DIM = 16
ID_FIELD = "id"
VECTOR_FIELD = "vector"
CATEGORY_FIELD = "category"
SCORE_FIELD = "score"
JSON_FIELD = "json_meta"
CONTENT_FIELD = "content"
CONTENT_ZH_FIELD = "content_zh"
CONTENT_ALT_FIELD = "content_alt"
CONTENT_DEFAULT_FIELD = "content_default"
VARCHAR_TEXT_FIELD = "varchar_text"
CONTENT_SPARSE_FIELD = "content_sparse"
CONTENT_ZH_SPARSE_FIELD = "content_zh_sparse"
CONTENT_DEFAULT_SPARSE_FIELD = "content_default_sparse"

STANDARD_ANALYZER = {"tokenizer": "standard"}
JIEBA_ANALYZER = {
    "tokenizer": {
        "type": "jieba",
        "dict": ["向量数据库", "混合搜索", "稀疏向量"],
        "mode": "exact",
        "hmm": False,
    }
}
DEFAULT_TEXT = "milvus text lob sentinel vector database"
EXPLICIT_DEFAULT_TEXT = "explicit default text lob marker atlas"
TEXT_FIELDS = [CONTENT_FIELD, CONTENT_ZH_FIELD, CONTENT_ALT_FIELD, CONTENT_DEFAULT_FIELD]
TEXT_USER_DEFINED_INDEX_CASES = [
    pytest.param("AUTOINDEX", {}, id="AUTOINDEX"),
    pytest.param("INVERTED", {}, id="INVERTED"),
    pytest.param("BITMAP", {}, id="BITMAP"),
    pytest.param("TRIE", {}, id="TRIE"),
    pytest.param("STL_SORT", {}, id="STL_SORT"),
    pytest.param("NGRAM", {"params": {"min_gram": 2, "max_gram": 4}}, id="NGRAM"),
]
SHARED_COLLECTION_NAME = "test_text_lob" + cf.gen_unique_str("_")
ANALYZER_TOKEN_CACHE = {}


def make_text(size, seed):
    """Return deterministic ASCII text with exact byte length."""
    if size == 0:
        return ""
    base = f"seed {seed} vector database milvus text lob storage bm25 payload boundary checksum {seed} "
    text = (base * ((size // len(base)) + 1))[:size]
    return text


def sha256_text(value):
    if value is None:
        return None
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


def payload_meta(value):
    if value is None:
        return {
            "state": "null",
            "bytes": None,
            "chars": None,
            "prefix": None,
            "suffix": None,
            "sha256": None,
        }
    state = "empty" if value == "" else "value"
    encoded = value.encode("utf-8")
    return {
        "state": state,
        "bytes": len(encoded),
        "chars": len(value),
        "prefix": value[:32],
        "suffix": value[-32:] if value else "",
        "sha256": hashlib.sha256(encoded).hexdigest(),
    }


def assert_text_payload(actual, expected):
    actual_meta = payload_meta(actual)
    for key in ["state", "bytes", "chars", "prefix", "suffix", "sha256"]:
        assert actual_meta[key] == expected[key], (
            f"text payload mismatch on {key}: "
            f"actual={actual_meta[key]!r}, expected={expected[key]!r}, "
            f"actual_meta={actual_meta}, expected_meta={expected}"
        )


def vector_for_pk(pk):
    rng = random.Random(19530 + pk)
    return [rng.random() for _ in range(DIM)]


def build_row(
    pk,
    scenario,
    content,
    content_zh=None,
    content_alt=None,
    varchar_text=None,
    content_default=DEFAULT_TEXT,
):
    if content_zh is None and content is not None and len(content) <= 4096:
        content_zh = content
    if content_alt is None and content is not None:
        content_alt = f"alternate text lob payload for {scenario}: {content[:128]}"
    if varchar_text is None:
        varchar_text = content if content is not None and len(content) <= 60000 else f"{scenario} vector database"

    row = {
        ID_FIELD: pk,
        VECTOR_FIELD: vector_for_pk(pk),
        CATEGORY_FIELD: scenario,
        SCORE_FIELD: float(pk) / 10.0,
        JSON_FIELD: {
            "scenario": scenario,
            "content_sha256": sha256_text(content),
            "content_bytes": None if content is None else len(content.encode("utf-8")),
        },
        CONTENT_FIELD: content,
        CONTENT_ZH_FIELD: content_zh,
        CONTENT_ALT_FIELD: content_alt,
        VARCHAR_TEXT_FIELD: varchar_text,
        "dynamic_note": f"dynamic-{scenario}",
    }
    if content_default is not None:
        row[CONTENT_DEFAULT_FIELD] = content_default
    return row


def build_text_lob_rows(base_pk=0, include_boundaries=True, include_one_mib=True, include_ten_mib=False):
    rows = [
        build_row(base_pk + 0, "small", "vector database milvus text lob smoke"),
        build_row(base_pk + 1, "empty", "", content_zh="", content_alt="", varchar_text=""),
        build_row(base_pk + 2, "null", None, content_zh=None, content_alt=None, varchar_text=None),
        build_row(
            base_pk + 3,
            "unicode",
            "Milvus stores multilingual text: English 中文 日本語 Русский العربية emoji 😀🚀 데이터베이스",
            content_zh="向量数据库 支持 中文检索 和 混合搜索",
            varchar_text="Milvus stores multilingual text vector database emoji",
        ),
        build_row(base_pk + 8, "bm25_low", "vector database"),
        build_row(
            base_pk + 9,
            "bm25_mid",
            "vector database " * 4 + "milvus retrieval",
        ),
        build_row(
            base_pk + 10,
            "bm25_high",
            "vector database " * 12 + "milvus bm25 ranking ranking",
        ),
        build_row(
            base_pk + 11,
            "chinese",
            "english sidecar text for chinese bm25",
            content_zh="向量数据库 支持 中文检索。Milvus 提供 混合搜索 和 稀疏向量 检索。",
            varchar_text="chinese vector database sidecar",
        ),
        build_row(base_pk + 12, "sentinel", "explicit row that verifies sentinel text output"),
        build_row(
            base_pk + 13,
            "default_explicit",
            "row that explicitly sets default text field",
            content_default=EXPLICIT_DEFAULT_TEXT,
        ),
    ]
    if include_boundaries:
        rows.extend(
            [
                build_row(base_pk + 4, "below_64k", make_text(64 * 1024 - 17, f"{base_pk}-below")),
                build_row(base_pk + 5, "at_64k", make_text(64 * 1024, f"{base_pk}-at")),
                build_row(base_pk + 6, "above_64k", make_text(64 * 1024 + 4096, f"{base_pk}-above")),
            ]
        )
    if include_one_mib:
        rows.append(
            build_row(
                base_pk + 7,
                "one_mib",
                make_text(1024 * 1024, f"{base_pk}-one-mib"),
                content_zh=None,
                content_alt=make_text(128 * 1024, f"{base_pk}-alt-one-mib"),
                varchar_text="one mib vector database",
            )
        )
    if include_ten_mib:
        rows.append(
            build_row(
                base_pk + 14,
                "ten_mib",
                make_text(10 * 1024 * 1024, f"{base_pk}-ten-mib"),
                content_zh=None,
                content_alt=make_text(256 * 1024, f"{base_pk}-alt-ten-mib"),
                varchar_text="ten mib vector database",
            )
        )
    return sorted(rows, key=lambda row: row[ID_FIELD])


def expected_payloads(rows):
    expected = {}
    for row in rows:
        expected_default = row.get(CONTENT_DEFAULT_FIELD, DEFAULT_TEXT)
        expected[row[ID_FIELD]] = {
            CONTENT_FIELD: payload_meta(row.get(CONTENT_FIELD)),
            CONTENT_ZH_FIELD: payload_meta(row.get(CONTENT_ZH_FIELD)),
            CONTENT_ALT_FIELD: payload_meta(row.get(CONTENT_ALT_FIELD)),
            CONTENT_DEFAULT_FIELD: payload_meta(expected_default),
            VARCHAR_TEXT_FIELD: payload_meta(row.get(VARCHAR_TEXT_FIELD)),
        }
    return expected


def build_text_lob_schema(client, include_default=True, partition_key_field=None, content_default_value=None):
    schema_kwargs = {"auto_id": False, "enable_dynamic_field": True}
    if partition_key_field is not None:
        schema_kwargs["partition_key_field"] = partition_key_field
    schema = client.create_schema(**schema_kwargs)
    schema.add_field(field_name=ID_FIELD, datatype=DataType.INT64, is_primary=True)
    schema.add_field(field_name=VECTOR_FIELD, datatype=DataType.FLOAT_VECTOR, dim=DIM)
    schema.add_field(field_name=CATEGORY_FIELD, datatype=DataType.VARCHAR, max_length=64)
    schema.add_field(field_name=SCORE_FIELD, datatype=DataType.FLOAT)
    schema.add_field(field_name=JSON_FIELD, datatype=DataType.JSON)
    schema.add_field(
        field_name=CONTENT_FIELD,
        datatype=DataType.TEXT,
        nullable=True,
        enable_analyzer=True,
        enable_match=True,
        analyzer_params=STANDARD_ANALYZER,
    )
    schema.add_field(
        field_name=CONTENT_ZH_FIELD,
        datatype=DataType.TEXT,
        nullable=True,
        enable_analyzer=True,
        enable_match=True,
        analyzer_params=JIEBA_ANALYZER,
    )
    schema.add_field(
        field_name=CONTENT_ALT_FIELD,
        datatype=DataType.TEXT,
        nullable=True,
        enable_analyzer=True,
        enable_match=True,
        analyzer_params=STANDARD_ANALYZER,
    )
    if include_default:
        default_field_kwargs = {
            "field_name": CONTENT_DEFAULT_FIELD,
            "datatype": DataType.TEXT,
            "nullable": True,
            "enable_analyzer": True,
            "enable_match": True,
            "analyzer_params": STANDARD_ANALYZER,
        }
        if content_default_value is not None:
            default_field_kwargs["default_value"] = content_default_value
        schema.add_field(**default_field_kwargs)
    schema.add_field(
        field_name=VARCHAR_TEXT_FIELD,
        datatype=DataType.VARCHAR,
        max_length=65535,
        nullable=True,
        enable_analyzer=True,
        enable_match=True,
        analyzer_params=STANDARD_ANALYZER,
    )
    schema.add_field(field_name=CONTENT_SPARSE_FIELD, datatype=DataType.SPARSE_FLOAT_VECTOR)
    schema.add_field(field_name=CONTENT_ZH_SPARSE_FIELD, datatype=DataType.SPARSE_FLOAT_VECTOR)
    if include_default:
        schema.add_field(field_name=CONTENT_DEFAULT_SPARSE_FIELD, datatype=DataType.SPARSE_FLOAT_VECTOR)

    schema.add_function(
        Function(
            name="content_bm25",
            function_type=FunctionType.BM25,
            input_field_names=[CONTENT_FIELD],
            output_field_names=[CONTENT_SPARSE_FIELD],
            params={},
        )
    )
    schema.add_function(
        Function(
            name="content_zh_bm25",
            function_type=FunctionType.BM25,
            input_field_names=[CONTENT_ZH_FIELD],
            output_field_names=[CONTENT_ZH_SPARSE_FIELD],
            params={},
        )
    )
    if include_default:
        schema.add_function(
            Function(
                name="content_default_bm25",
                function_type=FunctionType.BM25,
                input_field_names=[CONTENT_DEFAULT_FIELD],
                output_field_names=[CONTENT_DEFAULT_SPARSE_FIELD],
                params={},
            )
        )
    return schema


def build_text_lob_index_params(
    client,
    include_default=True,
    sparse_index_type="SPARSE_INVERTED_INDEX",
    include_varchar_text_index=True,
):
    index_params = client.prepare_index_params()
    index_params.add_index(field_name=VECTOR_FIELD, index_type="FLAT", metric_type="COSINE")
    index_params.add_index(field_name=CONTENT_SPARSE_FIELD, index_type=sparse_index_type, metric_type="BM25")
    index_params.add_index(field_name=CONTENT_ZH_SPARSE_FIELD, index_type=sparse_index_type, metric_type="BM25")
    if include_default:
        index_params.add_index(
            field_name=CONTENT_DEFAULT_SPARSE_FIELD,
            index_type=sparse_index_type,
            metric_type="BM25",
        )
    if include_varchar_text_index:
        index_params.add_index(field_name=VARCHAR_TEXT_FIELD, index_type="AUTOINDEX")
    return index_params


def query_by_ids(testcase, client, collection_name, ids, output_fields, partition_names=None):
    fields = list(dict.fromkeys([ID_FIELD] + output_fields))
    rows, _ = testcase.query(
        client,
        collection_name,
        filter=f"{ID_FIELD} in {list(ids)}",
        output_fields=fields,
        partition_names=partition_names,
        consistency_level="Strong",
    )
    return {row[ID_FIELD]: row for row in rows}


def assert_count(testcase, client, collection_name, expected, filter_expr=f"{ID_FIELD} >= 0", partition_names=None):
    rows, _ = testcase.query(
        client,
        collection_name,
        filter=filter_expr,
        output_fields=["count(*)"],
        partition_names=partition_names,
        consistency_level="Strong",
    )
    assert int(rows[0]["count(*)"]) == expected


def assert_rows_payload(rows_by_id, expected, fields):
    assert set(rows_by_id) == set(expected), f"row ids mismatch: actual={set(rows_by_id)}, expected={set(expected)}"
    for pk, row in rows_by_id.items():
        for field in fields:
            assert_text_payload(row.get(field), expected[pk][field])


def collect_query_iterator(iterator):
    rows = []
    try:
        while True:
            batch = iterator.next()
            if not batch:
                break
            rows.extend(batch)
    finally:
        iterator.close()
    return rows


def result_entity(hit):
    entity = hit.get("entity")
    if entity is None:
        return hit
    return entity


def result_id(hit):
    if ID_FIELD in hit:
        return hit[ID_FIELD]
    return result_entity(hit)[ID_FIELD]


def named_item_name(item):
    if isinstance(item, dict):
        return item.get("name")
    return getattr(item, "name", None)


def named_item_value(item, key, default=None):
    if isinstance(item, dict):
        return item.get(key, default)
    return getattr(item, key, default)


def is_text_datatype(value):
    return value == DataType.TEXT or value == DataType.TEXT.value or str(value).upper().endswith("TEXT")


def token_text(token):
    if isinstance(token, dict):
        return token.get("token", "")
    return getattr(token, "token", str(token))


def analyzer_tokens(testcase, client, text, analyzer_params):
    cache_key = (
        json.dumps(analyzer_params, sort_keys=True),
        hashlib.sha256((text or "").encode("utf-8")).hexdigest(),
    )
    if cache_key in ANALYZER_TOKEN_CACHE:
        return ANALYZER_TOKEN_CACHE[cache_key]
    # Only the token string is consumed below, so skip detail/hash payloads the
    # server would otherwise compute and serialize (notably for large LOB texts).
    res, _ = testcase.run_analyzer(client, text, analyzer_params, with_detail=False, with_hash=False)
    tokens = getattr(res, "tokens", res)
    token_values = [token_text(token) for token in tokens if token_text(token)]
    ANALYZER_TOKEN_CACHE[cache_key] = token_values
    return token_values


def contains_minimum_tokens(value, tokens, minimum):
    if value is None:
        return minimum == 0
    matched = sum(1 for token in tokens if token and token in value)
    return matched >= minimum


def expected_text_match_ids(rows, field, tokens, minimum=1):
    expected = set()
    for row in rows:
        value = row.get(field)
        if contains_minimum_tokens(value, tokens, minimum):
            expected.add(row[ID_FIELD])
    return expected


def expected_text_match_ids_by_analyzer(testcase, client, rows, field, query, analyzer_params, minimum=1):
    query_tokens = set(analyzer_tokens(testcase, client, query, analyzer_params))
    expected = set()
    for row in rows:
        value = row.get(field)
        if value is None:
            continue
        row_tokens = set(analyzer_tokens(testcase, client, value, analyzer_params))
        if len(query_tokens & row_tokens) >= minimum:
            expected.add(row[ID_FIELD])
    return expected


def assert_analyzer_minimum_match(testcase, client, value, query, analyzer_params, minimum=1):
    query_tokens = set(analyzer_tokens(testcase, client, query, analyzer_params))
    value_tokens = set(analyzer_tokens(testcase, client, value or "", analyzer_params))
    assert len(query_tokens & value_tokens) >= minimum


def hit_distance(hit):
    if "distance" in hit:
        return hit["distance"]
    if "score" in hit:
        return hit["score"]
    return None


def assert_search_results(
    res,
    nq,
    limit,
    metric=None,
    output_fields=None,
    expected_ids=None,
    required_ids=None,
    exact_limit=False,
):
    assert len(res) == nq, f"unexpected nq: actual={len(res)}, expected={nq}"
    for hits in res:
        assert len(hits) > 0
        if exact_limit:
            assert len(hits) == limit
        else:
            assert len(hits) <= limit
        ids = [result_id(hit) for hit in hits]
        assert len(ids) == len(set(ids)), f"duplicate search result ids: {ids}"
        if expected_ids is not None:
            assert set(ids) <= set(expected_ids), f"unexpected ids: actual={set(ids)}, expected subset={expected_ids}"
        if required_ids is not None:
            assert set(required_ids) <= set(ids), f"required ids missing: required={required_ids}, actual={set(ids)}"
        for hit in hits:
            entity = result_entity(hit)
            for field in output_fields or []:
                if field != ID_FIELD:
                    assert field in entity, f"missing output field {field} in hit {hit}"
        if metric is not None and len(hits) > 1:
            distances = [hit_distance(hit) for hit in hits]
            assert all(distance is not None for distance in distances), f"missing distances in hits: {hits}"
            if metric.upper() == "L2":
                assert all(distances[i] <= distances[i + 1] for i in range(len(distances) - 1)), distances
            else:
                assert all(distances[i] >= distances[i + 1] for i in range(len(distances) - 1)), distances


def segment_identifier(segment):
    if isinstance(segment, dict):
        for key in ["id", "segment_id", "segmentID"]:
            if key in segment:
                return str(segment[key])
    for attr in ["id", "segment_id", "segmentID"]:
        if hasattr(segment, attr):
            return str(getattr(segment, attr))
    return repr(segment)


def item_value(item, keys, default=None):
    for key in keys:
        if isinstance(item, dict) and key in item:
            return item[key]
        if hasattr(item, key):
            return getattr(item, key)
    return default


def int_env(name, default):
    raw = os.getenv(name)
    if raw is None:
        return default
    try:
        return int(raw)
    except ValueError:
        pytest.fail(f"{name} must be an integer, got {raw!r}")


def object_key(*parts):
    return "/".join(str(part).strip("/") for part in parts if part not in [None, ""])


def minio_endpoint(minio_host):
    endpoint = (minio_host or "localhost").strip()
    if "://" in endpoint:
        endpoint = endpoint.split("://", 1)[1]
    endpoint = endpoint.split("/", 1)[0]
    if ":" not in endpoint:
        endpoint = f"{endpoint}:9000"
    return endpoint


def new_minio_client(minio_host):
    minio_mod = pytest.importorskip("minio", reason="TEXT LOB layout inspection requires minio client")
    return minio_mod.Minio(
        minio_endpoint(minio_host),
        access_key=os.getenv("MILVUS_MINIO_ACCESS_KEY", "minioadmin"),
        secret_key=os.getenv("MILVUS_MINIO_SECRET_KEY", "minioadmin"),
        secure=os.getenv("MILVUS_MINIO_SECURE", "false").lower() in ["1", "true", "yes"],
    )


def ensure_minio_bucket(minio_client, bucket):
    try:
        if not minio_client.bucket_exists(bucket):
            pytest.skip(f"TEXT LOB layout inspection requires existing MinIO bucket {bucket!r}")
    except Exception as exc:
        pytest.skip(f"TEXT LOB layout inspection requires MinIO bucket access: {exc}")


def list_minio_keys(minio_client, bucket, prefix):
    prefix = object_key(prefix)
    if prefix and not prefix.endswith("/"):
        prefix = f"{prefix}/"
    return sorted(obj.object_name for obj in minio_client.list_objects(bucket, prefix=prefix, recursive=True))


def field_id_from_desc(desc, field_name):
    for field in desc.get("fields", []):
        if named_item_name(field) == field_name:
            field_id = item_value(field, ["field_id", "fieldID", "id"])
            assert field_id is not None, f"field {field_name!r} has no field_id in describe_collection: {field}"
            return str(field_id)
    pytest.fail(f"field {field_name!r} not found in describe_collection: {desc}")


def collection_id_from_desc(desc):
    collection_id = item_value(desc, ["collection_id", "collectionID", "id"])
    assert collection_id is not None, f"collection_id missing from describe_collection: {desc}"
    return str(collection_id)


def select_segment_by_rows(segments, expected_rows):
    assert segments, "expected at least one persistent segment after flush"
    exact_matches = []
    for segment in segments:
        row_count = item_value(segment, ["num_rows", "numRows", "num_of_rows", "numOfRows"])
        if row_count is not None and int(row_count) == expected_rows:
            exact_matches.append(segment)
    if len(exact_matches) == 1:
        return exact_matches[0]
    if len(segments) == 1:
        return segments[0]
    pytest.fail(f"could not identify TEXT LOB segment: segments={segments}, expected_rows={expected_rows}")


def find_segment_base_path(minio_client, bucket, root_path, collection_id, segment_id):
    prefix = object_key(root_path, "insert_log", collection_id)
    keys = list_minio_keys(minio_client, bucket, prefix)
    for key in keys:
        parts = key.split("/")
        for index, part in enumerate(parts):
            if part == str(segment_id):
                return "/".join(parts[: index + 1])
    return None


def read_parquet_object(minio_client, bucket, key):
    pq = pytest.importorskip("pyarrow.parquet", reason="TEXT LOB layout inspection requires pyarrow")
    response = minio_client.get_object(bucket, key)
    try:
        return pq.read_table(io.BytesIO(response.read()))
    finally:
        response.close()
        response.release_conn()


def bytes_value(value):
    if value is None:
        return None
    if hasattr(value, "as_py"):
        value = value.as_py()
    if isinstance(value, bytes):
        return value
    if isinstance(value, bytearray):
        return bytes(value)
    if isinstance(value, memoryview):
        return value.tobytes()
    return None


def int_values(values):
    converted = []
    for value in values:
        if hasattr(value, "as_py"):
            value = value.as_py()
        if not isinstance(value, int) or isinstance(value, bool):
            return None
        converted.append(int(value))
    return converted


def column_names_for(field_id, field_name):
    return [str(field_id), field_name, f"field_{field_id}", f"{field_name}_{field_id}"]


def candidate_columns(table, preferred_names, predicate):
    candidates = []
    names = list(table.schema.names)
    for name in preferred_names:
        if name in names:
            values = table[name].to_pylist()
            converted = predicate(values)
            if converted is not None:
                candidates.append((name, converted))
    for name in names:
        if name in preferred_names:
            continue
        values = table[name].to_pylist()
        converted = predicate(values)
        if converted is not None:
            candidates.append((name, converted))
    return candidates


def text_reference_values(values):
    converted = [bytes_value(value) for value in values]
    if any(value is not None and len(value) > 0 and value[0] in [0, 1] for value in converted):
        return converted
    return None


def read_text_reference_rows(minio_client, bucket, segment_base, content_field_id, id_field_id, expected_ids):
    data_prefix = object_key(segment_base, "_data")
    parquet_keys = [key for key in list_minio_keys(minio_client, bucket, data_prefix) if key.endswith(".parquet")]
    if not parquet_keys:
        parquet_keys = [key for key in list_minio_keys(minio_client, bucket, segment_base) if key.endswith(".parquet")]
    assert parquet_keys, f"no parquet reference files found under segment base {segment_base}"

    id_candidates = []
    ref_candidates = []
    parquet_debug = []
    for key in parquet_keys:
        table = read_parquet_object(minio_client, bucket, key)
        parquet_debug.append({"key": key, "columns": list(table.schema.names), "rows": table.num_rows})
        id_candidates.extend(
            (key, name, values)
            for name, values in candidate_columns(table, column_names_for(id_field_id, ID_FIELD), int_values)
        )
        ref_candidates.extend(
            (key, name, values)
            for name, values in candidate_columns(
                table, column_names_for(content_field_id, CONTENT_FIELD), text_reference_values
            )
        )

    expected_id_set = set(expected_ids)
    assert id_candidates, f"no primary-key column found in parquet files: {parquet_debug}"
    assert ref_candidates, f"no TEXT reference column found in parquet files: {parquet_debug}"

    id_key, id_name, ids = max(id_candidates, key=lambda candidate: len(expected_id_set & set(candidate[2])))
    assert expected_id_set <= set(ids), (
        f"primary-key column {id_name} from {id_key} does not contain expected ids; "
        f"expected={expected_id_set}, actual={ids}, parquet={parquet_debug}"
    )

    matching_refs = [candidate for candidate in ref_candidates if len(candidate[2]) == len(ids)]
    assert matching_refs, (
        f"no TEXT reference column has the same row count as primary-key column {id_name} from {id_key}; "
        f"ref_candidates={[(key, name, len(values)) for key, name, values in ref_candidates]}, parquet={parquet_debug}"
    )
    ref_key, ref_name, refs = matching_refs[0]
    log.info(f"TEXT LOB reference layout: pk={id_key}:{id_name}, text={ref_key}:{ref_name}")
    return {pk: ref for pk, ref in zip(ids, refs) if pk in expected_id_set}


def classify_text_ref(ref_bytes, expected_text):
    assert ref_bytes is not None and len(ref_bytes) > 0, "TEXT reference must be non-empty bytes"
    expected_bytes = expected_text.encode("utf-8")
    if ref_bytes[0] == 0:
        assert ref_bytes[1:] == expected_bytes, (
            f"inline TEXT payload mismatch: actual_bytes={len(ref_bytes) - 1}, expected_bytes={len(expected_bytes)}"
        )
        return "inline"
    if ref_bytes[0] == 1:
        assert len(ref_bytes) == 24, f"LOB reference must be 24 bytes, got {len(ref_bytes)}"
        return "lob"
    pytest.fail(f"unknown TEXT reference flag {ref_bytes[0]!r}")


def lob_object_key(partition_base, content_field_id, ref_bytes):
    file_id = str(uuid.UUID(bytes=ref_bytes[4:20]))
    return object_key(partition_base, "lobs", content_field_id, "_data", f"{file_id}.vx")


def persistent_segment_ids(testcase, client, collection_name):
    segments, _ = testcase.list_persistent_segments(client, collection_name)
    return tuple(sorted(segment_identifier(segment) for segment in segments))


def insert_flush_batches(testcase, client, collection_name, rows, batch_size):
    for start in range(0, len(rows), batch_size):
        testcase.insert(client, collection_name, rows[start : start + batch_size])
        testcase.flush(client, collection_name)


def assert_compaction_rewrote_segments(before_segments, after_segments):
    assert len(before_segments) >= 2, f"compaction test needs multiple sealed segments, got {before_segments}"
    assert len(after_segments) > 0
    assert set(after_segments) != set(before_segments), (
        f"compaction did not rewrite persistent segments: before={before_segments}, after={after_segments}"
    )
    assert len(after_segments) <= len(before_segments), (
        f"compaction should not increase persistent segment count: before={before_segments}, after={after_segments}"
    )


@pytest.mark.xdist_group("TestMilvusClientTextLOB")
class TestMilvusClientTextLOBShared(TestMilvusClientV2Base):
    """Read-only TEXT LOB coverage backed by one shared collection."""

    shared_rows = []
    shared_expected = {}
    shared_ids = []

    @pytest.fixture(scope="module", autouse=True)
    def prepare_text_lob_collection(self, request):
        client = self._client()
        collection_name = SHARED_COLLECTION_NAME

        def teardown():
            try:
                if client.has_collection(collection_name):
                    client.drop_collection(collection_name)
            except Exception as exc:
                log.warning(f"failed to drop shared TEXT LOB collection {collection_name}: {exc}")

        request.addfinalizer(teardown)

        if client.has_collection(collection_name):
            client.drop_collection(collection_name)

        self.__class__.shared_rows = build_text_lob_rows()
        self.__class__.shared_expected = expected_payloads(self.shared_rows)
        self.__class__.shared_ids = [row[ID_FIELD] for row in self.shared_rows]

        schema = build_text_lob_schema(client)
        index_params = build_text_lob_index_params(client)
        client.create_collection(
            collection_name=collection_name,
            schema=schema,
            index_params=index_params,
            consistency_level="Strong",
        )
        client.insert(collection_name=collection_name, data=self.shared_rows)
        client.flush(collection_name=collection_name)
        client.load_collection(collection_name=collection_name)

    @pytest.mark.tags(CaseLabel.L0)
    def test_text_lob_shared_schema_and_payloads(self):
        """
        target: verify TEXT LOB schema, BM25 functions, sentinel TEXT, and exact payload retrieval
        method: describe shared collection and query every deterministic row by primary key
        expected: TEXT fields/functions exist and returned TEXT payload checksums match inserted data
        """
        client = self._client()
        desc, _ = self.describe_collection(client, SHARED_COLLECTION_NAME)
        fields = {field["name"]: field for field in desc["fields"]}
        for field in [CONTENT_FIELD, CONTENT_ZH_FIELD, CONTENT_ALT_FIELD, CONTENT_DEFAULT_FIELD]:
            assert is_text_datatype(fields[field]["type"])
        functions = {named_item_name(function): function for function in desc.get("functions", [])}
        for function_name, input_field, output_field in [
            ("content_bm25", CONTENT_FIELD, CONTENT_SPARSE_FIELD),
            ("content_zh_bm25", CONTENT_ZH_FIELD, CONTENT_ZH_SPARSE_FIELD),
            ("content_default_bm25", CONTENT_DEFAULT_FIELD, CONTENT_DEFAULT_SPARSE_FIELD),
        ]:
            function = functions[function_name]
            function_type = named_item_value(function, "function_type", named_item_value(function, "type"))
            assert function_type in [FunctionType.BM25, FunctionType.BM25.value, "BM25"]
            assert named_item_value(function, "input_field_names") == [input_field]
            assert named_item_value(function, "output_field_names") == [output_field]

        for field in [CONTENT_SPARSE_FIELD, CONTENT_ZH_SPARSE_FIELD, CONTENT_DEFAULT_SPARSE_FIELD]:
            index_info, _ = self.describe_index(client, SHARED_COLLECTION_NAME, index_name=field)
            assert index_info["index_type"] == "SPARSE_INVERTED_INDEX"
            assert index_info["metric_type"] == "BM25"

        assert_count(self, client, SHARED_COLLECTION_NAME, len(self.shared_rows))
        rows_by_id = query_by_ids(
            self,
            client,
            SHARED_COLLECTION_NAME,
            self.shared_ids,
            TEXT_FIELDS + [VARCHAR_TEXT_FIELD],
        )
        assert_rows_payload(rows_by_id, self.shared_expected, TEXT_FIELDS + [VARCHAR_TEXT_FIELD])

    @pytest.mark.tags(CaseLabel.L0)
    def test_text_lob_dense_search_output_fields(self):
        """
        target: verify vector search can return TEXT LOB output fields
        method: search with an exact fixture vector and validate returned TEXT checksums
        expected: every returned content/content_alt payload matches expected metadata
        """
        client = self._client()
        res, _ = self.search(
            client,
            SHARED_COLLECTION_NAME,
            data=[vector_for_pk(0)],
            anns_field=VECTOR_FIELD,
            search_params={"metric_type": "COSINE", "params": {}},
            limit=3,
            output_fields=[ID_FIELD, CONTENT_FIELD, CONTENT_ALT_FIELD],
            consistency_level="Strong",
        )
        assert_search_results(
            res,
            nq=1,
            limit=3,
            metric="COSINE",
            output_fields=[CONTENT_FIELD, CONTENT_ALT_FIELD],
            required_ids={0},
        )
        for hit in res[0]:
            pk = result_id(hit)
            entity = result_entity(hit)
            assert_text_payload(entity.get(CONTENT_FIELD), self.shared_expected[pk][CONTENT_FIELD])
            assert_text_payload(entity.get(CONTENT_ALT_FIELD), self.shared_expected[pk][CONTENT_ALT_FIELD])

    @pytest.mark.tags(CaseLabel.L0)
    def test_text_lob_query_iterator_payloads(self):
        """
        target: verify query_iterator returns all TEXT LOB rows exactly once with intact payloads
        method: iterate shared collection in small batches and validate checksums
        expected: no missing/duplicate primary keys and all TEXT payload metadata matches
        """
        client = self._client()
        iterator, _ = self.query_iterator(
            client,
            SHARED_COLLECTION_NAME,
            batch_size=3,
            filter=f"{ID_FIELD} >= 0",
            output_fields=[ID_FIELD, CONTENT_FIELD],
            consistency_level="Strong",
        )
        rows = collect_query_iterator(iterator)
        rows_by_id = {row[ID_FIELD]: row for row in rows}
        assert_rows_payload(rows_by_id, self.shared_expected, [CONTENT_FIELD])

    @pytest.mark.tags(CaseLabel.L1)
    def test_text_lob_text_match_text_and_varchar(self):
        """
        target: verify text_match works on DataType.TEXT and comparable analyzer-enabled VARCHAR
        method: query both fields with the same analyzer-derived token set
        expected: every returned row contains at least one analyzed query token in the matched field
        """
        client = self._client()
        text_match_ids = None
        for field in [CONTENT_FIELD, VARCHAR_TEXT_FIELD]:
            expected_ids = expected_text_match_ids_by_analyzer(
                self, client, self.shared_rows, field, "vector database", STANDARD_ANALYZER
            )
            rows, _ = self.query(
                client,
                SHARED_COLLECTION_NAME,
                filter=f'text_match({field}, "vector database")',
                output_fields=[ID_FIELD, field],
                consistency_level="Strong",
            )
            assert {row[ID_FIELD] for row in rows} == expected_ids
            for row in rows:
                assert_analyzer_minimum_match(self, client, row.get(field), "vector database", STANDARD_ANALYZER)
            if field == CONTENT_FIELD:
                text_match_ids = {row[ID_FIELD] for row in rows}

        upper_rows, _ = self.query(
            client,
            SHARED_COLLECTION_NAME,
            filter=f'TEXT_MATCH({CONTENT_FIELD}, "vector database")',
            output_fields=[ID_FIELD, CONTENT_FIELD],
            consistency_level="Strong",
        )
        assert {row[ID_FIELD] for row in upper_rows} == text_match_ids

    @pytest.mark.tags(CaseLabel.L1)
    def test_text_lob_text_match_minimum_should_match(self):
        """
        target: verify minimum_should_match on TEXT fields
        method: query vector/database/milvus with thresholds 1, 2, 3, and an impossible threshold
        expected: returned rows satisfy the requested analyzed-token cardinality
        """
        client = self._client()
        query = "vector database milvus"
        tokens = analyzer_tokens(self, client, query, STANDARD_ANALYZER)
        for minimum in [1, 2, 3]:
            expected_ids = expected_text_match_ids_by_analyzer(
                self, client, self.shared_rows, CONTENT_FIELD, query, STANDARD_ANALYZER, minimum
            )
            rows, _ = self.query(
                client,
                SHARED_COLLECTION_NAME,
                filter=f'text_match({CONTENT_FIELD}, "{query}", minimum_should_match={minimum})',
                output_fields=[ID_FIELD, CONTENT_FIELD],
                consistency_level="Strong",
            )
            assert {row[ID_FIELD] for row in rows} == expected_ids
            for row in rows:
                assert_analyzer_minimum_match(self, client, row.get(CONTENT_FIELD), query, STANDARD_ANALYZER, minimum)

        rows, _ = self.query(
            client,
            SHARED_COLLECTION_NAME,
            filter=f'text_match({CONTENT_FIELD}, "{query}", minimum_should_match={len(tokens) + 1})',
            output_fields=[ID_FIELD],
            consistency_level="Strong",
        )
        assert rows == []

    @pytest.mark.tags(CaseLabel.L1)
    def test_text_lob_text_match_filter_template_and_dense_filter(self):
        """
        target: verify text_match filter templates and ANN filtering over TEXT rows
        method: compare literal/template query filters and run dense ANN with a TEXT filter
        expected: template result IDs match literal IDs and every ANN hit satisfies the analyzed token filter
        """
        client = self._client()
        expected_ids = expected_text_match_ids_by_analyzer(
            self, client, self.shared_rows, CONTENT_FIELD, "vector database", STANDARD_ANALYZER
        )
        literal_rows, _ = self.query(
            client,
            SHARED_COLLECTION_NAME,
            filter=f'text_match({CONTENT_FIELD}, "vector database")',
            output_fields=[ID_FIELD],
            consistency_level="Strong",
        )
        assert {row[ID_FIELD] for row in literal_rows} == expected_ids
        template_rows, _ = self.query(
            client,
            SHARED_COLLECTION_NAME,
            filter=f"text_match({CONTENT_FIELD}, {{query}})",
            filter_params={"query": "vector database"},
            output_fields=[ID_FIELD],
            consistency_level="Strong",
        )
        assert {row[ID_FIELD] for row in literal_rows} == {row[ID_FIELD] for row in template_rows}

        res, _ = self.search(
            client,
            SHARED_COLLECTION_NAME,
            data=[vector_for_pk(0)],
            anns_field=VECTOR_FIELD,
            search_params={"metric_type": "COSINE", "params": {}},
            filter=f'text_match({CONTENT_FIELD}, "vector database")',
            limit=10,
            output_fields=[ID_FIELD, CONTENT_FIELD],
            consistency_level="Strong",
        )
        assert_search_results(
            res,
            nq=1,
            limit=10,
            metric="COSINE",
            output_fields=[CONTENT_FIELD],
            expected_ids=expected_ids,
        )
        for hit in res[0]:
            entity = result_entity(hit)
            assert_analyzer_minimum_match(self, client, entity.get(CONTENT_FIELD), "vector database", STANDARD_ANALYZER)

    @pytest.mark.tags(CaseLabel.L1)
    def test_text_lob_bm25_search_and_filter(self):
        """
        target: verify BM25 search over TEXT and BM25 combined with text_match filter
        method: search content_sparse using text queries and validate weak ranking/checksum invariants
        expected: high-frequency fixture rows appear near the top and returned payloads are exact
        """
        client = self._client()
        res, _ = self.search(
            client,
            SHARED_COLLECTION_NAME,
            data=["ranking"],
            anns_field=CONTENT_SPARSE_FIELD,
            search_params={"metric_type": "BM25", "params": {}},
            limit=3,
            output_fields=[ID_FIELD, CONTENT_FIELD],
            consistency_level="Strong",
        )
        assert_search_results(
            res,
            nq=1,
            limit=3,
            metric="BM25",
            output_fields=[CONTENT_FIELD],
            required_ids={10},
        )
        hit_ids = [result_id(hit) for hit in res[0]]
        assert hit_ids[0] == 10
        for hit in res[0]:
            pk = result_id(hit)
            assert_text_payload(result_entity(hit).get(CONTENT_FIELD), self.shared_expected[pk][CONTENT_FIELD])

        ranking_ids = expected_text_match_ids_by_analyzer(
            self, client, self.shared_rows, CONTENT_FIELD, "ranking", STANDARD_ANALYZER
        )
        filtered, _ = self.search(
            client,
            SHARED_COLLECTION_NAME,
            data=["bm25 ranking"],
            anns_field=CONTENT_SPARSE_FIELD,
            search_params={"metric_type": "BM25", "params": {}},
            filter=f'text_match({CONTENT_FIELD}, "ranking")',
            limit=3,
            output_fields=[ID_FIELD, CONTENT_FIELD],
            consistency_level="Strong",
        )
        assert_search_results(
            filtered,
            nq=1,
            limit=3,
            metric="BM25",
            output_fields=[CONTENT_FIELD],
            expected_ids=ranking_ids,
        )
        assert all("ranking" in result_entity(hit)[CONTENT_FIELD].lower() for hit in filtered[0])

        default_res, _ = self.search(
            client,
            SHARED_COLLECTION_NAME,
            data=["explicit default marker atlas"],
            anns_field=CONTENT_DEFAULT_SPARSE_FIELD,
            search_params={"metric_type": "BM25", "params": {}},
            limit=len(self.shared_rows),
            output_fields=[ID_FIELD, CONTENT_DEFAULT_FIELD],
            consistency_level="Strong",
        )
        assert_search_results(
            default_res,
            nq=1,
            limit=len(self.shared_rows),
            metric="BM25",
            output_fields=[CONTENT_DEFAULT_FIELD],
            required_ids={13},
        )
        assert result_id(default_res[0][0]) == 13
        for hit in default_res[0]:
            pk = result_id(hit)
            assert_text_payload(
                result_entity(hit).get(CONTENT_DEFAULT_FIELD),
                self.shared_expected[pk][CONTENT_DEFAULT_FIELD],
            )

    @pytest.mark.tags(CaseLabel.L1)
    def test_text_lob_chinese_bm25(self):
        """
        target: verify jieba-analyzed TEXT works with BM25 and text_match
        method: run Chinese BM25 and text_match on content_zh
        expected: Chinese fixture row is returned and payload checksum is exact
        """
        client = self._client()
        res, _ = self.search(
            client,
            SHARED_COLLECTION_NAME,
            data=["向量数据库 中文检索"],
            anns_field=CONTENT_ZH_SPARSE_FIELD,
            search_params={"metric_type": "BM25", "params": {}},
            limit=3,
            output_fields=[ID_FIELD, CONTENT_ZH_FIELD],
            consistency_level="Strong",
        )
        assert_search_results(res, nq=1, limit=3, metric="BM25", output_fields=[CONTENT_ZH_FIELD])
        hit_ids = {result_id(hit) for hit in res[0]}
        assert 11 in hit_ids or 3 in hit_ids
        for hit in res[0]:
            pk = result_id(hit)
            assert_text_payload(result_entity(hit).get(CONTENT_ZH_FIELD), self.shared_expected[pk][CONTENT_ZH_FIELD])

        expected_ids = expected_text_match_ids_by_analyzer(
            self, client, self.shared_rows, CONTENT_ZH_FIELD, "向量数据库", JIEBA_ANALYZER
        )
        rows, _ = self.query(
            client,
            SHARED_COLLECTION_NAME,
            filter=f'text_match({CONTENT_ZH_FIELD}, "向量数据库")',
            output_fields=[ID_FIELD, CONTENT_ZH_FIELD],
            consistency_level="Strong",
        )
        assert {row[ID_FIELD] for row in rows} == expected_ids

    @pytest.mark.tags(CaseLabel.L1)
    def test_text_lob_hybrid_dense_bm25(self):
        """
        target: verify hybrid dense+BM25 search can return TEXT LOB fields
        method: combine exact dense vector request and BM25 text request
        expected: hybrid search succeeds and returned TEXT payloads match checksum metadata
        """
        client = self._client()
        filter_expr = f'text_match({CONTENT_FIELD}, "ranking")'
        dense_req = AnnSearchRequest(
            data=[vector_for_pk(0)],
            anns_field=VECTOR_FIELD,
            param={"metric_type": "COSINE", "params": {}},
            limit=3,
            expr=filter_expr,
        )
        bm25_req = AnnSearchRequest(
            data=["vector database milvus"],
            anns_field=CONTENT_SPARSE_FIELD,
            param={"metric_type": "BM25", "params": {}},
            limit=3,
            expr=filter_expr,
        )
        res, _ = self.hybrid_search(
            client,
            SHARED_COLLECTION_NAME,
            reqs=[dense_req, bm25_req],
            ranker=WeightedRanker(0.4, 0.6),
            limit=3,
            output_fields=[ID_FIELD, CONTENT_FIELD, CONTENT_ALT_FIELD],
            consistency_level="Strong",
        )
        assert_search_results(res, nq=1, limit=3, output_fields=[CONTENT_FIELD, CONTENT_ALT_FIELD])
        for hit in res[0]:
            pk = result_id(hit)
            entity = result_entity(hit)
            assert "ranking" in entity.get(CONTENT_FIELD).lower()
            assert_text_payload(entity.get(CONTENT_FIELD), self.shared_expected[pk][CONTENT_FIELD])
            assert_text_payload(entity.get(CONTENT_ALT_FIELD), self.shared_expected[pk][CONTENT_ALT_FIELD])


class TestMilvusClientTextLOBIndependent(TestMilvusClientV2Base):
    """TEXT LOB cases requiring isolated collections."""

    def create_text_lob_collection(
        self,
        client,
        collection_name,
        rows=None,
        load=True,
        sparse_index_type="SPARSE_INVERTED_INDEX",
        include_varchar_text_index=True,
        partition_key_field=None,
        num_partitions=None,
    ):
        schema = build_text_lob_schema(client, partition_key_field=partition_key_field)
        index_params = build_text_lob_index_params(
            client,
            sparse_index_type=sparse_index_type,
            include_varchar_text_index=include_varchar_text_index,
        )
        create_kwargs = {
            "schema": schema,
            "consistency_level": "Strong",
            "load": False,
        }
        if num_partitions is not None:
            create_kwargs["num_partitions"] = num_partitions
        self.create_collection(
            client,
            collection_name,
            **create_kwargs,
        )
        self.create_index(client, collection_name, index_params=index_params)
        if rows:
            self.insert(client, collection_name, rows)
            self.flush(client, collection_name)
        if load:
            self.load_collection(client, collection_name)

    @pytest.mark.tags(CaseLabel.L1)
    def test_text_lob_mmap_collection_and_index(self):
        """
        target: verify TEXT LOB query/search remains correct when collection and indexes enable mmap
        method: enable collection and index mmap before loading, insert boundary/large TEXT rows, then query/text_match/BM25
        expected: mmap properties are visible and TEXT payload checksums/search results remain correct
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        rows = build_text_lob_rows(base_pk=2600, include_boundaries=True, include_one_mib=True)
        expected = expected_payloads(rows)
        self.create_text_lob_collection(client, collection_name, rows=None, load=False)

        self.alter_collection_properties(client, collection_name, properties={"mmap.enabled": True})
        desc, _ = self.describe_collection(client, collection_name)
        assert desc.get("properties", {}).get("mmap.enabled") == "True"

        for index_name in [
            VECTOR_FIELD,
            CONTENT_SPARSE_FIELD,
            CONTENT_ZH_SPARSE_FIELD,
            CONTENT_DEFAULT_SPARSE_FIELD,
            VARCHAR_TEXT_FIELD,
        ]:
            self.alter_index_properties(
                client,
                collection_name,
                index_name=index_name,
                properties={"mmap.enabled": True},
            )
            index_info, _ = self.describe_index(client, collection_name, index_name=index_name)
            assert index_info.get("mmap.enabled") == "True"

        self.insert(client, collection_name, rows)
        self.flush(client, collection_name)
        self.load_collection(client, collection_name)

        row_ids = [row[ID_FIELD] for row in rows]
        rows_by_id = query_by_ids(self, client, collection_name, row_ids, [CONTENT_FIELD, CONTENT_ALT_FIELD])
        assert_rows_payload(rows_by_id, expected, [CONTENT_FIELD, CONTENT_ALT_FIELD])

        expected_text_match_ids = expected_text_match_ids_by_analyzer(
            self,
            client,
            rows,
            CONTENT_FIELD,
            "vector database",
            STANDARD_ANALYZER,
        )
        text_match_rows, _ = self.query(
            client,
            collection_name,
            filter=f'text_match({CONTENT_FIELD}, "vector database")',
            output_fields=[ID_FIELD, CONTENT_FIELD],
            consistency_level="Strong",
        )
        assert {row[ID_FIELD] for row in text_match_rows} == expected_text_match_ids

        res, _ = self.search(
            client,
            collection_name,
            data=["ranking"],
            anns_field=CONTENT_SPARSE_FIELD,
            search_params={"metric_type": "BM25", "params": {}},
            limit=3,
            output_fields=[ID_FIELD, CONTENT_FIELD],
            consistency_level="Strong",
        )
        assert_search_results(res, nq=1, limit=3, metric="BM25", output_fields=[CONTENT_FIELD], required_ids={2610})
        assert result_id(res[0][0]) == 2610
        assert_text_payload(result_entity(res[0][0]).get(CONTENT_FIELD), expected[2610][CONTENT_FIELD])

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize("index_type,index_kwargs", TEXT_USER_DEFINED_INDEX_CASES)
    def test_text_lob_rejects_user_defined_varchar_scalar_index_types(self, index_type, index_kwargs):
        """
        target: verify TEXT rejects user-created scalar indexes, including index types supported by VARCHAR
        method: build a minimal TEXT collection and attempt to create each VARCHAR scalar index type on the TEXT field
        expected: index creation fails with the TEXT user-created scalar index validation error
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        schema = client.create_schema(auto_id=False, enable_dynamic_field=False)
        schema.add_field(field_name=ID_FIELD, datatype=DataType.INT64, is_primary=True)
        schema.add_field(field_name=VECTOR_FIELD, datatype=DataType.FLOAT_VECTOR, dim=DIM)
        schema.add_field(
            field_name=CONTENT_FIELD,
            datatype=DataType.TEXT,
            nullable=True,
            enable_analyzer=True,
            enable_match=True,
            analyzer_params=STANDARD_ANALYZER,
        )
        self.create_collection(client, collection_name, schema=schema, consistency_level="Strong")

        index_params = client.prepare_index_params()
        index_params.add_index(field_name=CONTENT_FIELD, index_type=index_type, **index_kwargs)
        self.create_index(
            client,
            collection_name,
            index_params=index_params,
            check_task=CheckTasks.err_res,
            check_items={
                ct.err_code: 1100,
                ct.err_msg: "TEXT field does not support user-created scalar index",
            },
        )

    @pytest.mark.tags(CaseLabel.L1)
    def test_text_lob_growing_then_sealed_visibility(self):
        """
        target: verify loaded TEXT collection can query/search growing LOB data before flush
        method: load empty collection, insert without flush, query/search, then flush/reload and compare checksums
        expected: growing and sealed reads return identical TEXT payload metadata
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        rows = build_text_lob_rows(base_pk=1000, include_boundaries=True, include_one_mib=False)
        expected = expected_payloads(rows)
        self.create_text_lob_collection(client, collection_name, rows=None, load=True)

        self.insert(client, collection_name, rows)
        growing_rows = query_by_ids(self, client, collection_name, [row[ID_FIELD] for row in rows], [CONTENT_FIELD])
        assert_rows_payload(growing_rows, expected, [CONTENT_FIELD])
        res, _ = self.search(
            client,
            collection_name,
            data=[vector_for_pk(1000)],
            anns_field=VECTOR_FIELD,
            search_params={"metric_type": "COSINE", "params": {}},
            limit=3,
            output_fields=[ID_FIELD, CONTENT_FIELD],
            consistency_level="Strong",
        )
        assert_search_results(
            res,
            nq=1,
            limit=3,
            metric="COSINE",
            output_fields=[CONTENT_FIELD],
            required_ids={1000},
        )

        self.flush(client, collection_name)
        self.release_collection(client, collection_name)
        self.load_collection(client, collection_name)
        sealed_rows = query_by_ids(self, client, collection_name, [row[ID_FIELD] for row in rows], [CONTENT_FIELD])
        assert_rows_payload(sealed_rows, expected, [CONTENT_FIELD])

    @pytest.mark.tags(CaseLabel.L1)
    def test_text_lob_not_loaded_flush_and_reload(self):
        """
        target: verify unloaded TEXT insert/flush succeeds without requiring growing-source logs
        method: create an unloaded TEXT/BM25 collection, insert and flush, then load/reload and validate payloads/search
        expected: flush succeeds and loaded/reloaded TEXT payloads remain exact
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        rows = [
            build_row(2600, "unloaded_small", "g02 unloaded writebuffer vector database"),
            build_row(
                2601,
                "unloaded_large",
                make_text(96 * 1024, "g02-unloaded-large"),
                content_zh=None,
                content_alt=make_text(64 * 1024, "g02-unloaded-alt"),
                varchar_text="g02 unloaded large vector database",
            ),
            build_row(
                2602,
                "unloaded_bm25",
                "writebuffer vector database " * 8 + "manual flush text lob",
            ),
        ]
        expected = expected_payloads(rows)
        self.create_text_lob_collection(client, collection_name, rows=None, load=False)
        load_state = self.get_load_state(client, collection_name)[0]
        assert load_state["state"] == LoadState.NotLoad, (
            f"Expected NotLoad before unloaded insert/flush, but got {load_state['state']}"
        )

        self.insert(client, collection_name, rows)
        self.flush(client, collection_name)
        self.load_collection(client, collection_name)

        row_ids = [row[ID_FIELD] for row in rows]
        rows_by_id = query_by_ids(self, client, collection_name, row_ids, TEXT_FIELDS + [VARCHAR_TEXT_FIELD])
        assert_rows_payload(rows_by_id, expected, TEXT_FIELDS + [VARCHAR_TEXT_FIELD])
        res, _ = self.search(
            client,
            collection_name,
            data=["writebuffer vector database"],
            anns_field=CONTENT_SPARSE_FIELD,
            search_params={"metric_type": "BM25", "params": {}},
            limit=3,
            output_fields=[ID_FIELD, CONTENT_FIELD],
            consistency_level="Strong",
        )
        assert_search_results(
            res,
            nq=1,
            limit=3,
            metric="BM25",
            output_fields=[CONTENT_FIELD],
            required_ids={2602},
        )

        self.release_collection(client, collection_name)
        self.load_collection(client, collection_name)
        reloaded_rows = query_by_ids(self, client, collection_name, row_ids, [CONTENT_FIELD, CONTENT_ALT_FIELD])
        assert_rows_payload(reloaded_rows, expected, [CONTENT_FIELD, CONTENT_ALT_FIELD])

    @pytest.mark.tags(CaseLabel.L1)
    def test_text_lob_text_match_without_text_index_release_reload(self):
        """
        target: verify text_match does not depend on an explicit text scalar index and survives release/load
        method: create TEXT collection without AUTOINDEX on TEXT/VARCHAR, then compare analyzer-derived IDs before and after reload
        expected: text_match IDs match the analyzer oracle before and after release/load
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        rows = [
            build_row(2100, "tm_no_index_hit", "vector database search"),
            build_row(2101, "tm_no_index_miss", "unrelated document"),
            build_row(2102, "tm_no_index_two_tokens", "milvus vector database"),
        ]
        self.create_text_lob_collection(client, collection_name, rows=rows, load=True, include_varchar_text_index=False)
        expected_ids = expected_text_match_ids_by_analyzer(
            self, client, rows, CONTENT_FIELD, "vector database", STANDARD_ANALYZER
        )

        before_rows, _ = self.query(
            client,
            collection_name,
            filter=f'text_match({CONTENT_FIELD}, "vector database")',
            output_fields=[ID_FIELD, CONTENT_FIELD],
            consistency_level="Strong",
        )
        assert {row[ID_FIELD] for row in before_rows} == expected_ids
        self.release_collection(client, collection_name)
        self.load_collection(client, collection_name)
        after_rows, _ = self.query(
            client,
            collection_name,
            filter=f'text_match({CONTENT_FIELD}, "vector database")',
            output_fields=[ID_FIELD, CONTENT_FIELD],
            consistency_level="Strong",
        )
        assert {row[ID_FIELD] for row in after_rows} == expected_ids

    @pytest.mark.tags(CaseLabel.L1)
    def test_text_lob_bm25_sparse_wand_index(self):
        """
        target: verify BM25 generated sparse vectors work with SPARSE_WAND index
        method: create TEXT/BM25 collection with SPARSE_WAND indexes and search after reload
        expected: SPARSE_WAND index metadata is visible and BM25 search returns the ranking-heavy row first
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        rows = build_text_lob_rows(base_pk=2200, include_boundaries=False, include_one_mib=False)
        expected = expected_payloads(rows)
        self.create_text_lob_collection(client, collection_name, rows=rows, load=True, sparse_index_type="SPARSE_WAND")
        self.release_collection(client, collection_name)
        self.load_collection(client, collection_name)

        index_info, _ = self.describe_index(client, collection_name, index_name=CONTENT_SPARSE_FIELD)
        assert index_info["index_type"] == "SPARSE_WAND"
        assert index_info["metric_type"] == "BM25"
        res, _ = self.search(
            client,
            collection_name,
            data=["ranking"],
            anns_field=CONTENT_SPARSE_FIELD,
            search_params={"metric_type": "BM25", "params": {}},
            limit=3,
            output_fields=[ID_FIELD, CONTENT_FIELD],
            consistency_level="Strong",
        )
        assert_search_results(res, nq=1, limit=3, metric="BM25", output_fields=[CONTENT_FIELD], required_ids={2210})
        assert result_id(res[0][0]) == 2210
        assert_text_payload(result_entity(res[0][0]).get(CONTENT_FIELD), expected[2210][CONTENT_FIELD])

    @pytest.mark.tags(CaseLabel.L1)
    def test_text_lob_bm25_nullable_input(self):
        """
        target: verify BM25 handles nullable TEXT input rows
        method: insert BM25 rows with None, empty, and non-empty TEXT, then search after reload
        expected: insert/search succeed, NULL input does not match BM25, and returned TEXT payloads are exact
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        rows = [
            build_row(2250, "bm25_nullable_null", None, content_zh=None, content_alt="nullable null sidecar"),
            build_row(2251, "bm25_nullable_empty", "", content_zh="", content_alt="nullable empty sidecar"),
            build_row(2252, "bm25_nullable_hit", "nullable bm25 vector database ranking ranking"),
            build_row(2253, "bm25_nullable_miss", "unrelated tokens only"),
        ]
        expected = expected_payloads(rows)
        self.create_text_lob_collection(client, collection_name, rows=rows, load=True)
        self.release_collection(client, collection_name)
        self.load_collection(client, collection_name)

        rows_by_id = query_by_ids(self, client, collection_name, [row[ID_FIELD] for row in rows], [CONTENT_FIELD])
        assert_rows_payload(rows_by_id, expected, [CONTENT_FIELD])
        res, _ = self.search(
            client,
            collection_name,
            data=["nullable bm25 ranking"],
            anns_field=CONTENT_SPARSE_FIELD,
            search_params={"metric_type": "BM25", "params": {}},
            limit=4,
            output_fields=[ID_FIELD, CONTENT_FIELD],
            consistency_level="Strong",
        )
        assert_search_results(
            res,
            nq=1,
            limit=4,
            metric="BM25",
            output_fields=[CONTENT_FIELD],
            required_ids={2252},
        )
        hit_ids = {result_id(hit) for hit in res[0]}
        assert 2250 not in hit_ids
        assert 2251 not in hit_ids
        for hit in res[0]:
            pk = result_id(hit)
            assert_text_payload(result_entity(hit).get(CONTENT_FIELD), expected[pk][CONTENT_FIELD])

    @pytest.mark.tags(CaseLabel.L1)
    def test_text_lob_partition_key_text_query_search(self):
        """
        target: verify TEXT payloads work in a collection that uses a partition key
        method: use category as partition key, insert two key values, release/load, then query and search with the key filter
        expected: only matching partition-key rows are visible and TEXT payloads remain exact
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        rows = [
            build_row(2300, "pk_a", "partition key alpha vector database"),
            build_row(2301, "pk_b", "partition key beta unrelated"),
            build_row(2302, "pk_a", make_text(96 * 1024, "partition-key-alpha-large")),
        ]
        expected = expected_payloads([row for row in rows if row[CATEGORY_FIELD] == "pk_a"])
        self.create_text_lob_collection(
            client,
            collection_name,
            rows=rows,
            load=True,
            partition_key_field=CATEGORY_FIELD,
            num_partitions=4,
        )
        self.release_collection(client, collection_name)
        self.load_collection(client, collection_name)

        rows_by_id = query_by_ids(
            self,
            client,
            collection_name,
            [2300, 2302],
            [CONTENT_FIELD],
        )
        assert_rows_payload(rows_by_id, expected, [CONTENT_FIELD])
        res, _ = self.search(
            client,
            collection_name,
            data=[vector_for_pk(2300)],
            anns_field=VECTOR_FIELD,
            search_params={"metric_type": "COSINE", "params": {}},
            filter=f'{CATEGORY_FIELD} == "pk_a"',
            limit=5,
            output_fields=[ID_FIELD, CONTENT_FIELD, CATEGORY_FIELD],
            consistency_level="Strong",
        )
        assert_search_results(res, nq=1, limit=5, metric="COSINE", output_fields=[CONTENT_FIELD, CATEGORY_FIELD])
        assert {result_entity(hit)[CATEGORY_FIELD] for hit in res[0]} == {"pk_a"}

    @pytest.mark.tags(CaseLabel.L1)
    def test_text_lob_64kb_boundary_readback_flush_reload(self):
        """
        target: verify below/equal/above 64 KiB TEXT payloads read back exactly after sealing
        method: insert only threshold-boundary rows, flush, release, load, and validate checksum metadata
        expected: all boundary payloads preserve byte length, prefix, suffix, and sha256
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        rows = [
            build_row(1900, "below_64k", make_text(64 * 1024 - 17, "boundary-below")),
            build_row(1901, "at_64k", make_text(64 * 1024, "boundary-at")),
            build_row(1902, "above_64k", make_text(64 * 1024 + 4096, "boundary-above")),
        ]
        expected = expected_payloads(rows)
        self.create_text_lob_collection(client, collection_name, rows=rows, load=True)
        self.release_collection(client, collection_name)
        self.load_collection(client, collection_name)

        rows_by_id = query_by_ids(self, client, collection_name, [1900, 1901, 1902], [CONTENT_FIELD])
        assert_rows_payload(rows_by_id, expected, [CONTENT_FIELD])

    @pytest.mark.tags(CaseLabel.L1)
    def test_text_lob_delete_and_upsert_large_payload(self):
        """
        target: verify delete/upsert preserve TEXT LOB visibility and checksum correctness
        method: delete one row, upsert another with a larger TEXT payload, flush/reload, then query by PK
        expected: deleted PK is invisible and upserted PK returns the new checksum
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        rows = build_text_lob_rows(base_pk=1100, include_boundaries=False, include_one_mib=False)
        self.create_text_lob_collection(client, collection_name, rows=rows, load=True)

        self.delete(client, collection_name, ids=[1100])
        self.flush(client, collection_name)
        assert_count(self, client, collection_name, len(rows) - 1)
        assert query_by_ids(self, client, collection_name, [1100], [CONTENT_FIELD]) == {}

        updated = build_row(1101, "upsert_large", make_text(128 * 1024, "upsert-large"))
        self.upsert(client, collection_name, [updated])
        self.flush(client, collection_name)
        self.release_collection(client, collection_name)
        self.load_collection(client, collection_name)
        assert_count(self, client, collection_name, len(rows) - 1)

        expected = expected_payloads([updated])
        row = query_by_ids(self, client, collection_name, [1101], [CONTENT_FIELD, CONTENT_ALT_FIELD])
        assert_rows_payload(row, expected, [CONTENT_FIELD, CONTENT_ALT_FIELD])

    @pytest.mark.tags(CaseLabel.L1)
    def test_text_lob_multi_batch_repeated_flush_checksum(self):
        """
        target: verify repeated insert/flush batches keep final TEXT checksum map intact
        method: insert deterministic batches with 64 KiB boundary payloads and flush each batch
        expected: final count and all payload checksums match expected metadata
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        rows = build_text_lob_rows(base_pk=1200, include_boundaries=True, include_one_mib=False)
        expected = expected_payloads(rows)
        self.create_text_lob_collection(client, collection_name, rows=None, load=False)
        for start in range(0, len(rows), 3):
            self.insert(client, collection_name, rows[start : start + 3])
            self.flush(client, collection_name)
        self.load_collection(client, collection_name)

        assert_count(self, client, collection_name, len(rows))
        rows_by_id = query_by_ids(self, client, collection_name, [row[ID_FIELD] for row in rows], [CONTENT_FIELD])
        assert_rows_payload(rows_by_id, expected, [CONTENT_FIELD])

    @pytest.mark.tags(CaseLabel.L2)
    def test_text_lob_ten_mib_payload_flush_reload(self):
        """
        target: verify 10 MiB TEXT payload survives flush/release/load with exact checksum metadata
        method: insert one deterministic 10 MiB row, flush, release, load, and query by primary key
        expected: byte length, char length, prefix, suffix, and sha256 all match the inserted payload
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        rows = [
            build_row(
                1814,
                "ten_mib",
                make_text(10 * 1024 * 1024, "1800-ten-mib"),
                content_zh=None,
                content_alt=make_text(256 * 1024, "1800-alt-ten-mib"),
                varchar_text="ten mib vector database",
            )
        ]
        expected = expected_payloads(rows)
        self.create_text_lob_collection(client, collection_name, rows=rows, load=True)
        self.release_collection(client, collection_name)
        self.load_collection(client, collection_name)

        rows_by_id = query_by_ids(self, client, collection_name, [1814], [CONTENT_FIELD, CONTENT_ALT_FIELD])
        assert_rows_payload(rows_by_id, expected, [CONTENT_FIELD, CONTENT_ALT_FIELD])

    @pytest.mark.tags(CaseLabel.L1)
    def test_text_lob_partition_drop_isolation(self):
        """
        target: verify TEXT LOB rows remain partition-isolated and dropping a partition removes its LOB payloads
        method: insert large TEXT into two partitions, drop one partition, and query the survivor
        expected: remaining partition has exact checksums and dropped partition is no longer visible
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        p1 = "text_lob_p1"
        p2 = "text_lob_p2"
        rows_p1 = [
            build_row(1300, "partition_p1_large", make_text(96 * 1024, "partition-p1")),
            build_row(1301, "partition_p1_small", "partition one vector database"),
        ]
        rows_p2 = [
            build_row(1310, "partition_p2_large", make_text(96 * 1024, "partition-p2")),
            build_row(1311, "partition_p2_small", "partition two vector database"),
        ]
        expected_p2 = expected_payloads(rows_p2)

        self.create_text_lob_collection(client, collection_name, rows=None, load=False)
        self.create_partition(client, collection_name, p1)
        self.create_partition(client, collection_name, p2)
        self.insert(client, collection_name, rows_p1, partition_name=p1)
        self.insert(client, collection_name, rows_p2, partition_name=p2)
        self.flush(client, collection_name)
        self.load_collection(client, collection_name)

        assert_count(self, client, collection_name, len(rows_p1), partition_names=[p1])
        assert_count(self, client, collection_name, len(rows_p2), partition_names=[p2])
        self.release_partitions(client, collection_name, [p1])
        self.drop_partition(client, collection_name, p1)

        rows_by_id = query_by_ids(
            self,
            client,
            collection_name,
            [row[ID_FIELD] for row in rows_p2],
            [CONTENT_FIELD],
            partition_names=[p2],
        )
        assert_rows_payload(rows_by_id, expected_p2, [CONTENT_FIELD])
        assert_count(self, client, collection_name, len(rows_p2), partition_names=[p2])
        assert self.has_partition(client, collection_name, p1)[0] is False
        assert p1 not in self.list_partitions(client, collection_name)[0]
        assert query_by_ids(self, client, collection_name, [row[ID_FIELD] for row in rows_p1], [CONTENT_FIELD]) == {}
        assert_count(self, client, collection_name, len(rows_p2))

    @pytest.mark.tags(CaseLabel.L1)
    def test_text_lob_collection_drop_large_payload(self):
        """
        target: verify dropping a collection removes access to large TEXT LOB payloads
        method: insert and flush large TEXT rows, drop the collection, then query the dropped collection
        expected: collection no longer exists and subsequent query fails with collection-not-found
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        rows = [
            build_row(2700, "drop_collection_large", make_text(128 * 1024, "drop-collection-large")),
            build_row(2701, "drop_collection_small", "drop collection vector database"),
        ]
        expected = expected_payloads(rows)
        self.create_text_lob_collection(client, collection_name, rows=rows, load=True)
        rows_by_id = query_by_ids(self, client, collection_name, [row[ID_FIELD] for row in rows], [CONTENT_FIELD])
        assert_rows_payload(rows_by_id, expected, [CONTENT_FIELD])

        self.drop_collection(client, collection_name)
        assert self.has_collection(client, collection_name)[0] is False
        self.query(
            client,
            collection_name,
            filter=f"{ID_FIELD} >= 0",
            output_fields=[ID_FIELD, CONTENT_FIELD],
            check_task=CheckTasks.err_res,
            check_items={ct.err_code: 1, ct.err_msg: "collection not found"},
        )

    @pytest.mark.tags(CaseLabel.L1)
    def test_text_lob_concurrent_insert_and_flush(self):
        """
        target: verify concurrent TEXT LOB inserts and user flushes keep final data consistent
        method: insert batches from multiple clients while flushes run, then load and validate checksums
        expected: no insert/flush errors, final count matches, and all TEXT payload checksums are exact
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        thread_count = 4
        rows_per_thread = 4
        rows = []
        for thread_idx in range(thread_count):
            base_pk = 2800 + thread_idx * 100
            for row_idx in range(rows_per_thread):
                rows.append(
                    build_row(
                        base_pk + row_idx,
                        f"concurrent_{thread_idx}_{row_idx}",
                        make_text(8192 + row_idx * 1024, f"concurrent-{thread_idx}-{row_idx}"),
                        content_alt=make_text(4096, f"concurrent-alt-{thread_idx}-{row_idx}"),
                    )
                )
        expected = expected_payloads(rows)
        rows_by_thread = [rows[start : start + rows_per_thread] for start in range(0, len(rows), rows_per_thread)]
        self.create_text_lob_collection(client, collection_name, rows=None, load=True)

        def insert_batch(batch):
            worker_client = self._client()
            try:
                result = self.insert(worker_client, collection_name, batch)[0]
                assert result["insert_count"] == len(batch)
            finally:
                self.close(worker_client)

        with ThreadPoolExecutor(max_workers=thread_count + 1) as executor:
            futures = [executor.submit(insert_batch, batch) for batch in rows_by_thread]
            futures.append(executor.submit(self.flush, client, collection_name))
            for future in as_completed(futures):
                future.result()

        self.flush(client, collection_name)
        self.release_collection(client, collection_name)
        self.load_collection(client, collection_name)
        assert_count(self, client, collection_name, len(rows))
        rows_by_id = query_by_ids(
            self,
            client,
            collection_name,
            [row[ID_FIELD] for row in rows],
            [CONTENT_FIELD, CONTENT_ALT_FIELD],
        )
        assert_rows_payload(rows_by_id, expected, [CONTENT_FIELD, CONTENT_ALT_FIELD])

    @pytest.mark.tags(CaseLabel.L1)
    def test_text_lob_add_collection_field(self):
        """
        target: verify MilvusClient.add_collection_field supports adding a TEXT field
        method: add an analyzer-enabled nullable TEXT field after one row exists, then insert null/LOB values
        expected: old and omitted values are null, the LOB payload is exact, and text_match finds only that row
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        added_text_field = "added_text"

        schema = client.create_schema(auto_id=False, enable_dynamic_field=False)
        schema.add_field(field_name=ID_FIELD, datatype=DataType.INT64, is_primary=True)
        schema.add_field(field_name=VECTOR_FIELD, datatype=DataType.FLOAT_VECTOR, dim=DIM)
        index_params = client.prepare_index_params()
        index_params.add_index(field_name=VECTOR_FIELD, index_type="FLAT", metric_type="COSINE")
        # Passing index_params to create_collection auto-loads it; keep it unloaded until schema evolution is done.
        self.create_collection(
            client,
            collection_name,
            schema=schema,
            consistency_level="Strong",
            load=False,
        )
        self.create_index(client, collection_name, index_params=index_params)

        insert_result, _ = self.insert(
            client,
            collection_name,
            [{ID_FIELD: 0, VECTOR_FIELD: vector_for_pk(0)}],
        )
        assert insert_result["insert_count"] == 1

        client.add_collection_field(
            collection_name=collection_name,
            field_name=added_text_field,
            data_type=DataType.TEXT,
            nullable=True,
            enable_analyzer=True,
            enable_match=True,
            analyzer_params=STANDARD_ANALYZER,
        )

        desc, _ = self.describe_collection(client, collection_name)
        added_field = next(field for field in desc["fields"] if field["name"] == added_text_field)
        assert is_text_datatype(added_field["type"])
        assert added_field.get("nullable") is True
        assert str(added_field["params"].get("enable_analyzer")).lower() == "true"
        assert str(added_field["params"].get("enable_match")).lower() == "true"
        analyzer_params = added_field["params"]["analyzer_params"]
        if isinstance(analyzer_params, str):
            analyzer_params = json.loads(analyzer_params)
        assert analyzer_params == STANDARD_ANALYZER

        marker = "addedfieldmarker "
        added_text = marker + make_text(64 * 1024 + 1 - len(marker), "added-field")
        insert_result, _ = self.insert(
            client,
            collection_name,
            [
                {ID_FIELD: 1, VECTOR_FIELD: vector_for_pk(1)},
                {ID_FIELD: 2, VECTOR_FIELD: vector_for_pk(2), added_text_field: added_text},
            ],
        )
        assert insert_result["insert_count"] == 2

        self.flush(client, collection_name)
        self.load_collection(client, collection_name)

        expected = {
            0: {added_text_field: payload_meta(None)},
            1: {added_text_field: payload_meta(None)},
            2: {added_text_field: payload_meta(added_text)},
        }
        rows_by_id = query_by_ids(
            self,
            client,
            collection_name,
            expected.keys(),
            [added_text_field],
        )
        assert_rows_payload(rows_by_id, expected, [added_text_field])
        assert added_text_field in rows_by_id[0]
        assert added_text_field in rows_by_id[1]
        assert rows_by_id[0][added_text_field] is None
        assert rows_by_id[1][added_text_field] is None

        matched_rows, _ = self.query(
            client,
            collection_name,
            filter=f"text_match({added_text_field}, 'addedfieldmarker')",
            output_fields=[ID_FIELD],
            consistency_level="Strong",
        )
        assert {row[ID_FIELD] for row in matched_rows} == {2}

    @pytest.mark.tags(CaseLabel.L1)
    def test_text_lob_drop_field_sdk_support(self):
        """
        target: verify dropping a TEXT field if the MilvusClient SDK exposes the API
        method: skip on SDKs without drop_collection_field; otherwise drop content_alt and verify remaining TEXT
        expected: unsupported SDKs are skipped; supported SDKs remove the field without corrupting content
        """
        client = self._client()
        if not hasattr(client, "drop_collection_field"):
            pytest.skip("MilvusClient.drop_collection_field is not available in this SDK checkout")

        collection_name = cf.gen_collection_name_by_testcase_name()
        rows = build_text_lob_rows(base_pk=1400, include_boundaries=False, include_one_mib=False)
        expected = expected_payloads(rows)
        self.create_text_lob_collection(client, collection_name, rows=rows, load=True)
        client.drop_collection_field(collection_name=collection_name, field_name=CONTENT_ALT_FIELD)
        self.flush(client, collection_name)
        self.release_collection(client, collection_name)
        self.load_collection(client, collection_name)

        desc, _ = self.describe_collection(client, collection_name)
        assert CONTENT_ALT_FIELD not in {field["name"] for field in desc["fields"]}
        dropped_field_rows, _ = self.query(
            client,
            collection_name,
            filter=f"{ID_FIELD} in {[row[ID_FIELD] for row in rows]}",
            output_fields=[ID_FIELD, CONTENT_ALT_FIELD],
            consistency_level="Strong",
        )
        assert all(CONTENT_ALT_FIELD not in row for row in dropped_field_rows)
        rows_by_id = query_by_ids(
            self,
            client,
            collection_name,
            [row[ID_FIELD] for row in rows],
            [CONTENT_FIELD],
        )
        assert_rows_payload(rows_by_id, expected, [CONTENT_FIELD])

    @pytest.mark.tags(CaseLabel.L2)
    def test_text_lob_compaction_low_delete_ratio(self):
        """
        target: verify compaction after low delete ratio preserves TEXT and text-search visibility
        method: delete a small subset, compact, reload, and validate survivor checksums/search membership
        expected: all survivor TEXT checksums match and text/BM25 search returns survivor rows without deleted rows
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        rows = build_text_lob_rows(base_pk=1500, include_boundaries=True, include_one_mib=False)
        extra_rows = [build_row(1520 + i, f"reuse_extra_{i}", make_text(4096, f"reuse-{i}")) for i in range(4)]
        rows.extend(extra_rows)
        self.create_text_lob_collection(client, collection_name, rows=None, load=False)
        insert_flush_batches(self, client, collection_name, rows, batch_size=8)
        self.load_collection(client, collection_name)
        deleted = {1500, 1520}
        self.delete(client, collection_name, ids=sorted(deleted))
        self.flush(client, collection_name)
        text_match_before, _ = self.query(
            client,
            collection_name,
            filter=f'text_match({CONTENT_FIELD}, "vector database")',
            output_fields=[ID_FIELD],
            consistency_level="Strong",
        )
        text_match_before_ids = {row[ID_FIELD] for row in text_match_before}
        assert text_match_before_ids
        assert deleted.isdisjoint(text_match_before_ids)
        before_segments = persistent_segment_ids(self, client, collection_name)

        compact_id = self.compact(client, collection_name)[0]
        assert self.wait_for_compaction_ready(client, compact_id, timeout=300)
        after_segments = persistent_segment_ids(self, client, collection_name)
        assert_compaction_rewrote_segments(before_segments, after_segments)
        self.release_collection(client, collection_name)
        self.load_collection(client, collection_name)

        survivors = [row for row in rows if row[ID_FIELD] not in deleted]
        expected = expected_payloads(survivors)
        assert_count(self, client, collection_name, len(survivors))
        rows_by_id = query_by_ids(self, client, collection_name, [row[ID_FIELD] for row in survivors], [CONTENT_FIELD])
        assert_rows_payload(rows_by_id, expected, [CONTENT_FIELD])
        bm25, _ = self.search(
            client,
            collection_name,
            data=["vector database"],
            anns_field=CONTENT_SPARSE_FIELD,
            search_params={"metric_type": "BM25", "params": {}},
            limit=len(text_match_before_ids),
            output_fields=[ID_FIELD],
            consistency_level="Strong",
        )
        assert_search_results(
            bm25,
            nq=1,
            limit=len(text_match_before_ids),
            metric="BM25",
            output_fields=[ID_FIELD],
            expected_ids=text_match_before_ids,
            required_ids=text_match_before_ids,
        )
        text_match_after, _ = self.query(
            client,
            collection_name,
            filter=f'text_match({CONTENT_FIELD}, "vector database")',
            output_fields=[ID_FIELD],
            consistency_level="Strong",
        )
        assert {row[ID_FIELD] for row in text_match_after} == text_match_before_ids

    @pytest.mark.tags(CaseLabel.L2)
    def test_text_lob_compaction_high_delete_ratio(self):
        """
        target: verify compaction after high delete ratio preserves surviving LOB payloads
        method: delete most rows, compact, reload, and validate exact survivor checksum map
        expected: deleted rows are gone and remaining TEXT payloads are byte-exact
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        rows = [build_row(1600 + i, f"rewrite_{i}", make_text(8192 + i, f"rewrite-{i}")) for i in range(12)]
        rows.append(build_row(1700, "rewrite_large_survivor", make_text(128 * 1024, "rewrite-survivor")))
        self.create_text_lob_collection(client, collection_name, rows=None, load=False)
        insert_flush_batches(self, client, collection_name, rows, batch_size=6)
        self.load_collection(client, collection_name)
        survivor_ids = {1600, 1605, 1610, 1700}
        deleted = [row[ID_FIELD] for row in rows if row[ID_FIELD] not in survivor_ids]
        self.delete(client, collection_name, ids=deleted)
        self.flush(client, collection_name)
        before_segments = persistent_segment_ids(self, client, collection_name)

        compact_id = self.compact(client, collection_name)[0]
        assert self.wait_for_compaction_ready(client, compact_id, timeout=300)
        after_segments = persistent_segment_ids(self, client, collection_name)
        assert_compaction_rewrote_segments(before_segments, after_segments)
        self.release_collection(client, collection_name)
        self.load_collection(client, collection_name)

        survivors = [row for row in rows if row[ID_FIELD] in survivor_ids]
        expected = expected_payloads(survivors)
        assert_count(self, client, collection_name, len(survivors))
        rows_by_id = query_by_ids(self, client, collection_name, survivor_ids, [CONTENT_FIELD])
        assert_rows_payload(rows_by_id, expected, [CONTENT_FIELD])
        assert query_by_ids(self, client, collection_name, deleted, [CONTENT_FIELD]) == {}


class TestMilvusClientTextLOBNegative(TestMilvusClientV2Base):
    """Negative TEXT/BM25 validation coverage for schema, index, insert, and text_match errors."""

    def _schema_with_base_fields(self, client):
        schema = client.create_schema(auto_id=False, enable_dynamic_field=False)
        schema.add_field(field_name=ID_FIELD, datatype=DataType.INT64, is_primary=True)
        schema.add_field(field_name=VECTOR_FIELD, datatype=DataType.FLOAT_VECTOR, dim=DIM)
        return schema

    @pytest.mark.tags(CaseLabel.L1)
    def test_text_lob_rejects_invalid_bm25_function_schemas(self):
        """
        target: verify invalid BM25 function schemas are rejected
        method: create collections with missing input, missing output, analyzer-disabled input, nullable output, and wrong output type
        expected: collection creation fails with a clear validation error
        """
        client = self._client()
        cases = []

        schema = self._schema_with_base_fields(client)
        schema.add_field(field_name=CONTENT_FIELD, datatype=DataType.TEXT, enable_analyzer=True)
        schema.add_field(field_name=CONTENT_SPARSE_FIELD, datatype=DataType.SPARSE_FLOAT_VECTOR)
        schema.add_function(
            Function(
                name="missing_input",
                function_type=FunctionType.BM25,
                input_field_names=["missing"],
                output_field_names=[CONTENT_SPARSE_FIELD],
            )
        )
        cases.append((schema, "not found"))

        schema = self._schema_with_base_fields(client)
        schema.add_field(field_name=CONTENT_FIELD, datatype=DataType.TEXT, enable_analyzer=True)
        schema.add_function(
            Function(
                name="missing_output",
                function_type=FunctionType.BM25,
                input_field_names=[CONTENT_FIELD],
                output_field_names=["missing_sparse"],
            )
        )
        cases.append((schema, "not found"))

        schema = self._schema_with_base_fields(client)
        schema.add_field(field_name=CONTENT_FIELD, datatype=DataType.TEXT, enable_analyzer=False)
        schema.add_field(field_name=CONTENT_SPARSE_FIELD, datatype=DataType.SPARSE_FLOAT_VECTOR)
        schema.add_function(
            Function(
                name="input_without_analyzer",
                function_type=FunctionType.BM25,
                input_field_names=[CONTENT_FIELD],
                output_field_names=[CONTENT_SPARSE_FIELD],
            )
        )
        cases.append((schema, "analyzer"))

        schema = self._schema_with_base_fields(client)
        schema.add_field(field_name=CONTENT_FIELD, datatype=DataType.TEXT, enable_analyzer=True)
        schema.add_field(field_name=CONTENT_SPARSE_FIELD, datatype=DataType.SPARSE_FLOAT_VECTOR, nullable=True)
        schema.add_function(
            Function(
                name="nullable_output",
                function_type=FunctionType.BM25,
                input_field_names=[CONTENT_FIELD],
                output_field_names=[CONTENT_SPARSE_FIELD],
            )
        )
        cases.append((schema, "nullable"))

        schema = self._schema_with_base_fields(client)
        schema.add_field(field_name=CONTENT_FIELD, datatype=DataType.TEXT, enable_analyzer=True)
        schema.add_field(field_name=CONTENT_SPARSE_FIELD, datatype=DataType.FLOAT_VECTOR, dim=DIM)
        schema.add_function(
            Function(
                name="wrong_output_type",
                function_type=FunctionType.BM25,
                input_field_names=[CONTENT_FIELD],
                output_field_names=[CONTENT_SPARSE_FIELD],
            )
        )
        cases.append((schema, "SPARSE_FLOAT_VECTOR"))

        for schema, err_msg in cases:
            self.create_collection(
                client,
                cf.gen_collection_name_by_testcase_name(),
                schema=schema,
                check_task=CheckTasks.err_res,
                check_items={ct.err_code: 65535, ct.err_msg: err_msg},
            )

    @pytest.mark.tags(CaseLabel.L1)
    def test_text_lob_rejects_bad_bm25_metric_and_manual_output(self):
        """
        target: verify BM25 output rejects wrong index metric and user-supplied function output
        method: create a valid BM25 collection, try L2 sparse index, then insert with generated sparse field present
        expected: both operations fail with validation errors
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        schema = build_text_lob_schema(client, include_default=False)
        self.create_collection(client, collection_name, schema=schema, consistency_level="Strong")

        index_params = client.prepare_index_params()
        index_params.add_index(field_name=CONTENT_SPARSE_FIELD, index_type="SPARSE_INVERTED_INDEX", metric_type="L2")
        self.create_index(
            client,
            collection_name,
            index_params=index_params,
            check_task=CheckTasks.err_res,
            check_items={ct.err_code: 1100, ct.err_msg: "metric"},
        )

        bad_row = build_row(2400, "manual_bm25_output", "manual output should be rejected")
        bad_row.pop(CONTENT_DEFAULT_FIELD, None)
        bad_row[CONTENT_SPARSE_FIELD] = {1: 1.0}
        self.insert(
            client,
            collection_name,
            [bad_row],
            check_task=CheckTasks.err_res,
            check_items={ct.err_code: 65535, ct.err_msg: "function output field"},
        )

    @pytest.mark.tags(CaseLabel.L1)
    def test_text_lob_rejects_partition_key_schema(self):
        """
        target: verify TEXT fields cannot be used as partition keys
        method: create a collection schema whose TEXT field is marked is_partition_key
        expected: collection creation fails because only Int64 and VarChar are supported partition-key types
        """
        client = self._client()
        schema = self._schema_with_base_fields(client)
        schema.add_field(
            field_name=CONTENT_FIELD,
            datatype=DataType.TEXT,
            enable_analyzer=True,
            enable_match=True,
            analyzer_params=STANDARD_ANALYZER,
            is_partition_key=True,
        )
        self.create_collection(
            client,
            cf.gen_collection_name_by_testcase_name(),
            schema=schema,
            consistency_level="Strong",
            check_task=CheckTasks.err_res,
            check_items={
                ct.err_code: 1,
                ct.err_msg: "DataType.INT64 or DataType.VARCHAR",
            },
        )

    @pytest.mark.tags(CaseLabel.L1)
    def test_text_lob_rejects_default_value_schema(self):
        """
        target: verify TEXT fields reject default_value schema definitions
        method: create a collection whose TEXT field declares default_value
        expected: collection creation fails before rows can omit the TEXT field
        """
        client = self._client()
        schema = self._schema_with_base_fields(client)
        schema.add_field(
            field_name=CONTENT_DEFAULT_FIELD,
            datatype=DataType.TEXT,
            nullable=True,
            enable_analyzer=True,
            enable_match=True,
            analyzer_params=STANDARD_ANALYZER,
            default_value=DEFAULT_TEXT,
        )
        self.create_collection(
            client,
            cf.gen_collection_name_by_testcase_name(),
            schema=schema,
            consistency_level="Strong",
            check_task=CheckTasks.err_res,
            check_items={ct.err_code: 1100, ct.err_msg: "default_value"},
        )

    @pytest.mark.tags(CaseLabel.L1)
    def test_text_lob_text_match_requires_enable_match(self):
        """
        target: verify text_match rejects analyzer-enabled TEXT fields without enable_match
        method: create TEXT field with analyzer but no match support and run text_match filter
        expected: query fails with enable_match validation error
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        schema = client.create_schema(auto_id=False, enable_dynamic_field=False)
        schema.add_field(field_name=ID_FIELD, datatype=DataType.INT64, is_primary=True)
        schema.add_field(field_name=VECTOR_FIELD, datatype=DataType.FLOAT_VECTOR, dim=DIM)
        schema.add_field(field_name=CONTENT_FIELD, datatype=DataType.TEXT, enable_analyzer=True, enable_match=False)
        index_params = client.prepare_index_params()
        index_params.add_index(field_name=VECTOR_FIELD, index_type="FLAT", metric_type="COSINE")
        self.create_collection(
            client, collection_name, schema=schema, index_params=index_params, consistency_level="Strong"
        )
        self.insert(
            client,
            collection_name,
            [{ID_FIELD: 2500, VECTOR_FIELD: vector_for_pk(2500), CONTENT_FIELD: "vector database"}],
        )
        self.flush(client, collection_name)
        self.load_collection(client, collection_name)
        self.query(
            client,
            collection_name,
            filter=f'text_match({CONTENT_FIELD}, "vector")',
            output_fields=[ID_FIELD],
            check_task=CheckTasks.err_res,
            check_items={ct.err_code: 65535, ct.err_msg: "does not enable match"},
        )

    @pytest.mark.tags(CaseLabel.L1)
    def test_text_lob_rejects_invalid_analyzer_config(self):
        """
        target: verify invalid analyzer configuration is rejected at collection creation
        method: create TEXT field with an unknown tokenizer
        expected: collection creation fails with analyzer validation error
        """
        client = self._client()
        schema = self._schema_with_base_fields(client)
        schema.add_field(
            field_name=CONTENT_FIELD,
            datatype=DataType.TEXT,
            enable_analyzer=True,
            enable_match=True,
            analyzer_params={"tokenizer": "not_a_tokenizer"},
        )
        self.create_collection(
            client,
            cf.gen_collection_name_by_testcase_name(),
            schema=schema,
            check_task=CheckTasks.err_res,
            check_items={ct.err_code: 65535, ct.err_msg: "analyzer"},
        )


class TestMilvusClientTextLOBEnvironmentGated(TestMilvusClientV2Base):
    """Plan coverage that needs external storage, config mutation, or cluster orchestration."""

    @pytest.mark.tags(CaseLabel.ClusterOnly)
    def test_text_lob_release_during_insert_or_flush(self):
        """
        target: verify release handoff preserves TEXT LOB rows while insert and flush start at the same time
        method: use threading.Barrier to launch release, flush of existing growing rows, and insert of new rows together
        expected: post-concurrency flush and reload return exact TEXT payloads for both flushed and inserted rows
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        flush_rows = [
            build_row(
                2900,
                "release_flush_small",
                "release handoff flush row vector database",
            ),
            build_row(
                2901,
                "release_flush_large",
                make_text(128 * 1024, "release-flush-large"),
                content_zh=None,
                content_alt=make_text(64 * 1024, "release-flush-alt"),
                varchar_text="release flush large vector database",
            ),
        ]
        insert_rows = [
            build_row(
                2910,
                "release_insert_small",
                "release handoff insert row vector database",
            ),
            build_row(
                2911,
                "release_insert_large",
                make_text(192 * 1024, "release-insert-large"),
                content_zh=None,
                content_alt=make_text(64 * 1024, "release-insert-alt"),
                varchar_text="release insert large vector database",
            ),
        ]
        rows = flush_rows + insert_rows
        expected = expected_payloads(rows)

        schema = build_text_lob_schema(client)
        index_params = build_text_lob_index_params(client)
        self.create_collection(
            client,
            collection_name,
            schema=schema,
            index_params=index_params,
            consistency_level="Strong",
        )
        self.load_collection(client, collection_name)
        result = self.insert(client, collection_name, flush_rows)[0]
        assert result["insert_count"] == len(flush_rows)

        start_barrier = threading.Barrier(3)

        def run_with_barrier(name, operation):
            worker_client = self._client()
            try:
                start_barrier.wait(timeout=30)
                return operation(worker_client)
            except threading.BrokenBarrierError as exc:
                pytest.fail(f"{name} did not reach the release/insert/flush barrier: {exc}")
            finally:
                self.close(worker_client)

        def insert_during_release(worker_client):
            result = self.insert(worker_client, collection_name, insert_rows)[0]
            assert result["insert_count"] == len(insert_rows)

        with ThreadPoolExecutor(max_workers=3) as executor:
            futures = [
                executor.submit(
                    run_with_barrier,
                    "release",
                    lambda worker_client: self.release_collection(worker_client, collection_name),
                ),
                executor.submit(
                    run_with_barrier,
                    "flush",
                    lambda worker_client: self.flush(worker_client, collection_name),
                ),
                executor.submit(run_with_barrier, "insert", insert_during_release),
            ]
            for future in as_completed(futures):
                future.result()

        self.flush(client, collection_name)
        self.load_collection(client, collection_name)
        row_ids = [row[ID_FIELD] for row in rows]
        rows_by_id = query_by_ids(
            self,
            client,
            collection_name,
            row_ids,
            [CONTENT_FIELD, CONTENT_ALT_FIELD, VARCHAR_TEXT_FIELD],
        )
        assert_rows_payload(rows_by_id, expected, [CONTENT_FIELD, CONTENT_ALT_FIELD, VARCHAR_TEXT_FIELD])
        assert_count(self, client, collection_name, len(rows))

        self.release_collection(client, collection_name)
        self.load_collection(client, collection_name)
        reloaded_rows = query_by_ids(
            self,
            client,
            collection_name,
            row_ids,
            [CONTENT_FIELD, CONTENT_ALT_FIELD, VARCHAR_TEXT_FIELD],
        )
        assert_rows_payload(reloaded_rows, expected, [CONTENT_FIELD, CONTENT_ALT_FIELD, VARCHAR_TEXT_FIELD])

    @pytest.mark.tags(CaseLabel.ClusterOnly)
    def test_text_lob_inline_vs_lob_object_layout(self, minio_host, minio_bucket):
        """
        target: verify T-03 TEXT LOB threshold classification in the persisted Storage V3 layout
        method: insert below/equal/above inlineThreshold payloads, seal/reload, then inspect parquet references in MinIO
        expected: below-threshold payload is inline, equal/above-threshold payloads are LOB refs, and all read back exactly
        """
        threshold = int_env("MILVUS_TEXT_INLINE_THRESHOLD", 65536)
        assert threshold > 0, f"MILVUS_TEXT_INLINE_THRESHOLD must be positive, got {threshold}"
        minio_client = new_minio_client(minio_host)
        ensure_minio_bucket(minio_client, minio_bucket)
        root_path = os.getenv("MILVUS_MINIO_ROOT_PATH", "file")

        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        payloads = {
            3100: make_text(threshold - 1, "inline-threshold-below"),
            3101: make_text(threshold, "inline-threshold-at"),
            3102: make_text(threshold + 1, "inline-threshold-above"),
        }
        rows = [
            {ID_FIELD: pk, VECTOR_FIELD: vector_for_pk(pk), CONTENT_FIELD: content} for pk, content in payloads.items()
        ]
        expected = expected_payloads(rows)

        schema = client.create_schema(auto_id=False, enable_dynamic_field=False)
        schema.add_field(field_name=ID_FIELD, datatype=DataType.INT64, is_primary=True)
        schema.add_field(field_name=VECTOR_FIELD, datatype=DataType.FLOAT_VECTOR, dim=DIM)
        schema.add_field(field_name=CONTENT_FIELD, datatype=DataType.TEXT)
        index_params = client.prepare_index_params()
        index_params.add_index(field_name=VECTOR_FIELD, index_type="FLAT", metric_type="COSINE")
        self.create_collection(
            client,
            collection_name,
            schema=schema,
            index_params=index_params,
            consistency_level="Strong",
        )
        self.insert(client, collection_name, rows)
        self.flush(client, collection_name)
        self.release_collection(client, collection_name)
        self.load_collection(client, collection_name)

        rows_by_id = query_by_ids(self, client, collection_name, payloads.keys(), [CONTENT_FIELD])
        assert_rows_payload(rows_by_id, expected, [CONTENT_FIELD])

        desc, _ = self.describe_collection(client, collection_name)
        collection_id = collection_id_from_desc(desc)
        id_field_id = field_id_from_desc(desc, ID_FIELD)
        content_field_id = field_id_from_desc(desc, CONTENT_FIELD)
        segments, _ = self.list_persistent_segments(client, collection_name)
        segment = select_segment_by_rows(segments, len(rows))
        segment_id = segment_identifier(segment)

        segment_base = find_segment_base_path(minio_client, minio_bucket, root_path, collection_id, segment_id)
        assert segment_base is not None, (
            f"could not find segment {segment_id} under "
            f"{object_key(root_path, 'insert_log', collection_id)} in bucket {minio_bucket}"
        )
        partition_base = "/".join(segment_base.split("/")[:-1])
        refs = read_text_reference_rows(
            minio_client,
            minio_bucket,
            segment_base,
            content_field_id,
            id_field_id,
            expected_ids=payloads.keys(),
        )
        assert set(refs) == set(payloads), f"missing TEXT references: actual={set(refs)}, expected={set(payloads)}"

        assert classify_text_ref(refs[3100], payloads[3100]) == "inline"
        for pk in [3101, 3102]:
            assert classify_text_ref(refs[pk], payloads[pk]) == "lob"

        lob_prefix = object_key(partition_base, "lobs", content_field_id, "_data")
        lob_keys = list_minio_keys(minio_client, minio_bucket, lob_prefix)
        assert lob_keys, f"expected LOB objects under {lob_prefix}"
        for pk in [3101, 3102]:
            key = lob_object_key(partition_base, content_field_id, refs[pk])
            assert key in lob_keys, f"LOB object {key} referenced by row {pk} was not found in {lob_keys}"

    @pytest.mark.tags(CaseLabel.L2)
    def test_text_lob_bm25_multi_analyzer_by_field(self):
        """
        target: verify TEXT BM25 supports multi_analyzer_params selected by a scalar field
        method: create a TEXT BM25 field with by_field analyzers, insert multilingual rows, and search per analyzer
        expected: schema records multi_analyzer_params and each analyzer search returns only its matching TEXT row
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        language_field = "language"
        multi_analyzer_params = {
            "by_field": language_field,
            "analyzers": {
                "en": {"type": "english"},
                "zh": {"type": "chinese"},
                "default": {"tokenizer": "standard"},
            },
            "alias": {"eng": "en", "chinese": "zh"},
        }
        schema = client.create_schema(auto_id=False, enable_dynamic_field=True)
        schema.add_field(field_name=ID_FIELD, datatype=DataType.INT64, is_primary=True)
        schema.add_field(field_name=VECTOR_FIELD, datatype=DataType.FLOAT_VECTOR, dim=DIM)
        schema.add_field(field_name=language_field, datatype=DataType.VARCHAR, max_length=16)
        schema.add_field(
            field_name=CONTENT_FIELD,
            datatype=DataType.TEXT,
            nullable=True,
            enable_analyzer=True,
            multi_analyzer_params=multi_analyzer_params,
        )
        schema.add_field(field_name=CONTENT_SPARSE_FIELD, datatype=DataType.SPARSE_FLOAT_VECTOR)
        schema.add_function(
            Function(
                name="content_multi_analyzer_bm25",
                function_type=FunctionType.BM25,
                input_field_names=[CONTENT_FIELD],
                output_field_names=[CONTENT_SPARSE_FIELD],
                params={},
            )
        )
        index_params = client.prepare_index_params()
        index_params.add_index(field_name=VECTOR_FIELD, index_type="FLAT", metric_type="COSINE")
        index_params.add_index(field_name=CONTENT_SPARSE_FIELD, index_type="SPARSE_INVERTED_INDEX", metric_type="BM25")
        rows = [
            {
                ID_FIELD: 3100,
                VECTOR_FIELD: vector_for_pk(3100),
                language_field: "eng",
                CONTENT_FIELD: "wolves running swiftly across vector database archives",
            },
            {
                ID_FIELD: 3101,
                VECTOR_FIELD: vector_for_pk(3101),
                language_field: "chinese",
                CONTENT_FIELD: "向量数据库 支持 中文检索 和 稀疏向量 排序",
            },
            {
                ID_FIELD: 3102,
                VECTOR_FIELD: vector_for_pk(3102),
                language_field: "fr",
                CONTENT_FIELD: "default analyzer vector database document",
            },
        ]
        expected = expected_payloads(rows)
        self.create_collection(
            client,
            collection_name,
            schema=schema,
            index_params=index_params,
            consistency_level="Strong",
        )
        self.insert(client, collection_name, rows)
        self.flush(client, collection_name)
        self.load_collection(client, collection_name)

        desc, _ = self.describe_collection(client, collection_name)
        fields = {field["name"]: field for field in desc["fields"]}
        assert json.loads(fields[CONTENT_FIELD]["params"]["multi_analyzer_params"]) == multi_analyzer_params
        rows_by_id = query_by_ids(self, client, collection_name, [row[ID_FIELD] for row in rows], [CONTENT_FIELD])
        assert_rows_payload(rows_by_id, expected, [CONTENT_FIELD])
        search_cases = [
            ("running wolves", "en", {3100}),
            ("中文检索 稀疏向量", "zh", {3101}),
            ("default analyzer document", "default", {3102}),
        ]
        for query, analyzer_name, expected_ids in search_cases:
            res, _ = self.search(
                client,
                collection_name,
                data=[query],
                anns_field=CONTENT_SPARSE_FIELD,
                search_params={"metric_type": "BM25", "analyzer_name": analyzer_name},
                limit=3,
                output_fields=[ID_FIELD, CONTENT_FIELD, language_field],
                consistency_level="Strong",
            )
            assert_search_results(
                res,
                nq=1,
                limit=3,
                metric="BM25",
                output_fields=[CONTENT_FIELD, language_field],
                required_ids=expected_ids,
            )
            hit_ids = {result_id(hit) for hit in res[0]}
            assert hit_ids == expected_ids
            for hit in res[0]:
                pk = result_id(hit)
                assert_text_payload(result_entity(hit).get(CONTENT_FIELD), expected[pk][CONTENT_FIELD])

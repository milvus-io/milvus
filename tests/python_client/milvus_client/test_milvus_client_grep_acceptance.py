"""Grep acceptance through public PyMilvus APIs.

Case IDs refer to the reviewed Milvus Grep test design.
"""

import hashlib
import io
import json
import os
import random
import re
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from contextlib import nullcontext
from copy import deepcopy
from uuid import uuid4

import pytest
from base.client_v2_base import TestMilvusClientV2Base
from common.common_type import CaseLabel
from common.grep_common import (
    assert_results,
    basic_rows,
    code_rows,
    detail,
    expected_literal,
    projected_rows,
    semantics_rows,
    source_row,
    sparse_rows,
)
from pymilvus import DataType
from pymilvus.client.types import LoadState
from pymilvus.client.types import SegmentState as ClientSegmentState
from pymilvus.exceptions import MilvusException, ParamError
from pymilvus.function_chain import FunctionChain, FunctionChainStage
from pymilvus.grpc_gen.common_pb2 import SegmentState
from utils.util_log import test_log as log

TIMEOUT = 60
FIELDS = ["id", "content", "repo", "path", "revision"]


def new_client(db_name=""):
    base = TestMilvusClientV2Base()
    uri, token = base._connection_params()
    client, _ = base.init_milvus_client(uri=uri, token=token or "", db_name=db_name, timeout=TIMEOUT)
    return client


def grep_chain(args, *, text_field="content", match_field="matches"):
    return FunctionChain(
        FunctionChainStage.QUERY_SCAN,
        name="grep",
        output_columns=[match_field],
    ).grep(text_field, arguments=args, output=match_field)


def run_grep(client, name, args="-F -e error", *, entry="grep", text_field="content", match_field="matches", **kwargs):
    options = {
        "filter": "",
        "output_fields": FIELDS,
        "limit": 100,
        "offset": 0,
        "consistency_level": "Strong",
        "timeout": TIMEOUT,
    }
    options.update(kwargs)
    if entry == "grep":
        return client.grep(name, text_field, args, match_field=match_field, **options)
    assert entry == "query"
    return client.query(
        name, function_chains=grep_chain(args, text_field=text_field, match_field=match_field), **options
    )


def visible_ids(client, name, *, ignore_growing=False):
    return {
        row["id"]
        for row in client.query(
            name,
            filter="",
            output_fields=["id"],
            limit=16384,
            consistency_level="Strong",
            ignore_growing=ignore_growing,
            timeout=TIMEOUT,
        )
    }


def confirm_state(client, name, rows, sealed_ids, *, wait=False):
    """Confirm the segment layout and verify the visible IDs in that layout."""
    expected = {row["id"] for row in rows}
    assert visible_ids(client, name) == expected, "Strong visibility barrier did not return the insert ledger"

    def segment_layout():
        sealed = {
            segment.segment_id: segment.num_rows
            for segment in client.list_loaded_segments(name, timeout=TIMEOUT)
            if segment.state == SegmentState.Sealed
        }
        persistent = {segment.segment_id for segment in client.list_persistent_segments(name, timeout=TIMEOUT)}
        return sealed, persistent

    # Even a post-check can see old and new segment IDs during compaction.
    deadline = time.monotonic() + (TIMEOUT if wait else 5)
    while True:
        sealed, persistent = segment_layout()
        sealed_rows = sum(sealed.values())
        if sealed_ids:
            layout_ready = set(sealed) <= persistent and sealed_rows == len(sealed_ids)
        else:
            layout_ready = not persistent and not sealed
        if layout_ready:
            actual_sealed = visible_ids(client, name, ignore_growing=True)
            if actual_sealed == sealed_ids:
                log.info(
                    f"Grep state evidence collection={name} visible={len(expected)} sealed={sealed_rows} "
                    f"segments={sorted(sealed.items())}"
                )
                return
            if not wait or time.monotonic() >= deadline:
                # A Query can race a segment replacement. Confirm the same layout.
                confirmed_sealed, confirmed_persistent = segment_layout()
                still_persistent = set(sealed) <= confirmed_persistent
                if not sealed_ids:
                    still_persistent = not confirmed_persistent
                if confirmed_sealed == sealed and still_persistent:
                    pytest.fail(
                        f"Sealed query result mismatch for {name}: expected IDs={sorted(sealed_ids)}, "
                        f"actual IDs={sorted(actual_sealed)}, missing IDs={sorted(sealed_ids - actual_sealed)}, "
                        f"unexpected IDs={sorted(actual_sealed - sealed_ids)}, loaded Sealed segments={sealed}"
                    )
                sealed, persistent = confirmed_sealed, confirmed_persistent
                sealed_rows = sum(sealed.values())

        layout_evidence = f"loaded Sealed segments={sealed}, persistent segment IDs={sorted(persistent)}"
        if len(sealed_ids) < sealed_rows <= len(expected) and set(sealed) <= persistent:
            pytest.skip(
                f"BLOCKED: Growing data was sealed in {name}; expected {len(sealed_ids)} Sealed rows, {layout_evidence}"
            )
        if time.monotonic() >= deadline:
            pytest.fail(
                f"Target segment layout unavailable for {name}; expected {len(sealed_ids)} Sealed rows, {layout_evidence}"
            )
        time.sleep(0.5)


def prepare_collection(
    client,
    request,
    rows,
    *,
    dtype=DataType.VARCHAR,
    state="growing",
    shards=1,
    complex_fields=False,
    name=None,
    db_name="",
    partition_key=False,
    string_pk=False,
):
    name = name or f"grep_{dtype.name.lower()}_{state}_{uuid4().hex[:12]}"
    schema = client.create_schema(auto_id=False, enable_dynamic_field=complex_fields)
    schema.add_field(
        "id",
        DataType.VARCHAR if string_pk else DataType.INT64,
        is_primary=True,
        **({"max_length": 64} if string_pk else {}),
    )
    schema.add_field("vector", DataType.FLOAT_VECTOR, dim=4)
    schema.add_field("content", dtype, nullable=True, **({"max_length": 65535} if dtype == DataType.VARCHAR else {}))
    for field in ("repo", "path", "revision"):
        schema.add_field(
            field,
            DataType.VARCHAR,
            max_length=256,
            **({"is_partition_key": True} if partition_key and field == "repo" else {}),
        )
    if complex_fields:
        schema.add_field("meta", DataType.JSON)
        schema.add_field("tags", DataType.ARRAY, element_type=DataType.VARCHAR, max_capacity=8, max_length=64)
        schema.add_field("score", DataType.INT64, nullable=True)
        parts = client.create_struct_field_schema()
        parts.add_field("text", DataType.VARCHAR, max_length=64)
        parts.add_field("rank", DataType.INT64)
        schema.add_field("parts", DataType.ARRAY, element_type=DataType.STRUCT, struct_schema=parts, max_capacity=8)
    indexes = client.prepare_index_params()
    indexes.add_index("vector", index_type="FLAT", metric_type="L2")

    def cleanup():
        cleanup_client = new_client(db_name)
        try:
            if cleanup_client.has_collection(name, timeout=TIMEOUT):
                cleanup_client.drop_collection(name, timeout=TIMEOUT)
        finally:
            cleanup_client.close()

    # Register before setup so partial creation/load/insert failures clean up.
    request.addfinalizer(cleanup)
    client.create_collection(
        name,
        schema=schema,
        index_params=indexes,
        shards_num=shards,
        consistency_level="Strong",
        timeout=TIMEOUT,
        **({"num_partitions": 8} if partition_key else {}),
    )
    split = len(rows) // 2 if state == "mixed" else len(rows)
    if rows[:split]:
        client.insert(name, rows[:split], timeout=TIMEOUT)
    sealed_ids = set()
    if state in ("sealed", "mixed"):
        client.flush(name, timeout=TIMEOUT)
        sealed_ids = {row["id"] for row in rows[:split]}
        confirm_state(client, name, rows[:split], sealed_ids, wait=True)
    if rows[split:]:
        client.insert(name, rows[split:], timeout=TIMEOUT)
    confirm_state(client, name, rows, sealed_ids, wait=True)
    return {"name": name, "rows": rows, "sealed_ids": sealed_ids}


@pytest.fixture
def grep_client():
    client = new_client()
    try:
        yield client
    finally:
        client.close()


@pytest.fixture(
    scope="module",
    params=[("VARCHAR", "growing"), ("VARCHAR", "sealed"), ("TEXT", "growing"), ("TEXT", "sealed")],
    ids=lambda value: "-".join(value),
)
def grep_semantic_data(request):
    client = new_client()
    try:
        dtype, state = request.param
        data = prepare_collection(client, request, semantics_rows(), dtype=DataType[dtype], state=state)
        data["dtype"] = dtype
        return data
    finally:
        client.close()


def assert_input_error(operation, reason, *, server_codes=(1100, 1101, 1102, 2042)):
    """Only a public, relevant request error satisfies a negative test."""
    with pytest.raises(MilvusException) as caught:
        operation()
    error = caught.value
    message = str(error)
    # Request parameter codes from merr/errors.go; 2042 is the native
    # InvalidParameter code preserved by merr/segcore_codes_gen.go. Runtime
    # errors containing words like "grep" or "QueryScan" cannot satisfy this.
    if not isinstance(error, ParamError):
        assert error.code in server_codes, (error.code, message)
    assert not re.search(r"deadline|timed? out|connection|unavailable|not ready", message, re.I), message
    assert re.search(reason, message, re.I), message
    log.info(
        f"Grep rejection layer={'SDK' if isinstance(error, ParamError) else 'server'} "
        f"type={type(error).__name__} code={error.code} reason={message}"
    )


@pytest.fixture(scope="module")
def grep_validation_data(request):
    client = new_client()
    try:
        return prepare_collection(client, request, basic_rows())
    finally:
        client.close()


@pytest.fixture(scope="module")
def grep_complex_data(request):
    client = new_client()
    try:
        return prepare_collection(client, request, projected_rows(), complex_fields=True)
    finally:
        client.close()


@pytest.fixture(scope="module", params=["growing", "sealed"])
def grep_projection_data(request):
    client = new_client()
    try:
        return prepare_collection(client, request, projected_rows(), state=request.param, complex_fields=True)
    finally:
        client.close()


@pytest.mark.xdist_group("GrepShared")
class TestMilvusClientGrepValidationIndependent(TestMilvusClientV2Base):
    """When to add tests here: public request validation independent of segment state.

    Fixture: grep_validation_data; no state Cartesian product or forced Flush.
    """

    # Fixture ownership avoids the base's global SDK-connection cleanup.
    def setup_method(self, method):
        self._setup_objects()

    def teardown_method(self, method):
        pass

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize(
        "args,code,reason",
        [
            ("-i", 1101, "requires at least one -e/--regexp pattern"),
            ("-e", 1101, "requires a pattern"),
            ("error", 1100, "positional token"),
            ("-e error app.log", 1100, "positional token"),
            ("rg -e error", 1100, "positional token"),
            ("-v -e error", 1100, "unsupported grep option -v"),
            ("-c -e error", 1100, "unsupported grep option -c"),
            ("-P -e error", 1100, "unsupported grep option -P"),
            ("-U -e error", 1100, "unsupported grep option -U"),
            ("--multiline -e error", 1100, 'unsupported grep option "--multiline"'),
            ("-C 0 -e error", 1100, "unsupported grep option -C"),
            ("--unknown -e error", 1100, 'unsupported grep option "--unknown"'),
            ("--ignore-case=false -e error", 1100, "does not accept a value"),
            ('-e "error', 1100, "unclosed quote"),
            ("-w -x -e error", 1100, "-w and -x cannot be combined"),
            ("-x -w -e error", 1100, "-w and -x cannot be combined"),
            ("-e '['", 2042, "invalid regex"),
            ("-e 'foo\nbar'", 1100, "literal LF"),
            ("-e error\x00", 1100, "must not contain NUL"),
            (r"-e 'foo\nbar'", 2042, "pattern requires a line terminator"),
            ("-e '(?-u)error'", 2042, "inline Unicode disabling is not supported"),
        ],
    )
    def test_argument_errors_and_recovery(self, grep_client, grep_validation_data, args, code, reason):
        """TC-L1-05.
        target: Invalid requests fail explicitly without poisoning later queries.
        method: Both public entries run each bad argument on nonempty visible data.
        expected: Relevant public error; same client immediately returns valid truth.
        """
        client, data = grep_client, grep_validation_data
        name, rows = data["name"], data["rows"]
        output_fields = ["id", "content", "repo", "path", "revision"]
        expected_grep = expected_literal(rows, "error")
        expected_query = {row["id"]: [] for row in rows}

        for entry in ("baseline", "grep", "query"):
            if entry != "baseline":
                log.info(f"Grep negative arguments={args!r} entry={entry}")
                if entry == "grep":
                    with pytest.raises(MilvusException) as caught:
                        client.grep(
                            collection_name=name,
                            text_field="content",
                            args=args,
                            filter="",
                            output_fields=output_fields,
                            limit=100,
                            offset=0,
                            match_field="matches",
                            consistency_level="Strong",
                            timeout=60,
                        )
                else:
                    chain = FunctionChain(FunctionChainStage.QUERY_SCAN, name="grep", output_columns=["matches"])
                    chain.grep("content", arguments=args, output="matches")
                    with pytest.raises(MilvusException) as caught:
                        client.query(
                            collection_name=name,
                            filter="",
                            output_fields=output_fields,
                            limit=100,
                            offset=0,
                            function_chains=chain,
                            consistency_level="Strong",
                            timeout=60,
                        )
                error = caught.value
                log.info(
                    f"Grep rejection entry={entry} type={type(error).__name__} code={error.code} reason={error.message}"
                )
                assert not isinstance(error, ParamError), error
                assert error.code == code, error
                assert reason in error.message, error

            grep_rows = client.grep(
                collection_name=name,
                text_field="content",
                args="-F -e error",
                filter="",
                output_fields=output_fields,
                limit=100,
                offset=0,
                match_field="matches",
                consistency_level="Strong",
                timeout=60,
            )
            assert_results(grep_rows, rows, expected_grep, output_fields)
            query_rows = client.query(
                collection_name=name,
                filter="",
                output_fields=output_fields,
                limit=100,
                offset=0,
                consistency_level="Strong",
                timeout=60,
            )
            assert_results(query_rows, rows, expected_query, output_fields, match_field=None)

    @pytest.mark.tags(CaseLabel.L1)
    def test_input_fields_output_names_and_chains(self, grep_client, grep_complex_data):
        """TC-L1-06.
        target: Unsupported inputs/names/chains fail without corrupting legal output.
        method: Public builders only, with valid complex-field and Grep baselines.
        expected: Relevant errors; text_hits and ordinary JSON/ARRAY remain intact.
        """
        client, data = grep_client, grep_complex_data
        collection_name, rows = data["name"], data["rows"]
        fields = FIELDS + ["meta", "tags", "parts", "dynamic_text", "score", "vector"]
        ordinary = client.query(
            collection_name=collection_name,
            filter="",
            output_fields=fields,
            limit=100,
            offset=0,
            consistency_level="Strong",
            timeout=60,
        )
        assert_results(ordinary, rows, {row["id"]: [] for row in rows}, fields, match_field=None)
        expected = expected_literal(rows, "error")

        def recovered():
            result = client.grep(
                collection_name=collection_name,
                text_field="content",
                args="-F -e error",
                filter="",
                output_fields=fields,
                limit=100,
                offset=0,
                match_field="text_hits",
                consistency_level="Strong",
                timeout=60,
            )
            return assert_results(result, rows, expected, fields, match_field="text_hits")

        baseline = recovered()
        chain = FunctionChain(FunctionChainStage.QUERY_SCAN, name="grep", output_columns=["text_hits"])
        chain.grep("content", arguments="-F -e error", output="text_hits")
        result = client.query(
            collection_name=collection_name,
            filter="",
            output_fields=fields,
            limit=100,
            offset=0,
            function_chains=chain,
            consistency_level="Strong",
            timeout=60,
        )
        assert assert_results(result, rows, expected, fields, match_field="text_hits") == baseline

        input_fields = (
            "missing_text",
            "id",
            "meta",
            "tags",
            "vector",
            "dynamic_text",
            'meta["text"]',
            "tags[0]",
            "parts[text]",
        )
        field_cases = [
            (field, "matches", MilvusException, "must be a schema VarChar or Text field") for field in input_fields
        ]
        field_cases += [
            ("content", "content", MilvusException, "must not overwrite its input field"),
            ("content", "id", MilvusException, "conflicts with a schema field"),
            ("content", "RowID", MilvusException, "conflicts with a schema field"),
            ("content", "Timestamp", MilvusException, "conflicts with a schema field"),
            ("content", "_original_idx", ParamError, "output_columns contains reserved name"),
        ]
        for text_field, match_field, error_type, reason in field_cases:
            for entry in ("grep", "query"):
                log.info(f"Grep field rejection entry={entry} input={text_field!r} output={match_field!r}")
                with pytest.raises(error_type, match=reason) as caught:
                    if entry == "grep":
                        client.grep(
                            collection_name=collection_name,
                            text_field=text_field,
                            args="-F -e error",
                            filter="",
                            output_fields=["id"],
                            limit=100,
                            offset=0,
                            match_field=match_field,
                            consistency_level="Strong",
                            timeout=60,
                        )
                    else:
                        chain = FunctionChain(FunctionChainStage.QUERY_SCAN, name="grep", output_columns=[match_field])
                        chain.grep(text_field, arguments="-F -e error", output=match_field)
                        client.query(
                            collection_name=collection_name,
                            filter="",
                            output_fields=["id"],
                            limit=100,
                            offset=0,
                            function_chains=chain,
                            consistency_level="Strong",
                            timeout=60,
                        )
                error = caught.value
                assert type(error) is error_type, error
                if error_type is MilvusException:
                    assert error.code == 1100, error
                log.info(
                    f"Grep rejection entry={entry} type={type(error).__name__} code={error.code} reason={error.message}"
                )
                recovered()

        chain_cases = [
            (
                "duplicate outputs",
                lambda: FunctionChain(
                    FunctionChainStage.QUERY_SCAN, name="grep", output_columns=["matches", "matches"]
                ).grep("content", arguments="-e error", output="matches"),
                ParamError,
                "output_columns must be unique",
            ),
            (
                "unproduced output",
                lambda: FunctionChain(FunctionChainStage.QUERY_SCAN, name="grep", output_columns=["not_produced"]).grep(
                    "content", arguments="-e error", output="matches"
                ),
                MilvusException,
                'output "not_produced" is not produced',
            ),
            (
                "unnamed export",
                lambda: FunctionChain(FunctionChainStage.QUERY_SCAN, output_columns=["matches"]).grep(
                    "content", arguments="-e error", output="matches"
                ),
                ParamError,
                "output_columns requires a named QUERY_SCAN function chain",
            ),
            (
                "multiple chains",
                lambda: [grep_chain("-e error"), grep_chain("-e error", match_field="second")],
                ParamError,
                "query supports at most one QUERY_SCAN function chain",
            ),
            (
                "multiple Grep operations",
                lambda: grep_chain("-e error").grep("content", arguments="-e error", output="second"),
                MilvusException,
                "QueryScan requires exactly one grep operator",
            ),
            (
                "Grep followed by limit",
                lambda: grep_chain("-e error").limit(1),
                MilvusException,
                "QueryScan requires exactly one grep operator",
            ),
            (
                "wrong stage",
                lambda: FunctionChain(FunctionChainStage.L2_RERANK).grep(
                    "content", arguments="-e error", output="matches"
                ),
                ParamError,
                "grep requires QUERY_SCAN stage",
            ),
        ]
        for label, build, error_type, reason in chain_cases:
            log.info(f"Grep chain rejection case={label}")
            with pytest.raises(error_type, match=reason) as caught:
                client.query(
                    collection_name=collection_name,
                    filter="",
                    output_fields=["id"],
                    limit=100,
                    offset=0,
                    function_chains=build(),
                    consistency_level="Strong",
                    timeout=60,
                )
            error = caught.value
            assert type(error) is error_type, error
            if error_type is MilvusException:
                assert error.code == 1100, error
            log.info(
                f"Grep rejection case={label} type={type(error).__name__} code={error.code} reason={error.message}"
            )
            recovered()

    @pytest.mark.tags(CaseLabel.L1)
    def test_query_combination_boundaries(self, grep_client, grep_complex_data):
        """TC-L1-07.
        target: Grep rejects unsupported Query combinations instead of ignoring them.
        method: Establish each ordinary operation's result before adding the chain.
        expected: Count/aggregate/group/order/element/iterator/Search errors are relevant.
        """
        client, data = grep_client, grep_complex_data
        name, rows = data["name"], data["rows"]
        ids = {101, 202, 303}
        grep_rows = client.grep(
            collection_name=name,
            text_field="content",
            args="-F -e error",
            filter="id >= 0",
            output_fields=FIELDS,
            limit=100,
            offset=0,
            match_field="matches",
            consistency_level="Strong",
            timeout=60,
        )
        assert_results(grep_rows, rows, expected_literal(rows, "error"), FIELDS)

        chain = FunctionChain(FunctionChainStage.QUERY_SCAN, name="grep", output_columns=["matches"])
        chain.grep("content", arguments="-F -e error", output="matches")
        cases = [
            ("count", {"output_fields": ["count(*)"], "limit": None}, [{"count(*)": 3}]),
            ("sum", {"output_fields": ["sum(id)"], "limit": None}, [{"sum(id)": 606}]),
            (
                "group by",
                {"output_fields": ["repo", "count(*)"], "group_by_fields": ["repo"], "limit": 100},
                [{"repo": "a", "count(*)": 2}, {"repo": "b", "count(*)": 1}],
            ),
            (
                "order by",
                {"output_fields": ["id"], "order_by_fields": [{"field": "id", "order": "desc"}], "limit": 100},
                [{"id": 303}, {"id": 202}, {"id": 101}],
            ),
            (
                "element filter",
                {"filter": "element_filter(parts, $[rank] > 0)", "output_fields": ["id"], "limit": 100},
                [{"id": pk, "offset": 0} for pk in sorted(ids)],
            ),
        ]
        chain_error = "Query chains do not support count, aggregation, group-by, order-by or iterator"
        for label, parameters, expected in cases:
            options = {"filter": "id >= 0", **parameters}
            reason = "QueryScan supports only document-level Query" if label == "element filter" else chain_error
            for phase in ("baseline", "grep", "after grep", "query chain", "after query chain"):
                log.info(f"Grep combination case={label} phase={phase}")
                if phase == "grep":
                    with pytest.raises(MilvusException, match=reason) as caught:
                        client.grep(
                            collection_name=name,
                            text_field="content",
                            args="-F -e error",
                            offset=0,
                            match_field="matches",
                            consistency_level="Strong",
                            timeout=60,
                            **options,
                        )
                    assert type(caught.value) is MilvusException and caught.value.code == 1100, caught.value
                elif phase == "query chain":
                    with pytest.raises(MilvusException, match=reason) as caught:
                        client.query(
                            collection_name=name,
                            offset=0,
                            function_chains=chain,
                            consistency_level="Strong",
                            timeout=60,
                            **options,
                        )
                    assert type(caught.value) is MilvusException and caught.value.code == 1100, caught.value
                else:
                    result = list(
                        client.query(collection_name=name, offset=0, consistency_level="Strong", timeout=60, **options)
                    )
                    if label == "group by":
                        result.sort(key=lambda row: row["repo"])
                    elif label == "element filter":
                        result.sort(key=lambda row: row["id"])
                    assert result == expected, (label, phase, result)

        for phase in ("baseline", "query chain", "after query chain"):
            log.info(f"Grep combination case=iterator phase={phase}")
            if phase == "query chain":
                iterator = None
                try:
                    with pytest.raises(MilvusException, match=chain_error) as caught:
                        iterator = client.query_iterator(
                            collection_name=name,
                            filter="id >= 0",
                            output_fields=["id"],
                            batch_size=2,
                            limit=100,
                            function_chains=chain,
                            consistency_level="Strong",
                            timeout=60,
                        )
                        iterator.next()
                    assert type(caught.value) is MilvusException and caught.value.code == 1100, caught.value
                finally:
                    if iterator is not None:
                        iterator.close()
            else:
                iterator = client.query_iterator(
                    collection_name=name,
                    filter="id >= 0",
                    output_fields=["id"],
                    batch_size=2,
                    limit=100,
                    consistency_level="Strong",
                    timeout=60,
                )
                found = []
                try:
                    while batch := iterator.next():
                        found.extend(batch)
                    assert len(found) == len(ids) and {row["id"] for row in found} == ids
                finally:
                    iterator.close()

        # An empty export list lets Search reach its stage validation.
        search_chain = FunctionChain(FunctionChainStage.QUERY_SCAN, name="grep", output_columns=[])
        search_chain.grep("content", arguments="-F -e error", output="matches")
        for phase in ("baseline", "query chain", "after query chain"):
            log.info(f"Grep combination case=search phase={phase}")
            if phase == "query chain":
                with pytest.raises(
                    MilvusException, match="stage .*QueryScan.* is not supported in search request"
                ) as caught:
                    client.search(
                        collection_name=name,
                        data=[rows[0]["vector"]],
                        anns_field="vector",
                        limit=3,
                        search_params={"metric_type": "L2"},
                        function_chains=search_chain,
                        consistency_level="Strong",
                        timeout=60,
                    )
                assert type(caught.value) is MilvusException and caught.value.code == 1100, caught.value
            else:
                found = client.search(
                    collection_name=name,
                    data=[rows[0]["vector"]],
                    anns_field="vector",
                    limit=3,
                    search_params={"metric_type": "L2"},
                    consistency_level="Strong",
                    timeout=60,
                )
                assert len(found) == 1 and len(found[0]) == 3
                assert {hit["id"] for hit in found[0]} == ids


class GrepBase(TestMilvusClientV2Base):
    """Collections/connections are owned by fixtures, not global base teardown.

    The normal base teardown removes every SDK connection. Avoid closing other
    tests' shared clients and do not perform unrelated role/resource cleanup.
    Client construction still uses the repository base and CLI connection flags.
    """

    def setup_method(self, method):
        self._setup_objects()

    def teardown_method(self, method):
        pass


@pytest.mark.xdist_group("GrepShared")
class TestMilvusClientGrepSealedAndGrowing(GrepBase):
    """When to add tests here: public SDK entry points across text types and data states.

    Fixture: grep_client and per-case collection cleanup.
    """

    @pytest.mark.tags(CaseLabel.L0)
    @pytest.mark.asyncio
    @pytest.mark.parametrize("dtype", ["VARCHAR", "TEXT"])
    @pytest.mark.parametrize("state", ["growing", "sealed"])
    async def test_text_types_and_sdk_entries(self, grep_client, request, dtype, state):
        """TC-L0-02.
        target: Both text types and three public SDK entries return decoded matches.
        method: Compare complete projected rows with independent literal truth.
        expected: Twelve type/state/entry combinations agree, including every span.
        """
        client = grep_client
        name = f"grep_l0_02_{dtype.lower()}_{state}_{uuid4().hex[:12]}"

        def drop_test_collection():
            if client.has_collection(name, timeout=TIMEOUT):
                client.drop_collection(name, timeout=TIMEOUT)

        request.addfinalizer(drop_test_collection)
        schema = client.create_schema(auto_id=False, enable_dynamic_field=False)
        schema.add_field("id", DataType.INT64, is_primary=True)
        schema.add_field("vector", DataType.FLOAT_VECTOR, dim=4)
        text_options = {"max_length": 65535} if dtype == "VARCHAR" else {}
        schema.add_field("content", DataType[dtype], nullable=True, **text_options)
        schema.add_field("repo", DataType.VARCHAR, max_length=256)
        schema.add_field("path", DataType.VARCHAR, max_length=256)
        schema.add_field("revision", DataType.VARCHAR, max_length=256)
        client.create_collection(name, schema=schema, shards_num=1, consistency_level="Strong", timeout=TIMEOUT)
        indexes = client.prepare_index_params()
        indexes.add_index("vector", index_type="FLAT", metric_type="L2")
        client.create_index(name, index_params=indexes, timeout=TIMEOUT)
        client.load_collection(name, timeout=TIMEOUT)

        rows = basic_rows()
        client.insert(name, rows, timeout=TIMEOUT)
        sealed_ids = set()
        if state == "sealed":
            client.flush(name, timeout=TIMEOUT)
            sealed_ids = {row["id"] for row in rows}
        confirm_state(client, name, rows, sealed_ids, wait=True)

        fields = ["id", "path", "revision"]
        expected = expected_literal(rows, "error")
        assert set(expected) == {1, 3, 4, 5, 6, 11, 12}

        grep_rows = client.grep(
            collection_name=name,
            text_field="content",
            args="-F -e error",
            filter="",
            output_fields=fields,
            limit=100,
            offset=0,
            match_field="matches",
            consistency_level="Strong",
            timeout=TIMEOUT,
        )

        chain = FunctionChain(FunctionChainStage.QUERY_SCAN, name="grep", output_columns=["matches"])
        chain.grep("content", arguments="-F -e error", output="matches")
        query_rows = client.query(
            collection_name=name,
            filter="",
            output_fields=fields,
            limit=100,
            offset=0,
            function_chains=chain,
            consistency_level="Strong",
            timeout=TIMEOUT,
        )

        self.init_async_milvus_client()
        async_client = self.async_milvus_client_wrap.async_milvus_client
        try:
            async_rows = await async_client.grep(
                collection_name=name,
                text_field="content",
                args="-F -e error",
                filter="",
                output_fields=fields,
                limit=100,
                offset=0,
                match_field="matches",
                consistency_level="Strong",
                timeout=TIMEOUT,
            )
            async_by_id = assert_results(async_rows, rows, expected, fields)
        finally:
            await async_client.close()

        grep_by_id = assert_results(grep_rows, rows, expected, fields)
        query_by_id = assert_results(query_rows, rows, expected, fields)
        assert grep_by_id == query_by_id == async_by_id
        confirm_state(client, name, rows, sealed_ids)


@pytest.mark.xdist_group("GrepShared")
class TestMilvusClientGrepSemanticsSealedAndGrowing(GrepBase):
    """When to add tests here: read-only matching, grammar or coordinate contracts.

    Fixture: grep_semantic_data, shared VARCHAR/TEXT collections in both states.
    """

    @pytest.mark.tags(CaseLabel.L1)
    def test_matching_parameters_and_combinations(self, grep_client, grep_semantic_data):
        """TC-L1-01, ASCII options and grammar.
        target: Aliases, grouping, repeated patterns and last-wins case flags agree.
        method: Both SDK entries run each option spelling against fixed source truth.
        expected: Exact fields and spans, with no duplicate matches for repeated -e.
        """
        client, data = grep_client, grep_semantic_data
        name, rows = data["name"], data["rows"]
        confirm_state(client, name, rows, data["sealed_ids"])
        basic = [row for row in rows if row["id"] < 100]
        literal = expected_literal(basic, "error")
        insensitive = {**literal, **expected_literal(basic, "ERROR")}
        whole_word = {pk: hits for pk, hits in literal.items() if pk != 3}
        whole_line = {4: literal[4], 11: [detail("error", 2, 12, [(0, 5)])]}
        dot = {9: [detail("a.b", 1, 0, [(0, 3)])], 15: [detail("aXb", 1, 0, [(0, 3)])]}
        option_groups = [
            (["-e a.b", "--regexp a.b"], dot),
            (["-F -e a.b", "--fixed-strings --regexp a.b"], {9: dot[9]}),
            (
                [
                    "-i -e error",
                    "--ignore-case --regexp error",
                    "-Fi -e error",
                    "-F -i -e error",
                    "-ieerror",
                    "-s -i -e error",
                ],
                insensitive,
            ),
            (
                [
                    "-s -e error",
                    "--case-sensitive --regexp error",
                    "-i -s -e error",
                    "--regexp=error",
                    "-eerror",
                    "-F -F -e error",
                    "-e error -e error",
                    "-e error --",
                ],
                literal,
            ),
            (["-w -e error", "--word-regexp --regexp error", "-w -w -e error"], whole_word),
            (["-x -e error", "--line-regexp --regexp error", "-x -x -e error"], whole_line),
            (["-F -e 'error timeout'", '-F -e "error timeout"'], expected_literal(basic, "error timeout")),
            (["-F -e error -e a.b"], {**literal, 9: dot[9]}),
        ]
        matrix = [("id < 100", args, expected) for spellings, expected in option_groups for args in spellings]
        for pk, args, pattern in [
            (501, "-e -C", "-C"),
            (502, r"-F -e 'a\b'", r"a\b"),
            (504, r"-F -e error\ timeout", "error timeout"),
        ]:
            expected = expected_literal(rows, pattern, lambda row: row["id"] == pk)
            matrix.append((f"id == {pk}", args, expected))
        # Ordered alternatives can overlap; reversing order changes the span.
        for args, end in [("-e foo -e foobar", 3), ("-e foobar -e foo", 6)]:
            matrix.append(("id == 503", args, {503: [detail("foobar", 1, 0, [(0, end)])]}))

        for expr, args, expected in matrix:
            log.info(f"Grep arguments={args!r}, filter={expr!r}")
            grep_rows = client.grep(
                collection_name=name,
                text_field="content",
                args=args,
                filter=expr,
                output_fields=FIELDS,
                limit=100,
                offset=0,
                match_field="matches",
                consistency_level="Strong",
                timeout=TIMEOUT,
            )
            chain = FunctionChain(FunctionChainStage.QUERY_SCAN, name="grep", output_columns=["matches"])
            chain.grep("content", arguments=args, output="matches")
            query_rows = client.query(
                collection_name=name,
                filter=expr,
                output_fields=FIELDS,
                limit=100,
                offset=0,
                function_chains=chain,
                consistency_level="Strong",
                timeout=TIMEOUT,
            )
            grep_by_id = assert_results(grep_rows, rows, expected, FIELDS)
            query_by_id = assert_results(query_rows, rows, expected, FIELDS)
            assert grep_by_id == query_by_id
        confirm_state(client, name, rows, data["sealed_ids"])

    @pytest.mark.tags(CaseLabel.L1)
    def test_non_english_case_combinations(self, grep_client, grep_semantic_data):
        """TC-L1-01, Unicode simple folding.
        target: Non-ASCII case handling preserves original text and byte positions.
        method: Use manually reviewed expected pairs, never Python casefold truth.
        expected: Greek final sigma folds; sharp-s expansion and normalization do not.
        """
        client, data = grep_client, grep_semantic_data
        name, rows = data["name"], data["rows"]
        confirm_state(client, name, rows, data["sealed_ids"])
        cases = [
            ("ÉCOLE", [101], True),
            ("Σ", [102, 103], True),
            ("Ж", [104], True),
            ("错误 ÉCOLE Ж", [105], True),
            ("Straße", [106], False),
            ("é", [107], False),
        ]
        matrix = []
        for pattern, ids, folds in cases:
            selected = [row for row in rows if row["id"] in ids]
            folded = (
                {
                    row["id"]: [detail(row["content"], 1, 0, [(0, len(row["content"].encode("utf-8")))])]
                    for row in selected
                }
                if folds
                else {}
            )
            for literal_prefix in ("", "-F "):
                for flags, expected in [("-i", folded), ("-s", {}), ("-i -s", {}), ("-s -i", folded)]:
                    matrix.append((f"id in {ids}", f"{literal_prefix}{flags} -e '{pattern}'", expected))
        matrix.append(("id == 108", "-i -e é", {108: [detail("É é", 1, 0, [(0, 2), (3, 5)])]}))

        for expr, args, expected in matrix:
            log.info(f"Grep arguments={args!r}, filter={expr!r}")
            grep_rows = client.grep(
                collection_name=name,
                text_field="content",
                args=args,
                filter=expr,
                output_fields=FIELDS,
                limit=100,
                offset=0,
                match_field="matches",
                consistency_level="Strong",
                timeout=TIMEOUT,
            )
            chain = FunctionChain(FunctionChainStage.QUERY_SCAN, name="grep", output_columns=["matches"])
            chain.grep("content", arguments=args, output="matches")
            query_rows = client.query(
                collection_name=name,
                filter=expr,
                output_fields=FIELDS,
                limit=100,
                offset=0,
                function_chains=chain,
                consistency_level="Strong",
                timeout=TIMEOUT,
            )
            grep_by_id = assert_results(grep_rows, rows, expected, FIELDS)
            query_by_id = assert_results(query_rows, rows, expected, FIELDS)
            assert grep_by_id == query_by_id
        confirm_state(client, name, rows, data["sealed_ids"])

    @pytest.mark.tags(CaseLabel.L1)
    def test_line_numbers_and_byte_positions(self, grep_client, grep_semantic_data):
        """TC-L1-02.
        target: Coordinates retain LF, CR, BOM, NUL, tabs and Unicode distinctions.
        method: Compare fixed expected coordinates and recover spans from line and field bytes.
        expected: Duplicate lines remain separate; only the terminating LF is removed.
        """
        client, data = grep_client, grep_semantic_data
        name, rows, sealed_ids = data["name"], data["rows"], data["sealed_ids"]
        confirm_state(client, name, rows, sealed_ids)
        expected = {
            201: [detail("中文 error error", 2, 11, [(7, 12), (13, 18)])],
            202: [detail("🙂error e\u0301 error", 1, 0, [(4, 9), (14, 19)])],
            203: [detail("error", 1, 0, [(0, 5)]), detail("error", 2, 6, [(0, 5)])],
            204: [detail("error\r", 2, 3, [(0, 5)])],
            205: [detail("error then error", 3, 12, [(0, 5), (11, 16)])],
            206: [detail("\ufefferror\x00\terror", 1, 0, [(3, 8), (10, 15)])],
            207: [
                detail("L03 error", 3, 2, [(4, 9)]),
                detail("L07 error", 7, 15, [(4, 9)]),
                detail("L10 error", 10, 27, [(4, 9)]),
            ],
        }
        output_fields = ["id", "content", "repo", "path", "revision"]
        result = client.grep(
            collection_name=name,
            text_field="content",
            args="-F -e error",
            filter="id >= 201 and id <= 207",
            output_fields=output_fields,
            limit=100,
            offset=0,
            match_field="matches",
            consistency_level="Strong",
            timeout=60,
        )
        assert_results(result, rows, expected, output_fields)
        confirm_state(client, name, rows, sealed_ids)

    @pytest.mark.tags(CaseLabel.L1)
    def test_null_empty_lines_and_zero_length_matches(self, grep_client, grep_semantic_data):
        """TC-L1-03.
        target: Empty matches occur only at valid UTF-8 boundaries on real lines.
        method: Check explicit empty pattern, anchors and mixed a* spans.
        expected: NULL/empty field never match; no extra line is invented at EOF.
        """
        client, data = grep_client, grep_semantic_data
        name, rows, sealed_ids = data["name"], data["rows"], data["sealed_ids"]
        confirm_state(client, name, rows, sealed_ids)

        empty_lines = {
            303: [detail("", 1, 0, [(0, 0)])],
            304: [detail("", 1, 0, [(0, 0)]), detail("", 2, 1, [(0, 0)])],
        }
        all_samples = "id >= 301 and id <= 306"
        cases = [
            (
                '-e ""',
                all_samples,
                {
                    **empty_lines,
                    305: [detail("é中", 1, 0, [(0, 0), (2, 2), (5, 5)])],
                    306: [detail("aé", 1, 0, [(0, 0), (1, 1), (3, 3)])],
                },
            ),
            ("-e '^$'", all_samples, empty_lines),
            (
                "-e '^'",
                all_samples,
                {**empty_lines, 305: [detail("é中", 1, 0, [(0, 0)])], 306: [detail("aé", 1, 0, [(0, 0)])]},
            ),
            (
                "-e '$'",
                all_samples,
                {**empty_lines, 305: [detail("é中", 1, 0, [(5, 5)])], 306: [detail("aé", 1, 0, [(3, 3)])]},
            ),
            ("-e 'a*'", "id == 306", {306: [detail("aé", 1, 0, [(0, 1), (3, 3)])]}),
        ]
        output_fields = ["id", "content", "repo", "path", "revision"]
        for args, expr, expected in cases:
            log.info(f"Grep arguments={args!r}, filter={expr!r}")
            result = client.grep(
                collection_name=name,
                text_field="content",
                args=args,
                filter=expr,
                output_fields=output_fields,
                limit=100,
                offset=0,
                match_field="matches",
                consistency_level="Strong",
                timeout=60,
            )
            assert_results(result, rows, expected, output_fields)
        confirm_state(client, name, rows, sealed_ids)

    @pytest.mark.tags(CaseLabel.L1)
    def test_matching_boundaries_and_regex_filter(self, grep_client, grep_semantic_data):
        r"""TC-L1-04.
        target: Independent fields/lines and RE2 filters retain their own semantics.
        method: Hand-check Grep and =~ separately, then combine both conditions.
        expected: No record concatenation or cross-LF match; Unicode \w differs.
        """
        client, data = grep_client, grep_semantic_data
        name, rows, sealed_ids = data["name"], data["rows"], data["sealed_ids"]
        confirm_state(client, name, rows, sealed_ids)

        error_on_line_two = {403: [detail("ERROR disk", 2, 6, [(0, 5)])]}
        grep_cases = [
            ("-e 'abcdef'", "id in [401, 402]", {}),
            ("-e 'foo'", "id == 404", {404: [detail("foo", 1, 0, [(0, 3)])]}),
            ("-e 'bar'", "id == 404", {404: [detail("bar", 2, 4, [(0, 3)])]}),
            ("-e 'foo.*bar'", "id == 404", {}),
            ("-e '(?s)foo.*bar'", "id == 404", {}),
            (r"-e 'foo\sbar'", "id == 404", {}),
            ("-e 'error'", "id == 409", {409: [detail("error", 1, 0, [(0, 5)]), detail("error", 2, 6, [(0, 5)])]}),
            ("-e '^ERROR'", "id == 403", error_on_line_two),
            (r"-e '\w+'", "id == 405", {405: [detail("中文", 1, 0, [(0, 6)])]}),
            (r"-e '\berror\b'", "id == 406", {}),
            ("-e '(?i:a)b'", "id in [407, 408]", {407: [detail("Ab", 1, 0, [(0, 2)])]}),
        ]
        candidate_filter = "id in [403, 404, 410, 411]"
        regex_filter = f'{candidate_filter} and content =~ "^hello.*disk$"'
        if data["dtype"] == "VARCHAR":
            grep_cases += [
                (
                    "-e '^ERROR'",
                    candidate_filter,
                    {**error_on_line_two, 410: [detail("ERROR network", 1, 0, [(0, 5)])]},
                ),
                ("-e '^ERROR'", regex_filter, error_on_line_two),
            ]

        output_fields = ["id", "content", "repo", "path", "revision"]
        for args, expr, expected in grep_cases:
            log.info(f"Grep arguments={args!r}, filter={expr!r}")
            result = client.grep(
                collection_name=name,
                text_field="content",
                args=args,
                filter=expr,
                output_fields=output_fields,
                limit=100,
                offset=0,
                match_field="matches",
                consistency_level="Strong",
                timeout=60,
            )
            assert_results(result, rows, expected, output_fields)

        if data["dtype"] == "VARCHAR":
            regex_filter_cases = [
                ('id == 403 and content =~ "^ERROR"', set()),
                ('id == 404 and content =~ "foo.*bar"', {404}),
                (r'id == 405 and content =~ "\\w+"', set()),
                (r'id == 406 and content =~ "\\berror\\b"', {406}),
                ('id in [407, 408] and content =~ "(?i:a)b"', {407}),
                (regex_filter, {403, 411}),
            ]
            for expr, expected_ids in regex_filter_cases:
                log.info(f"Query filter={expr!r}")
                result = client.query(
                    collection_name=name,
                    filter=expr,
                    output_fields=["id"],
                    limit=100,
                    offset=0,
                    consistency_level="Strong",
                    timeout=60,
                )
                assert_results(result, rows, {pk: [] for pk in expected_ids}, ["id"], match_field=None)
        confirm_state(client, name, rows, sealed_ids)


class TestMilvusClientGrepIndependent(GrepBase):
    """When to add tests here: lifecycle, special data or per-case collection state.

    Fixture: grep_client and per-case collection cleanup.
    """

    @pytest.mark.tags(CaseLabel.L0)
    @pytest.mark.parametrize("state", ["growing", "sealed", "mixed"])
    def test_public_entries_in_data_states(self, grep_client, request, state):
        """TC-L0-01.
        target: Scan Grep works with growing, sealed and mixed visible data.
        method: Compare grep() and query(function_chains=...) against D0 truth.
        expected: Exact PK set, full fields and byte coordinates in all states.
        """
        client = grep_client
        name = f"grep_l0_01_{state}_{uuid4().hex[:12]}"

        def drop_test_collection():
            if client.has_collection(name, timeout=TIMEOUT):
                client.drop_collection(name, timeout=TIMEOUT)

        request.addfinalizer(drop_test_collection)
        schema = client.create_schema(auto_id=False, enable_dynamic_field=False)
        schema.add_field("id", DataType.INT64, is_primary=True)
        schema.add_field("vector", DataType.FLOAT_VECTOR, dim=4)
        schema.add_field("content", DataType.VARCHAR, max_length=65535, nullable=True)
        schema.add_field("repo", DataType.VARCHAR, max_length=256)
        schema.add_field("path", DataType.VARCHAR, max_length=256)
        schema.add_field("revision", DataType.VARCHAR, max_length=256)
        client.create_collection(name, schema=schema, shards_num=1, consistency_level="Strong", timeout=TIMEOUT)
        indexes = client.prepare_index_params()
        indexes.add_index("vector", index_type="FLAT", metric_type="L2")
        client.create_index(name, index_params=indexes, timeout=TIMEOUT)
        client.load_collection(name, timeout=TIMEOUT)

        rows = basic_rows()
        first_batch = rows[: len(rows) // 2] if state == "mixed" else rows
        client.insert(name, first_batch, timeout=TIMEOUT)
        sealed_ids = set()
        if state in ("sealed", "mixed"):
            client.flush(name, timeout=TIMEOUT)
            sealed_ids = {row["id"] for row in first_batch}
        if state == "mixed":
            confirm_state(client, name, first_batch, sealed_ids, wait=True)
            client.insert(name, rows[len(first_batch) :], timeout=TIMEOUT)
        confirm_state(client, name, rows, sealed_ids, wait=True)

        expected = expected_literal(rows, "error")
        assert set(expected) == {1, 3, 4, 5, 6, 11, 12}
        grep_rows = client.grep(
            name,
            text_field="content",
            args="-F -e error",
            filter="",
            output_fields=FIELDS,
            limit=100,
            offset=0,
            match_field="matches",
            consistency_level="Strong",
            timeout=TIMEOUT,
        )
        chain = FunctionChain(FunctionChainStage.QUERY_SCAN, name="grep", output_columns=["matches"])
        chain.grep("content", arguments="-F -e error", output="matches")
        query_rows = client.query(
            name,
            filter="",
            output_fields=FIELDS,
            limit=100,
            offset=0,
            function_chains=chain,
            consistency_level="Strong",
            timeout=TIMEOUT,
        )

        grep_by_id = assert_results(grep_rows, rows, expected, FIELDS)
        query_by_id = assert_results(query_rows, rows, expected, FIELDS)
        assert grep_by_id == query_by_id
        confirm_state(client, name, rows, sealed_ids)

    @pytest.mark.tags(CaseLabel.L0)
    @pytest.mark.parametrize("state", ["growing", "sealed"])
    def test_code_filter_and_filter_params(self, grep_client, request, state):
        """TC-L0-03.
        target: Repo/path filters and template parameters restrict Grep candidates.
        method: Use both entries with and without full-content projection.
        expected: Explicit/template filters yield {103}; no filter yields {101,102,103}.
        """
        client = grep_client
        name = f"grep_l0_03_{state}_{uuid4().hex[:12]}"

        def drop_test_collection():
            if client.has_collection(name, timeout=TIMEOUT):
                client.drop_collection(name, timeout=TIMEOUT)

        request.addfinalizer(drop_test_collection)
        schema = client.create_schema(auto_id=False, enable_dynamic_field=False)
        schema.add_field("id", DataType.INT64, is_primary=True)
        schema.add_field("vector", DataType.FLOAT_VECTOR, dim=4)
        schema.add_field("content", DataType.VARCHAR, max_length=65535, nullable=True)
        schema.add_field("repo", DataType.VARCHAR, max_length=256)
        schema.add_field("path", DataType.VARCHAR, max_length=256)
        schema.add_field("revision", DataType.VARCHAR, max_length=256)
        client.create_collection(name, schema=schema, shards_num=1, consistency_level="Strong", timeout=TIMEOUT)
        indexes = client.prepare_index_params()
        indexes.add_index("vector", index_type="FLAT", metric_type="L2")
        client.create_index(name, index_params=indexes, timeout=TIMEOUT)
        client.load_collection(name, timeout=TIMEOUT)

        rows = code_rows()
        client.insert(name, rows, timeout=TIMEOUT)
        sealed_ids = set()
        if state == "sealed":
            client.flush(name, timeout=TIMEOUT)
            sealed_ids = {row["id"] for row in rows}
        confirm_state(client, name, rows, sealed_ids, wait=True)

        chain = FunctionChain(FunctionChainStage.QUERY_SCAN, name="grep", output_columns=["matches"])
        chain.grep("content", arguments="-F -e 'legacy_call('", output="matches")
        for expr, filter_params, expected_ids in [
            ('repo == "demo" and path like "src/%"', {}, {103}),
            ("", {}, {101, 102, 103}),
            ('repo == {repo} and path like "src/%"', {"repo": "demo"}, {103}),
        ]:
            expected = expected_literal(
                rows,
                "legacy_call(",
                lambda row: not expr or (row["repo"] == "demo" and row["path"].startswith("src/")),
            )
            assert set(expected) == expected_ids
            metadata_fields = ["id", "repo", "path", "revision"]
            for fields in (metadata_fields, [*metadata_fields, "content"]):
                grep_rows = client.grep(
                    collection_name=name,
                    text_field="content",
                    args="-F -e 'legacy_call('",
                    filter=expr,
                    filter_params=filter_params,
                    output_fields=fields,
                    limit=100,
                    offset=0,
                    match_field="matches",
                    consistency_level="Strong",
                    timeout=TIMEOUT,
                )
                query_rows = client.query(
                    collection_name=name,
                    filter=expr,
                    filter_params=filter_params,
                    output_fields=fields,
                    limit=100,
                    offset=0,
                    function_chains=chain,
                    consistency_level="Strong",
                    timeout=TIMEOUT,
                )
                grep_by_id = assert_results(grep_rows, rows, expected, fields)
                query_by_id = assert_results(query_rows, rows, expected, fields)
                assert grep_by_id == query_by_id
        confirm_state(client, name, rows, sealed_ids)

    @pytest.mark.tags(CaseLabel.L0)
    @pytest.mark.parametrize("state", ["growing", "sealed"])
    def test_sparse_hits_and_pagination(self, grep_client, request, state):
        """TC-L0-04.
        target: Query pagination counts matches, not raw candidate records.
        method: Shuffle D1 and test all approved limit/offset pairs via both entries.
        expected: Exact page sizes and aligned rows; no assumed small-page ordering.
        """
        client = grep_client
        name = f"grep_l0_04_{state}_{uuid4().hex[:12]}"

        def drop_test_collection():
            if client.has_collection(name, timeout=TIMEOUT):
                client.drop_collection(name, timeout=TIMEOUT)

        request.addfinalizer(drop_test_collection)
        schema = client.create_schema(auto_id=False, enable_dynamic_field=False)
        schema.add_field("id", DataType.INT64, is_primary=True)
        schema.add_field("vector", DataType.FLOAT_VECTOR, dim=4)
        schema.add_field("content", DataType.VARCHAR, max_length=65535, nullable=True)
        schema.add_field("repo", DataType.VARCHAR, max_length=256)
        schema.add_field("path", DataType.VARCHAR, max_length=256)
        schema.add_field("revision", DataType.VARCHAR, max_length=256)
        client.create_collection(name, schema=schema, shards_num=1, consistency_level="Strong", timeout=TIMEOUT)
        indexes = client.prepare_index_params()
        indexes.add_index("vector", index_type="FLAT", metric_type="L2")
        client.create_index(name, index_params=indexes, timeout=TIMEOUT)
        client.load_collection(name, timeout=TIMEOUT)

        rows = sparse_rows()
        client.insert(name, rows, timeout=TIMEOUT)
        sealed_ids = set()
        if state == "sealed":
            client.flush(name, timeout=TIMEOUT)
            sealed_ids = {row["id"] for row in rows}
        confirm_state(client, name, rows, sealed_ids, wait=True)

        expected = expected_literal(rows, "error")
        assert set(expected) == {128, 255, 256, 383, 384}
        chain = FunctionChain(FunctionChainStage.QUERY_SCAN, name="grep", output_columns=["matches"])
        chain.grep("content", arguments="-F -e error", output="matches")
        for limit, offset, expected_count in [(1, 0, 1), (2, 1, 2), (3, 2, 3), (10, 0, 5), (1, 5, 0)]:
            grep_rows = client.grep(
                collection_name=name,
                text_field="content",
                args="-F -e error",
                filter="",
                output_fields=FIELDS,
                limit=limit,
                offset=offset,
                match_field="matches",
                consistency_level="Strong",
                timeout=TIMEOUT,
            )
            query_rows = client.query(
                collection_name=name,
                filter="",
                output_fields=FIELDS,
                limit=limit,
                offset=offset,
                function_chains=chain,
                consistency_level="Strong",
                timeout=TIMEOUT,
            )
            take_all = (limit, offset) == (10, 0)
            count = None if take_all else expected_count
            grep_by_id = assert_results(grep_rows, rows, expected, FIELDS, count=count)
            query_by_id = assert_results(query_rows, rows, expected, FIELDS, count=count)
            if take_all:
                assert grep_by_id == query_by_id
        confirm_state(client, name, rows, sealed_ids)


@pytest.mark.xdist_group("GrepShared")
class TestMilvusClientGrepProjectionSealedAndGrowing(GrepBase):
    """When to add tests here: projection and computed output alignment.

    Fixture: grep_projection_data, read-only complex data in both states.
    """

    @pytest.mark.tags(CaseLabel.L1)
    def test_output_fields_and_match_details(self, grep_client, grep_projection_data):
        """TC-L1-08.
        target: Projected fields and details belong to the same visible source row.
        method: Change Grep projections and check explicit chain detail export separately.
        expected: Full source content remains distinct from matching-line content.
        """
        client, data = grep_client, grep_projection_data
        name, rows, sealed_ids = data["name"], data["rows"], data["sealed_ids"]
        confirm_state(client, name, rows, sealed_ids)
        expected = {
            101: [detail("error", 2, 7, [(0, 5)])],
            202: [detail("中文 error", 1, 0, [(7, 12)])],
        }
        source_fields = ["id", "content", "repo", "path", "revision"]
        projections = [
            ["id"],
            ["id", "path", "revision", "score", "meta", "tags", "vector"],
            source_fields + ["score", "meta", "tags", "vector"],
        ]
        for fields in projections:
            for match_field in ("matches", "text_hits"):
                log.info(f"Grep projection fields={fields!r} match_field={match_field!r}")
                result = client.grep(
                    collection_name=name,
                    text_field="content",
                    args="-F -e error",
                    filter="",
                    output_fields=fields,
                    limit=100,
                    offset=0,
                    match_field=match_field,
                    consistency_level="Strong",
                    timeout=60,
                )
                assert_results(result, rows, expected, fields, match_field=match_field)
        for output_columns, match_field in [(["matches"], "matches"), ([], None)]:
            log.info(f"Query chain projection output_columns={output_columns!r}")
            chain = FunctionChain(FunctionChainStage.QUERY_SCAN, name="grep", output_columns=output_columns)
            chain.grep("content", arguments="-F -e error", output="matches")
            result = client.query(
                collection_name=name,
                filter="",
                output_fields=source_fields,
                limit=100,
                offset=0,
                function_chains=chain,
                consistency_level="Strong",
                timeout=60,
            )
            assert_results(result, rows, expected, source_fields, match_field=match_field)
        confirm_state(client, name, rows, sealed_ids)


class TestMilvusClientGrepLifecycleIndependent(GrepBase):
    """When to add tests here: empty data, mutations or lifecycle-specific fixtures.

    Fixture: grep_client and per-test collections registered with request finalizers.
    """

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.asyncio
    @pytest.mark.parametrize("scenario", ["empty", "null", "filtered", "no_match"])
    async def test_empty_results(self, grep_client, request, scenario):
        """TC-L1-09, empty-result branches.
        target: Legal empty results remain decoded lists through the public SDK.
        method: Check all scenarios with grep; async/query only empty/no-match owners.
        expected: No missing response, parsing exception or accidental source rows.
        """
        texts = {"null": None, "filtered": "error", "no_match": "plain text"}
        rows = [] if scenario == "empty" else [source_row(1, texts[scenario])]
        name = prepare_collection(grep_client, request, rows)["name"]
        expr = "id < 0" if scenario == "filtered" else ""
        ordinary = grep_client.query(
            collection_name=name,
            filter="",
            output_fields=FIELDS,
            limit=100,
            offset=0,
            consistency_level="Strong",
            timeout=60,
        )
        assert_results(ordinary, rows, {row["id"]: [] for row in rows}, FIELDS, match_field=None)
        if scenario == "filtered":
            result = grep_client.grep(
                collection_name=name,
                text_field="content",
                args="-F -e error",
                filter="",
                output_fields=FIELDS,
                limit=100,
                offset=0,
                match_field="matches",
                consistency_level="Strong",
                timeout=60,
            )
            assert_results(result, rows, {1: [detail("error", 1, 0, [(0, 5)])]}, FIELDS)
        for match_field in ("matches", "text_hits"):
            result = grep_client.grep(
                collection_name=name,
                text_field="content",
                args="-F -e error",
                filter=expr,
                output_fields=FIELDS,
                limit=100,
                offset=0,
                match_field=match_field,
                consistency_level="Strong",
                timeout=60,
            )
            assert list(result) == []
        if scenario in ("empty", "no_match"):
            chain = FunctionChain(FunctionChainStage.QUERY_SCAN, name="grep", output_columns=["matches"])
            chain.grep("content", arguments="-F -e error", output="matches")
            result = grep_client.query(
                collection_name=name,
                filter=expr,
                output_fields=FIELDS,
                limit=100,
                offset=0,
                function_chains=chain,
                consistency_level="Strong",
                timeout=60,
            )
            assert list(result) == []
            self.init_async_milvus_client()
            async_client = self.async_milvus_client_wrap.async_milvus_client
            try:
                for match_field in ("matches", "text_hits"):
                    result = await async_client.grep(
                        collection_name=name,
                        text_field="content",
                        args="-F -e error",
                        filter=expr,
                        output_fields=FIELDS,
                        limit=100,
                        offset=0,
                        match_field=match_field,
                        consistency_level="Strong",
                        timeout=60,
                    )
                    assert list(result) == []
            finally:
                await async_client.close()

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize("distribution", ["empty_shard", "nonmatching_shard", "all_nonmatching"])
    def test_empty_shard_results(self, grep_client, request, distribution):
        """TC-L1-09, actual shard distribution.
        target: Empty or nonmatching shards do not discard other shards' matches.
        method: Confirm populated insert channels from persistent segments after Flush.
        expected: One matching record survives; all-nonmatching data returns an empty list.
        """
        first_text = "plain text" if distribution == "all_nonmatching" else "error"
        rows = [source_row(1, first_text)]
        if distribution != "empty_shard":
            rows += [source_row(pk, "plain text") for pk in range(2, 1026)]
        name = prepare_collection(grep_client, request, rows, state="sealed", shards=2)["name"]
        assert grep_client.describe_collection(name, timeout=60)["num_shards"] == 2
        segments = grep_client.list_persistent_segments(name, timeout=60)
        channels = {s.insert_channel for s in segments if s.num_rows > 0}
        required = 1 if distribution == "empty_shard" else 2
        if len(channels) != required:
            pytest.skip(f"BLOCKED: need {required} populated shards, observed {len(channels)}")
        # With one matching entity and two populated channels, at least one
        # populated shard has no match. A one-entity collection leaves one empty.
        log.info(
            f"Grep shard evidence collection={name} channels={sorted(channels)} "
            f"segments={[(s.segment_id, s.insert_channel, s.num_rows) for s in segments]}"
        )
        expected = {} if distribution == "all_nonmatching" else {1: [detail("error", 1, 0, [(0, 5)])]}
        result = grep_client.grep(
            collection_name=name,
            text_field="content",
            args="-F -e error",
            filter="",
            output_fields=FIELDS,
            limit=100,
            offset=0,
            match_field="matches",
            consistency_level="Strong",
            timeout=60,
        )
        assert_results(result, rows, expected, FIELDS)

    @pytest.mark.tags(CaseLabel.L1)
    def test_upsert_delete_and_visible_version(self, grep_client, request):
        """TC-L1-11.
        target: Current source fields and match details replace old sealed versions.
        method: V1 -> V2 -> V3 -> delete, with an unchanged matching control record.
        expected: No old hit revival; V3 points to line two at byte offset seven.
        """
        client = grep_client
        name = f"grep_l1_11_{uuid4().hex[:12]}"

        def drop_test_collection():
            if client.has_collection(name, timeout=60):
                client.drop_collection(name, timeout=60)

        request.addfinalizer(drop_test_collection)
        schema = client.create_schema(auto_id=False, enable_dynamic_field=False)
        schema.add_field("id", DataType.INT64, is_primary=True)
        schema.add_field("vector", DataType.FLOAT_VECTOR, dim=4)
        schema.add_field("content", DataType.VARCHAR, max_length=65535, nullable=True)
        for field in ("repo", "path", "revision"):
            schema.add_field(field, DataType.VARCHAR, max_length=256)
        client.create_collection(name, schema=schema, shards_num=1, consistency_level="Strong", timeout=60)
        indexes = client.prepare_index_params()
        indexes.add_index("vector", index_type="FLAT", metric_type="L2")
        client.create_index(name, index_params=indexes, timeout=60)
        client.load_collection(name, replica_number=1, timeout=60)

        control = source_row(202, "error control")
        v1 = source_row(101, "error old", path="src/v1.py", revision="V1")
        v2 = source_row(101, "info only", path="src/v2.py", revision="V2")
        v3 = source_row(101, "header\n  error new", path="src/v3.py", revision="V3")
        control_hit = {202: [detail("error control", 1, 0, [(0, 5)])]}
        v1_hits = {101: [detail("error old", 1, 0, [(0, 5)])], **control_hit}
        stages = [
            ("V1", [v1, control], v1_hits),
            ("V1 sealed", [v1, control], v1_hits),
            ("V2", [v2, control], control_hit),
            ("V3", [v3, control], {101: [detail("  error new", 2, 7, [(2, 7)])], **control_hit}),
            ("deleted", [control], control_hit),
        ]

        v1_sealed_ids = set()
        missing_mixed_states = []
        for stage, visible_rows, expected_hits in stages:
            if stage == "V1":
                inserted = client.insert(collection_name=name, data=[v1, control], timeout=60)
                assert inserted["insert_count"] == 2
            elif stage == "V1 sealed":
                client.flush(collection_name=name, timeout=60)
                confirm_state(client, name, [v1, control], {101, 202}, wait=True)
                v1_sealed_ids = {
                    s.segment_id
                    for s in client.list_loaded_segments(collection_name=name, timeout=60)
                    if s.state == SegmentState.Sealed
                }
                assert v1_sealed_ids
            elif stage in ("V2", "V3"):
                replacement = v2 if stage == "V2" else v3
                updated = client.upsert(collection_name=name, data=[replacement], partial_update=False, timeout=60)
                assert updated["upsert_count"] == 1
            elif stage == "deleted":
                deleted = client.delete(collection_name=name, ids=[101], timeout=60)
                assert deleted["delete_count"] == 1

            ordinary = client.query(
                collection_name=name,
                filter="",
                output_fields=FIELDS,
                limit=100,
                consistency_level="Strong",
                timeout=60,
            )
            assert_results(ordinary, visible_rows, {row["id"]: [] for row in visible_rows}, FIELDS, match_field=None)
            hits = client.grep(
                collection_name=name,
                text_field="content",
                args="-F -e error",
                filter="",
                output_fields=FIELDS,
                limit=100,
                offset=0,
                match_field="matches",
                consistency_level="Strong",
                timeout=60,
            )
            assert_results(hits, visible_rows, expected_hits, FIELDS)

            if stage in ("V2", "V3"):
                sealed_rows = client.query(
                    collection_name=name,
                    filter="",
                    output_fields=FIELDS,
                    limit=100,
                    ignore_growing=True,
                    consistency_level="Strong",
                    timeout=60,
                )
                loaded_sealed_rows = {
                    s.segment_id: s.num_rows
                    for s in client.list_loaded_segments(collection_name=name, timeout=60)
                    if s.state == SegmentState.Sealed
                }
                replacements = {
                    s.segment_id: s.compaction_from
                    for s in client.list_persistent_segments(collection_name=name, timeout=60)
                    if s.compaction_from and set(s.compaction_from) <= v1_sealed_ids
                }
                v1_sealed_ids.update(replacements)
                # Sort compaction may replace the flushed segment before the upsert.
                old_rows_loaded = sum(rows for sid, rows in loaded_sealed_rows.items() if sid in v1_sealed_ids)
                sealed_pks = {row["id"] for row in sealed_rows}
                log.info(
                    f"Grep version evidence collection={name} stage={stage} sealed_pks={sorted(sealed_pks)} "
                    f"v1_sealed_segments={sorted(v1_sealed_ids)} replacements={replacements} "
                    f"loaded_sealed_rows={loaded_sealed_rows} old_rows_loaded={old_rows_loaded}"
                )
                expected_sealed_rows = visible_rows if 101 in sealed_pks else [control]
                assert_results(
                    sealed_rows,
                    expected_sealed_rows,
                    {row["id"]: [] for row in expected_sealed_rows},
                    FIELDS,
                    match_field=None,
                )
                if old_rows_loaded != 2 or 101 in sealed_pks:
                    missing_mixed_states.append(stage)

        if missing_mixed_states:
            pytest.skip(
                f"BLOCKED: functional assertions passed; old Sealed/new Growing state unavailable at {missing_mixed_states}"
            )

    @pytest.mark.tags(CaseLabel.L1)
    def test_long_text_positions_and_reload(self, grep_client, request):
        """TC-L1-21.
        target: Long TEXT retains complete source and hits beyond 64 KiB.
        method: Search literal markers before Flush, after sealed load, and after reload.
        expected: Exact full-text lengths/hashes and every first/middle/last byte span.
        """
        client = grep_client
        name = f"grep_l1_21_{uuid4().hex[:12]}"
        single = "GREP_HEAD " + "🙂中" * 10000 + " GREP_MIDDLE " + "x" * 70000 + " GREP_TAIL"
        multi = "GREP_HEAD\n" + ("中文🙂 ordinary line\n" * 40000) + "GREP_MIDDLE\n" + "x\n" * 10000 + "GREP_TAIL"
        rows = [
            source_row(1, single),
            source_row(2, multi),
            source_row(3, multi),
            source_row(4, None),
            source_row(5, ""),
        ]
        content_bytes = {row["id"]: row["content"].encode("utf-8") for row in rows if row["content"] is not None}
        fingerprints = {pk: (len(raw), hashlib.sha256(raw).hexdigest()) for pk, raw in content_bytes.items()}
        expected = {pattern: expected_literal(rows, pattern) for pattern in ("GREP_HEAD", "GREP_MIDDLE", "GREP_TAIL")}
        all_ids = {row["id"] for row in rows}

        schema = client.create_schema(auto_id=False, enable_dynamic_field=False)
        schema.add_field("id", DataType.INT64, is_primary=True)
        schema.add_field("vector", DataType.FLOAT_VECTOR, dim=4)
        schema.add_field("content", DataType.TEXT, nullable=True)
        for field in ("repo", "path", "revision"):
            schema.add_field(field, DataType.VARCHAR, max_length=256)
        client.create_collection(
            collection_name=name, schema=schema, shards_num=1, consistency_level="Strong", timeout=60
        )
        request.addfinalizer(lambda: client.drop_collection(collection_name=name, timeout=60))
        vector_indexes = client.prepare_index_params()
        vector_indexes.add_index(field_name="vector", index_type="FLAT", metric_type="L2")
        client.create_index(collection_name=name, index_params=vector_indexes, timeout=60)
        assert client.list_indexes(collection_name=name, field_name="content", timeout=60) == []
        client.load_collection(collection_name=name, replica_number=1, timeout=120)
        inserted = client.insert(collection_name=name, data=rows, timeout=60)
        assert inserted["insert_count"] == len(rows)
        log.info(f"Long TEXT source collection={name} fingerprints={fingerprints}")

        for stage in ("growing", "sealed", "reloaded"):
            if stage == "sealed":
                client.flush(collection_name=name, timeout=120)
            elif stage == "reloaded":
                client.release_collection(collection_name=name, timeout=60)
                deadline = time.monotonic() + 60
                while True:
                    load_state = client.get_load_state(collection_name=name, timeout=10)
                    if load_state["state"] == LoadState.NotLoad:
                        break
                    assert time.monotonic() < deadline, load_state
                    time.sleep(1)
                log.info(f"Long TEXT released collection={name} state={load_state}")
                client.load_collection(collection_name=name, replica_number=1, timeout=120)

            load_state = client.get_load_state(collection_name=name, timeout=60)
            assert load_state["state"] == LoadState.Loaded, load_state
            ordinary = client.query(
                collection_name=name,
                filter="",
                output_fields=FIELDS,
                limit=100,
                consistency_level="Strong",
                ignore_growing=False,
                timeout=60,
            )
            visible = assert_results(ordinary, rows, {pk: [] for pk in all_ids}, FIELDS, match_field=None)

            expected_sealed = set() if stage == "growing" else all_ids
            deadline = time.monotonic() + 60
            while True:
                sealed_only = client.query(
                    collection_name=name,
                    filter="",
                    output_fields=["id"],
                    limit=100,
                    consistency_level="Strong",
                    ignore_growing=True,
                    timeout=10,
                )
                sealed_ids = {row["id"] for row in sealed_only}
                if sealed_ids == expected_sealed:
                    break
                if time.monotonic() >= deadline:
                    pytest.skip(f"BLOCKED: {stage} expected Sealed IDs {expected_sealed}, got {sealed_ids}")
                time.sleep(0.5)
            log.info(
                f"Long TEXT visible collection={name} stage={stage} load={load_state} "
                f"ids={sorted(visible)} sealed_ids={sorted(sealed_ids)}"
            )

            for pattern, expected_hits in expected.items():
                actual = client.grep(
                    collection_name=name,
                    text_field="content",
                    args=f"-F -e {pattern}",
                    filter="",
                    output_fields=FIELDS,
                    limit=100,
                    offset=0,
                    match_field="matches",
                    consistency_level="Strong",
                    timeout=60,
                )
                found = assert_results(actual, rows, expected_hits, FIELDS)
                evidence = {}
                for pk, result in found.items():
                    returned = result["content"].encode("utf-8")
                    fingerprint = (len(returned), hashlib.sha256(returned).hexdigest())
                    assert fingerprint == fingerprints[pk], (stage, pattern, pk, fingerprint)
                    assert returned == content_bytes[pk], (stage, pattern, pk)
                    evidence[pk] = {
                        "path": result["path"],
                        "content_bytes": fingerprint[0],
                        "sha256": fingerprint[1],
                        "matches": [
                            {key: hit[key] for key in ("line_number", "line_start_offset", "starts", "ends")}
                            for hit in result["matches"]
                        ],
                    }
                log.info(f"Long TEXT Grep collection={name} stage={stage} pattern={pattern} hits={evidence}")

    @pytest.mark.tags(CaseLabel.L1)
    def test_manual_partition_filters(self, grep_client, request):
        """TC-L1-12, manual partitions.
        target: Partition scope, scalar predicates and Grep jointly select records.
        method: Run both public entries with all partitions loaded, only p_a loaded, and after reload.
        expected: Exact results and distinct server errors for missing and unloaded partitions.
        """
        client = grep_client
        name = f"grep_l1_12_manual_{uuid4().hex[:12]}"

        def drop_test_collection():
            if client.has_collection(name, timeout=60):
                client.drop_collection(name, timeout=60)

        request.addfinalizer(drop_test_collection)
        schema = client.create_schema(auto_id=False, enable_dynamic_field=False)
        schema.add_field("id", DataType.INT64, is_primary=True)
        schema.add_field("vector", DataType.FLOAT_VECTOR, dim=4)
        schema.add_field("content", DataType.VARCHAR, max_length=65535)
        for field in ("repo", "path", "revision"):
            schema.add_field(field, DataType.VARCHAR, max_length=256)
        client.create_collection(name, schema=schema, shards_num=1, consistency_level="Strong", timeout=60)

        partitions = {
            "_default": [source_row(100, "error", path="src/default.py")],
            "p_a": [
                source_row(101, "error", path="src/a.py"),
                source_row(102, "error", repo="other", path="src/a.py"),
                source_row(103, "error", path="docs/a.md"),
            ],
            "p_b": [source_row(201, "error", path="src/b.py"), source_row(202, "info", path="src/b.py")],
            "p_empty": [],
        }
        for partition, records in partitions.items():
            if partition != "_default":
                client.create_partition(collection_name=name, partition_name=partition, timeout=60)
            if records:
                inserted = client.insert(collection_name=name, data=records, partition_name=partition, timeout=60)
                assert inserted["insert_count"] == len(records)

        indexes = client.prepare_index_params()
        indexes.add_index("vector", index_type="FLAT", metric_type="L2")
        client.create_index(name, index_params=indexes, timeout=60)
        assert set(client.list_partitions(collection_name=name, timeout=60)) == set(partitions)
        all_rows = [row for records in partitions.values() for row in records]
        fixed_filter = 'repo == "demo" and path like "src/%"'
        # Each row lists the expected IDs from ordinary Query and then Grep.
        scope_matrix = [
            (["p_a"], {101}, {101}),
            (["p_b"], {201, 202}, {201}),
            (["p_a", "p_b"], {101, 201, 202}, {101, 201}),
            (["p_empty"], set(), set()),
            (["p_a", "p_empty"], {101}, {101}),
            (None, {100, 101, 201, 202}, {100, 101, 201}),
        ]
        chain = FunctionChain(FunctionChainStage.QUERY_SCAN, name="grep", output_columns=["matches"])
        chain.grep("content", arguments="-F -e error", output="matches")

        for phase in ("all", "only_p_a", "reloaded"):
            if phase == "only_p_a":
                client.release_collection(collection_name=name, timeout=60)
                client.load_partitions(collection_name=name, partition_names=["p_a"], timeout=60)
                assert client.get_load_state(name, partition_name="p_a", timeout=60)["state"] == LoadState.Loaded
                assert client.get_load_state(name, partition_name="p_b", timeout=60)["state"] == LoadState.NotLoad
                cases = [
                    (["p_b"], fixed_filter, {}, None, 201),
                    (["p_a", "p_b"], fixed_filter, {}, None, 201),
                    (["p_a"], fixed_filter, {}, {101}, None),
                ]
            else:
                client.load_partitions(collection_name=name, partition_names=list(partitions), timeout=60)
                assert client.get_load_state(name, partition_name="p_empty", timeout=60)["state"] == LoadState.Loaded
                cases = [(scope, fixed_filter, {}, grep_ids, None) for scope, _, grep_ids in scope_matrix]

            if phase == "all":
                baseline = client.query(
                    collection_name=name,
                    filter="",
                    output_fields=FIELDS,
                    limit=100,
                    consistency_level="Strong",
                    timeout=60,
                )
                assert_results(baseline, all_rows, {row["id"]: [] for row in all_rows}, FIELDS, match_field=None)
                for scope, query_ids, _ in scope_matrix:
                    ordinary = client.query(
                        collection_name=name,
                        filter=fixed_filter,
                        partition_names=scope,
                        output_fields=FIELDS,
                        limit=100,
                        consistency_level="Strong",
                        timeout=60,
                    )
                    assert_results(ordinary, all_rows, {pk: [] for pk in query_ids}, FIELDS, match_field=None)
                cases += [
                    (["p_a", "p_b"], 'repo == "other" or path like "src/%"', {}, {101, 102, 201}, None),
                    (["p_a", "p_b"], 'not (repo == "other") and path like "src/%"', {}, {101, 201}, None),
                    (["p_a", "p_b"], 'repo in ["demo", "other"] and id >= 102 and id < 202', {}, {102, 103, 201}, None),
                    (
                        ["p_a", "p_b"],
                        'repo == {repo} and path like "src/%" and id >= {lower}',
                        {"repo": "demo", "lower": 200},
                        {201},
                        None,
                    ),
                    (["p_missing"], fixed_filter, {}, None, 1100),
                    (["p_a", "p_missing"], fixed_filter, {}, None, 1100),
                    (["p_a"], fixed_filter, {}, {101}, None),
                ]

            for scope, expr, params, ids, error_code in cases:
                log.info(f"Grep partition case phase={phase} scope={scope} filter={expr!r} params={params}")
                with pytest.raises(MilvusException) if error_code else nullcontext() as grep_error:
                    grep_rows = client.grep(
                        collection_name=name,
                        text_field="content",
                        args="-F -e error",
                        filter=expr,
                        filter_params=params,
                        partition_names=scope,
                        output_fields=FIELDS,
                        limit=100,
                        offset=0,
                        match_field="matches",
                        consistency_level="Strong",
                        timeout=60,
                    )
                with pytest.raises(MilvusException) if error_code else nullcontext() as query_error:
                    chain_rows = client.query(
                        collection_name=name,
                        filter=expr,
                        filter_params=params,
                        partition_names=scope,
                        function_chains=chain,
                        output_fields=FIELDS,
                        limit=100,
                        offset=0,
                        consistency_level="Strong",
                        timeout=60,
                    )
                if error_code:
                    reason = "partition name p_missing not found" if error_code == 1100 else "partition not loaded"
                    for caught in (grep_error, query_error):
                        assert not isinstance(caught.value, ParamError), caught.value
                        assert caught.value.code == error_code, caught.value
                        assert reason in caught.value.message.lower(), caught.value
                    continue

                expected = expected_literal(all_rows, "error", lambda row: row["id"] in ids)
                assert set(expected) == ids
                assert assert_results(grep_rows, all_rows, expected, FIELDS) == assert_results(
                    chain_rows, all_rows, expected, FIELDS
                )

    @pytest.mark.tags(CaseLabel.L1)
    def test_partition_key_filters(self, grep_client, request):
        """TC-L1-12, Partition Key with tenant/repo/path scopes."""
        client = grep_client
        name = f"grep_l1_12_tenant_{uuid4().hex[:12]}"

        def drop_test_collection():
            if client.has_collection(name, timeout=60):
                client.drop_collection(name, timeout=60)

        request.addfinalizer(drop_test_collection)
        schema = client.create_schema(auto_id=False, enable_dynamic_field=False)
        schema.add_field("id", DataType.INT64, is_primary=True)
        schema.add_field("vector", DataType.FLOAT_VECTOR, dim=4)
        schema.add_field("content", DataType.VARCHAR, max_length=65535)
        for field in ("repo", "path", "revision"):
            schema.add_field(field, DataType.VARCHAR, max_length=256)
        schema.add_field("tenant", DataType.VARCHAR, max_length=32, is_partition_key=True)
        client.create_collection(
            name,
            schema=schema,
            shards_num=1,
            num_partitions=8,
            consistency_level="Strong",
            timeout=60,
        )
        rows = [
            dict(source_row(1, "error a", path="src/a.py"), tenant="A"),
            dict(source_row(2, "error b", path="src/b.py"), tenant="B"),
            dict(source_row(3, "plain", path="src/clean.py"), tenant="A"),
            dict(source_row(4, "error docs", path="docs/a.md"), tenant="A"),
            dict(source_row(5, "error other", repo="other", path="src/a.py"), tenant="A"),
        ]
        assert client.insert(collection_name=name, data=rows, timeout=60)["insert_count"] == len(rows)
        indexes = client.prepare_index_params()
        indexes.add_index("vector", index_type="FLAT", metric_type="L2")
        client.create_index(name, index_params=indexes, timeout=60)
        client.load_collection(collection_name=name, replica_number=1, timeout=60)
        fields = [*FIELDS, "tenant"]
        chain = FunctionChain(FunctionChainStage.QUERY_SCAN, name="grep", output_columns=["matches"])
        chain.grep("content", arguments="-F -e error", output="matches")
        for expr, query_ids, grep_ids in [
            ('tenant == "A"', {1, 3, 4, 5}, {1, 4, 5}),
            ('tenant in ["A", "B"]', {1, 2, 3, 4, 5}, {1, 2, 4, 5}),
            ('tenant == "missing"', set(), set()),
            ('tenant == "A" and repo == "demo" and path like "src/%"', {1, 3}, {1}),
        ]:
            ordinary = client.query(
                collection_name=name,
                filter=expr,
                output_fields=fields,
                limit=100,
                consistency_level="Strong",
                timeout=60,
            )
            assert_results(ordinary, rows, {pk: [] for pk in query_ids}, fields, match_field=None)
            grep_rows = client.grep(
                collection_name=name,
                text_field="content",
                args="-F -e error",
                filter=expr,
                output_fields=fields,
                limit=100,
                offset=0,
                match_field="matches",
                consistency_level="Strong",
                timeout=60,
            )
            chain_rows = client.query(
                collection_name=name,
                filter=expr,
                function_chains=chain,
                output_fields=fields,
                limit=100,
                offset=0,
                consistency_level="Strong",
                timeout=60,
            )
            expected = expected_literal(rows, "error", lambda row: row["id"] in grep_ids)
            assert set(expected) == grep_ids
            assert assert_results(grep_rows, rows, expected, fields) == assert_results(
                chain_rows, rows, expected, fields
            )

    @pytest.mark.tags(CaseLabel.L1)
    def test_database_isolation(self, grep_client, request):
        """TC-L1-12, switch databases with the same collection name and primary keys."""
        client = grep_client
        databases = [f"grep_l1_12_db_{uuid4().hex[:12]}" for _ in range(2)]
        name = f"grep_l1_12_same_{uuid4().hex[:12]}"

        def cleanup_databases():
            admin = new_client()
            try:
                existing = admin.list_databases(timeout=60)
                for db in databases:
                    if db in existing:
                        admin.use_database(db, timeout=60)
                        if admin.has_collection(name, timeout=60):
                            admin.drop_collection(name, timeout=60)
                        admin.use_database("default", timeout=60)
                        admin.drop_database(db, timeout=60)
            finally:
                admin.close()

        request.addfinalizer(cleanup_databases)
        schema = client.create_schema(auto_id=False, enable_dynamic_field=False)
        schema.add_field("id", DataType.INT64, is_primary=True)
        schema.add_field("vector", DataType.FLOAT_VECTOR, dim=4)
        schema.add_field("content", DataType.VARCHAR, max_length=65535)
        for field in ("repo", "path", "revision"):
            schema.add_field(field, DataType.VARCHAR, max_length=256)
        rows_by_database = [
            [
                source_row(1, "db_0\nerror 0", path="src/db0.py", revision="database_0"),
                source_row(2, "plain"),
            ],
            [
                source_row(1, "中文 error 1", path="src/db1.py", revision="database_1"),
                source_row(2, "error second"),
            ],
        ]
        for db, rows in zip(databases, rows_by_database, strict=True):
            client.create_database(db_name=db, timeout=60)
            client.use_database(db_name=db, timeout=60)
            client.create_collection(name, schema=schema, shards_num=1, consistency_level="Strong", timeout=60)
            assert client.insert(collection_name=name, data=rows, timeout=60)["insert_count"] == 2
            indexes = client.prepare_index_params()
            indexes.add_index("vector", index_type="FLAT", metric_type="L2")
            client.create_index(name, index_params=indexes, timeout=60)
            client.load_collection(collection_name=name, replica_number=1, timeout=60)

        chain = FunctionChain(FunctionChainStage.QUERY_SCAN, name="grep", output_columns=["matches"])
        chain.grep("content", arguments="-F -e error", output="matches")
        for idx, ids in [(0, {1}), (1, {1, 2}), (0, {1})]:
            client.use_database(db_name=databases[idx], timeout=60)
            rows = rows_by_database[idx]
            ordinary = client.query(
                collection_name=name,
                filter="",
                output_fields=FIELDS,
                limit=100,
                consistency_level="Strong",
                timeout=60,
            )
            assert_results(ordinary, rows, {1: [], 2: []}, FIELDS, match_field=None)
            grep_rows = client.grep(
                collection_name=name,
                text_field="content",
                args="-F -e error",
                filter="",
                output_fields=FIELDS,
                limit=100,
                offset=0,
                match_field="matches",
                consistency_level="Strong",
                timeout=60,
            )
            chain_rows = client.query(
                collection_name=name,
                filter="",
                function_chains=chain,
                output_fields=FIELDS,
                limit=100,
                offset=0,
                consistency_level="Strong",
                timeout=60,
            )
            expected = expected_literal(rows, "error")
            assert set(expected) == ids
            assert assert_results(grep_rows, rows, expected, FIELDS) == assert_results(
                chain_rows, rows, expected, FIELDS
            )


class TestMilvusClientGrepNgramSealedOnly(GrepBase):
    """Sealed collections with and without an explicit text NGRAM index."""

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize("dtype", [DataType.VARCHAR, DataType.TEXT], ids=["VARCHAR", "TEXT"])
    @pytest.mark.parametrize("row_count", [1023, 2048], ids=["below-1024", "above-1024"])
    def test_grep_results_with_and_without_ngram(self, grep_client, request, dtype, row_count):
        """TC-L1-14. Check both collections against fixed row and byte-position expectations."""
        client = grep_client
        prefix = f"grep_l1_14_{dtype.name.lower()}_{uuid4().hex[:12]}"
        raw_name, indexed_name = f"{prefix}_raw", f"{prefix}_ngram"
        names = (raw_name, indexed_name)

        def drop_test_collections():
            try:
                client.drop_collection(collection_name=raw_name, timeout=60)
            finally:
                client.drop_collection(collection_name=indexed_name, timeout=60)

        request.addfinalizer(drop_test_collections)
        special_texts = {
            1001: "foo123bar",
            1002: "bar123foo",
            1003: "foo\nbar",
            1004: "中文\nfoo42bar\nfoo999bar",
            1005: "FOO123BAR",
            1006: "foobaz",
            1007: "xx foo7bar yy",
            1008: "literal.a.b",
            1009: "foofoo",
            1010: "quux99",
            1011: "literalXaYb",
            1012: "foobar",
            1013: "foo123BAR",
        }
        rows = [source_row(pk, special_texts.get(pk, f"ordinary row {pk}")) for pk in range(1, row_count + 1)]
        sealed_ids = {row["id"] for row in rows}
        schema = client.create_schema(auto_id=False, enable_dynamic_field=False)
        schema.add_field("id", DataType.INT64, is_primary=True)
        schema.add_field("vector", DataType.FLOAT_VECTOR, dim=4)
        if dtype == DataType.VARCHAR:
            schema.add_field("content", DataType.VARCHAR, max_length=65535, nullable=True)
        else:
            schema.add_field("content", DataType.TEXT, nullable=True)
        for field in ("repo", "path", "revision"):
            schema.add_field(field, DataType.VARCHAR, max_length=256)

        for name in names:
            client.create_collection(
                collection_name=name, schema=schema, shards_num=1, consistency_level="Strong", timeout=60
            )
            inserted = client.insert(collection_name=name, data=rows, timeout=60)
            assert inserted["insert_count"] == len(rows)
            client.flush(collection_name=name, timeout=60)

            indexes = client.prepare_index_params()
            indexes.add_index(field_name="vector", index_type="FLAT", metric_type="L2")
            if name == indexed_name:
                indexes.add_index(
                    field_name="content",
                    index_type="NGRAM",
                    index_name="content_ngram",
                    params={"min_gram": 3, "max_gram": 4},
                )
            client.create_index(collection_name=name, index_params=indexes, timeout=180)
            client.load_collection(collection_name=name, replica_number=1, timeout=180)
            confirm_state(client, name, rows, sealed_ids, wait=True)

        assert client.list_indexes(collection_name=raw_name, field_name="content", timeout=60) == []
        assert client.list_indexes(collection_name=indexed_name, field_name="content", timeout=60) == ["content_ngram"]
        sealed_segments = {
            segment.segment_id: segment
            for segment in client.list_loaded_segments(collection_name=indexed_name, timeout=60)
            if segment.state == SegmentState.Sealed
        }
        sealed_sizes = {segment_id: segment.num_rows for segment_id, segment in sealed_segments.items()}
        assert sum(sealed_sizes.values()) == row_count, sealed_sizes
        # Index progress also counts segments below the default minimum build size.
        if row_count > 1024 and min(sealed_sizes.values()) <= 1024:
            pytest.skip(f"BLOCKED: expected loaded Sealed segments above 1024 rows: {sealed_sizes}")

        deadline = time.monotonic() + 120
        while True:
            info = client.describe_index(collection_name=indexed_name, index_name="content_ngram", timeout=60)
            assert info is not None and info["field_name"] == "content" and info["index_type"] == "NGRAM", info
            assert info["state"] != "Failed", info
            if (
                info["state"] == "Finished"
                and info["total_rows"] == info["indexed_rows"] == len(rows)
                and info["pending_index_rows"] == 0
            ):
                break
            assert time.monotonic() < deadline, info
            time.sleep(1)
        collection_id = next(iter(sealed_segments.values())).collection_id
        log.info(
            f"Grep NGRAM collection={indexed_name} collection_id={collection_id} sealed_sizes={sealed_sizes} index={info}"
        )

        foo123bar = detail("foo123bar", 1, 0, [(0, 9)])
        foo42bar = detail("foo42bar", 2, 7, [(0, 8)])
        foo999bar = detail("foo999bar", 3, 16, [(0, 9)])
        foo7bar = detail("xx foo7bar yy", 1, 0, [(3, 10)])
        foobar = detail("foobar", 1, 0, [(0, 6)])
        cases = [
            ('-e "foo.*bar"', {1001: [foo123bar], 1004: [foo42bar, foo999bar], 1007: [foo7bar], 1012: [foobar]}),
            ("-F -e literal.a.b", {1008: [detail("literal.a.b", 1, 0, [(0, 11)])]}),
            (
                '-e "foobaz|quux99"',
                {1006: [detail("foobaz", 1, 0, [(0, 6)])], 1010: [detail("quux99", 1, 0, [(0, 6)])]},
            ),
            ('-e "foo(123|42)bar"', {1001: [foo123bar], 1004: [foo42bar]}),
            ('-e "foo(123)?bar"', {1001: [foo123bar], 1012: [foobar]}),
            ('-e "(foo){2}"', {1009: [detail("foofoo", 1, 0, [(0, 6)])]}),
            ('-e "foo[47][0-9]?bar"', {1004: [foo42bar], 1007: [foo7bar]}),
            (
                "-i -e foo123",
                {
                    1001: [detail("foo123bar", 1, 0, [(0, 6)])],
                    1005: [detail("FOO123BAR", 1, 0, [(0, 6)])],
                    1013: [detail("foo123BAR", 1, 0, [(0, 6)])],
                },
            ),
            (
                '-e "(?i:foo)123BAR"',
                {
                    1005: [detail("FOO123BAR", 1, 0, [(0, 9)])],
                    1013: [detail("foo123BAR", 1, 0, [(0, 9)])],
                },
            ),
        ]
        for args, expected in cases:
            results = {}
            for name in names:
                log.info(f"Grep NGRAM query collection={name} args={args!r}")
                actual = client.grep(
                    collection_name=name,
                    text_field="content",
                    args=args,
                    filter="",
                    output_fields=FIELDS,
                    limit=2048,
                    offset=0,
                    match_field="matches",
                    consistency_level="Strong",
                    timeout=60,
                )
                log.info(f"Grep NGRAM response collection={name} args={args!r} rows={actual!r}")
                results[name] = assert_results(actual, rows, expected, FIELDS)
            assert results[raw_name] == results[indexed_name], args

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize("dtype", [DataType.VARCHAR, DataType.TEXT], ids=["VARCHAR", "TEXT"])
    def test_grep_short_and_broad_patterns_with_ngram(self, grep_client, request, dtype):
        """TC-L1-15. Keep matches from short, optional, and empty branches with a built NGRAM index."""
        client = grep_client
        prefix = f"grep_l1_15_{dtype.name.lower()}_{uuid4().hex[:12]}"
        raw_name, indexed_name = f"{prefix}_raw", f"{prefix}_ngram"
        names = (raw_name, indexed_name)

        def drop_test_collections():
            try:
                client.drop_collection(collection_name=raw_name, timeout=60)
            finally:
                client.drop_collection(collection_name=indexed_name, timeout=60)

        request.addfinalizer(drop_test_collections)
        texts = {
            301: "foo",
            302: "x",
            303: "bar",
            304: "7",
            305: "foobar",
            306: "fo",
            307: "fooo",
            308: "f",
            309: "hello",
            310: "",
            311: None,
            312: "\n",
        }
        rows = [source_row(pk, texts.get(pk, "")) for pk in range(1, 2049)]
        sealed_ids = {row["id"] for row in rows}
        schema = client.create_schema(auto_id=False, enable_dynamic_field=False)
        schema.add_field("id", DataType.INT64, is_primary=True)
        schema.add_field("vector", DataType.FLOAT_VECTOR, dim=4)
        if dtype == DataType.VARCHAR:
            schema.add_field("content", DataType.VARCHAR, max_length=65535, nullable=True)
        else:
            schema.add_field("content", DataType.TEXT, nullable=True)
        for field in ("repo", "path", "revision"):
            schema.add_field(field, DataType.VARCHAR, max_length=256)

        for name in names:
            client.create_collection(
                collection_name=name, schema=schema, shards_num=1, consistency_level="Strong", timeout=60
            )
            inserted = client.insert(collection_name=name, data=rows, timeout=60)
            assert inserted["insert_count"] == len(rows)
            client.flush(collection_name=name, timeout=60)
            indexes = client.prepare_index_params()
            indexes.add_index(field_name="vector", index_type="FLAT", metric_type="L2")
            if name == indexed_name:
                indexes.add_index(
                    field_name="content",
                    index_type="NGRAM",
                    index_name="content_ngram",
                    params={"min_gram": 3, "max_gram": 3},
                )
            client.create_index(collection_name=name, index_params=indexes, timeout=180)
            client.load_collection(collection_name=name, replica_number=1, timeout=180)
            confirm_state(client, name, rows, sealed_ids, wait=True)

        assert client.list_indexes(collection_name=raw_name, field_name="content", timeout=60) == []
        assert client.list_indexes(collection_name=indexed_name, field_name="content", timeout=60) == ["content_ngram"]
        segments = {
            segment.segment_id: segment
            for segment in client.list_loaded_segments(collection_name=indexed_name, timeout=60)
            if segment.state == SegmentState.Sealed
        }
        sealed_sizes = {segment_id: segment.num_rows for segment_id, segment in segments.items()}
        assert sum(sealed_sizes.values()) == len(rows), sealed_sizes
        if min(sealed_sizes.values()) <= 1024:
            pytest.skip(f"BLOCKED: expected loaded Sealed segments above 1024 rows: {sealed_sizes}")

        deadline = time.monotonic() + 120
        while True:
            info = client.describe_index(collection_name=indexed_name, index_name="content_ngram", timeout=60)
            assert info is not None and info["field_name"] == "content" and info["index_type"] == "NGRAM", info
            assert info["min_gram"] == info["max_gram"] == "3", info
            assert info["state"] != "Failed", info
            if (
                info["state"] == "Finished"
                and info["total_rows"] == info["indexed_rows"] == len(rows)
                and info["pending_index_rows"] == 0
            ):
                break
            assert time.monotonic() < deadline, info
            time.sleep(1)
        collection_id = next(iter(segments.values())).collection_id
        log.info(
            f"Grep NGRAM collection={indexed_name} collection_id={collection_id} sealed_sizes={sealed_sizes} index={info}"
        )

        foo = {
            301: [detail("foo", 1, 0, [(0, 3)])],
            305: [detail("foobar", 1, 0, [(0, 3)])],
            307: [detail("fooo", 1, 0, [(0, 3)])],
        }
        foobar = [detail("foobar", 1, 0, [(0, 6)])]
        blank_line = [detail("", 1, 0, [(0, 0)])]
        broad = {pk: [detail(texts[pk], 1, 0, [(0, len(texts[pk]))])] for pk in (301, 302, 303, 304, 306, 308, 309)}
        broad.update(
            {
                305: [detail("foobar", 1, 0, [(0, 3), (3, 6)])],
                307: [detail("fooo", 1, 0, [(0, 3), (3, 4)])],
                312: blank_line,
            }
        )
        with_empty = expected_literal(rows, "")
        with_empty.update(
            {
                301: foo[301],
                305: [detail("foobar", 1, 0, [(0, 3), (4, 4), (5, 5), (6, 6)])],
                307: [detail("fooo", 1, 0, [(0, 3), (4, 4)])],
            }
        )
        cases = [
            ('-e "foo|x"', {**foo, 302: [detail("x", 1, 0, [(0, 1)])]}),
            ('-e "foo|.*"', broad),
            ('-e "foo|[0-9]+"', {**foo, 304: [detail("7", 1, 0, [(0, 1)])]}),
            ('-e "foo(bar)?"', {**foo, 305: foobar}),
            ('-e "(foo)?bar"', {303: [detail("bar", 1, 0, [(0, 3)])], 305: foobar}),
            ('-e "foo*"', {**foo, 306: [detail("fo", 1, 0, [(0, 2)])], 307: [detail("fooo", 1, 0, [(0, 4)])]}),
            ('-e foo -e ""', with_empty),
        ]
        for args, expected in cases:
            results = {}
            for name in names:
                log.info(f"Grep NGRAM query collection={name} args={args!r}")
                actual = client.grep(
                    collection_name=name,
                    text_field="content",
                    args=args,
                    filter="",
                    output_fields=FIELDS,
                    limit=2048,
                    offset=0,
                    match_field="matches",
                    consistency_level="Strong",
                    timeout=60,
                )
                log.info(f"Grep NGRAM response collection={name} args={args!r} rows={actual!r}")
                results[name] = assert_results(actual, rows, expected, FIELDS)
            assert results[raw_name] == results[indexed_name], args

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize("dtype", [DataType.VARCHAR, DataType.TEXT], ids=["VARCHAR", "TEXT"])
    @pytest.mark.parametrize("gram_size", [1, 3], ids=["gram-1", "gram-3"])
    def test_grep_case_unicode_and_boundaries_with_ngram(self, grep_client, request, dtype, gram_size):
        """TC-L1-16. Check original text and UTF-8 spans for case and boundary matches."""
        client = grep_client
        prefix = f"grep_l1_16_{dtype.name.lower()}_g{gram_size}_{uuid4().hex[:12]}"
        raw_name, indexed_name = f"{prefix}_raw", f"{prefix}_ngram"
        names = (raw_name, indexed_name)

        def drop_test_collections():
            try:
                client.drop_collection(collection_name=raw_name, timeout=60)
            finally:
                client.drop_collection(collection_name=indexed_name, timeout=60)

        request.addfinalizer(drop_test_collections)
        texts = {
            1901: "error",
            1902: "ERROR",
            1903: "terror",
            1904: "error timeout",
            1905: "前error后",
            1906: "头🙂\n前 error 后",
            1907: "FOObar",
            1908: "FOOBAR",
            1909: "前中文🙂后",
            1910: "中文🙃",
            1911: "中🙂文",
            1912: "foo123bar",
            1913: "bar123foo",
            1914: "foo\nbar",
            1915: "error\nerror timeout",
            1951: "école",
            1952: "ÉCOLE",
            1953: "σ",
            1954: "ς",
            1955: "Σ",
            1956: "ж",
            1957: "Ж",
            1958: "错误 école ж",
            1959: "STRASSE",
            1960: "Straße",
            1961: "e\u0301",
            1962: "É é",
        }
        rows = [source_row(pk, texts.get(pk, "ordinary filler")) for pk in range(1, 2049)]
        sealed_ids = {row["id"] for row in rows}
        schema = client.create_schema(auto_id=False, enable_dynamic_field=False)
        schema.add_field("id", DataType.INT64, is_primary=True)
        schema.add_field("vector", DataType.FLOAT_VECTOR, dim=4)
        if dtype == DataType.VARCHAR:
            schema.add_field("content", DataType.VARCHAR, max_length=65535)
        else:
            schema.add_field("content", DataType.TEXT)
        for field in ("repo", "path", "revision"):
            schema.add_field(field, DataType.VARCHAR, max_length=256)

        for name in names:
            client.create_collection(
                collection_name=name, schema=schema, shards_num=1, consistency_level="Strong", timeout=60
            )
            inserted = client.insert(collection_name=name, data=rows, timeout=60)
            assert inserted["insert_count"] == len(rows)
            client.flush(collection_name=name, timeout=60)
            indexes = client.prepare_index_params()
            indexes.add_index(field_name="vector", index_type="FLAT", metric_type="L2")
            if name == indexed_name:
                indexes.add_index(
                    field_name="content",
                    index_type="NGRAM",
                    index_name="content_ngram",
                    params={"min_gram": gram_size, "max_gram": gram_size},
                )
            client.create_index(collection_name=name, index_params=indexes, timeout=180)
            client.load_collection(collection_name=name, replica_number=1, timeout=180)
            confirm_state(client, name, rows, sealed_ids, wait=True)

        assert client.list_indexes(collection_name=raw_name, field_name="content", timeout=60) == []
        assert client.list_indexes(collection_name=indexed_name, field_name="content", timeout=60) == ["content_ngram"]
        segments = {
            segment.segment_id: segment
            for segment in client.list_loaded_segments(collection_name=indexed_name, timeout=60)
            if segment.state == SegmentState.Sealed
        }
        sealed_sizes = {segment_id: segment.num_rows for segment_id, segment in segments.items()}
        assert sum(sealed_sizes.values()) == len(rows), sealed_sizes
        if min(sealed_sizes.values()) <= 1024:
            pytest.skip(f"BLOCKED: expected loaded Sealed segments above 1024 rows: {sealed_sizes}")

        deadline = time.monotonic() + 120
        while True:
            info = client.describe_index(collection_name=indexed_name, index_name="content_ngram", timeout=60)
            assert info is not None and info["field_name"] == "content" and info["index_type"] == "NGRAM", info
            assert info["min_gram"] == info["max_gram"] == str(gram_size), info
            assert info["state"] != "Failed", info
            if (
                info["state"] == "Finished"
                and info["total_rows"] == info["indexed_rows"] == len(rows)
                and info["pending_index_rows"] == 0
            ):
                break
            assert time.monotonic() < deadline, info
            time.sleep(1)
        collection_id = next(iter(segments.values())).collection_id
        log.info(
            f"Grep NGRAM collection={indexed_name} collection_id={collection_id} sealed_sizes={sealed_sizes} index={info}"
        )

        accent = {
            1951: [detail("école", 1, 0, [(0, 2)])],
            1958: [detail("错误 école ж", 1, 0, [(7, 9)])],
            1962: [detail("É é", 1, 0, [(3, 5)])],
        }
        whole_word = {
            1901: [detail("error", 1, 0, [(0, 5)])],
            1904: [detail("error timeout", 1, 0, [(0, 5)])],
            1906: [detail("前 error 后", 2, 8, [(4, 9)])],
            1915: [detail("error", 1, 0, [(0, 5)]), detail("error timeout", 2, 6, [(0, 5)])],
        }
        sensitive_error = {
            **whole_word,
            1903: [detail("terror", 1, 0, [(1, 6)])],
            1905: [detail("前error后", 1, 0, [(3, 8)])],
        }
        cases = {
            1: [
                ("-s -e é", accent),
                (
                    "-i -e é",
                    {
                        **accent,
                        1952: [detail("ÉCOLE", 1, 0, [(0, 2)])],
                        1962: [detail("É é", 1, 0, [(0, 2), (3, 5)])],
                    },
                ),
                ("-s -e Σ", {1955: [detail("Σ", 1, 0, [(0, 2)])]}),
                (
                    "-i -e Σ",
                    {pk: [detail(texts[pk], 1, 0, [(0, 2)])] for pk in (1953, 1954, 1955)},
                ),
                ("-F -s -e Ж", {1957: [detail("Ж", 1, 0, [(0, 2)])]}),
                (
                    "-F -i -e Ж",
                    {
                        1956: [detail("ж", 1, 0, [(0, 2)])],
                        1957: [detail("Ж", 1, 0, [(0, 2)])],
                        1958: [detail("错误 école ж", 1, 0, [(14, 16)])],
                    },
                ),
            ],
            3: [
                ("-s -e error", sensitive_error),
                ("-i -e error", {**sensitive_error, 1902: [detail("ERROR", 1, 0, [(0, 5)])]}),
                (
                    "-i -e ÉCOLE",
                    {
                        1951: [detail("école", 1, 0, [(0, 6)])],
                        1952: [detail("ÉCOLE", 1, 0, [(0, 6)])],
                        1958: [detail("错误 école ж", 1, 0, [(7, 13)])],
                    },
                ),
                ('-F -i -e "错误 ÉCOLE Ж"', {1958: [detail("错误 école ж", 1, 0, [(0, 16)])]}),
                ("-i -e Straße", {1960: [detail("Straße", 1, 0, [(0, 7)])]}),
                ('-e "(?i:foo)bar"', {1907: [detail("FOObar", 1, 0, [(0, 6)])]}),
                ("-F -e 中文🙂", {1909: [detail("前中文🙂后", 1, 0, [(3, 13)])]}),
                ('-e "foo.*bar"', {1912: [detail("foo123bar", 1, 0, [(0, 9)])]}),
                ("-w -e error", whole_word),
                ("-x -e error", {1901: whole_word[1901], 1915: [detail("error", 1, 0, [(0, 5)])]}),
            ],
        }
        for args, expected in cases[gram_size]:
            results = {}
            for name in names:
                log.info(f"Grep NGRAM query collection={name} args={args!r}")
                actual = client.grep(
                    collection_name=name,
                    text_field="content",
                    args=args,
                    filter="",
                    output_fields=FIELDS,
                    limit=2048,
                    offset=0,
                    match_field="matches",
                    consistency_level="Strong",
                    timeout=60,
                )
                log.info(f"Grep NGRAM response collection={name} args={args!r} rows={actual!r}")
                results[name] = assert_results(actual, rows, expected, FIELDS)
            assert results[raw_name] == results[indexed_name], args

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize("dtype", [DataType.VARCHAR, DataType.TEXT], ids=["VARCHAR", "TEXT"])
    def test_grep_complex_regex_with_and_without_ngram(self, grep_client, request, dtype):
        """TC-L2-02. Compare complex regex details against fixed truth with and without NGRAM."""
        client = grep_client
        prefix = f"grep_l2_02_{dtype.name.lower()}_{uuid4().hex[:12]}"
        raw_name, indexed_name = f"{prefix}_raw", f"{prefix}_ngram"
        names = (raw_name, indexed_name)

        def drop_test_collections():
            try:
                client.drop_collection(collection_name=raw_name, timeout=60)
            finally:
                client.drop_collection(collection_name=indexed_name, timeout=60)

        request.addfinalizer(drop_test_collections)
        texts = {
            101: "foo-timeout",
            102: "foo-error",
            103: "bar-timeout",
            104: "bar-error",
            105: "timeout error",
            106: "foo-bar",
            107: "error-foo",
            108: "foo\nerror",
            201: "prefixfoosuffix",
            202: "prefixbarsuffix",
            203: "prefixsuffix",
            204: "prefixbazsuffix",
            301: "中文🙂 FOO-timeout",
            302: "标题\nBAR-错误",
            303: "FOO-TIMEOUT",
            401: "header\nbar-error\nprefixsuffix",
            402: "foo-error prefixsuffix",
            501: "auth-read-transaction-status-failed",
            502: "cache-write-retry-transaction-status-timeout",
            503: "storage-open-transaction-status-timeout",
            504: "storage-close-retry-transaction-status-failed",
            505: "auth-open-transaction-status-failed",
            506: "cache-read-retries-transaction-status-timeout",
            507: "prefix auth-read-transaction-status-failed",
        }
        row_count = 2048
        rows = [source_row(pk, texts.get(pk, "ordinary filler")) for pk in range(1, row_count + 1)]
        sealed_ids = {row["id"] for row in rows}
        schema = client.create_schema(auto_id=False, enable_dynamic_field=False)
        schema.add_field("id", DataType.INT64, is_primary=True)
        schema.add_field("vector", DataType.FLOAT_VECTOR, dim=4)
        if dtype == DataType.VARCHAR:
            schema.add_field("content", DataType.VARCHAR, max_length=65535)
        else:
            schema.add_field("content", DataType.TEXT)
        for field in ("repo", "path", "revision"):
            schema.add_field(field, DataType.VARCHAR, max_length=256)
        vector_index = client.prepare_index_params()
        vector_index.add_index(field_name="vector", index_type="FLAT", metric_type="L2")
        for name in names:
            client.create_collection(
                collection_name=name, schema=schema, shards_num=1, consistency_level="Strong", timeout=60
            )
            inserted = client.insert(collection_name=name, data=rows, timeout=60)
            assert inserted["insert_count"] == row_count
            client.flush(collection_name=name, timeout=60)
            client.create_index(collection_name=name, index_params=vector_index, timeout=180)

        ngram = client.prepare_index_params()
        ngram.add_index(
            field_name="content",
            index_type="NGRAM",
            index_name="content_ngram",
            params={"min_gram": 3, "max_gram": 3},
        )
        client.create_index(collection_name=indexed_name, index_params=ngram, sync=False, timeout=60)
        assert client.list_indexes(collection_name=raw_name, field_name="content", timeout=60) == []
        assert client.list_indexes(collection_name=indexed_name, field_name="content", timeout=60) == ["content_ngram"]
        deadline = time.monotonic() + 120
        while True:
            info = client.describe_index(collection_name=indexed_name, index_name="content_ngram", timeout=10)
            assert info and info["field_name"] == "content" and info["index_type"] == "NGRAM", info
            assert info["min_gram"] == info["max_gram"] == "3", info
            assert info["state"] != "Failed", info
            if (
                info["state"] == "Finished"
                and info["total_rows"] == info["indexed_rows"] == row_count
                and info["pending_index_rows"] == 0
            ):
                break
            assert time.monotonic() < deadline, info
            time.sleep(1)

        for name in names:
            client.load_collection(collection_name=name, replica_number=1, timeout=180)
            assert client.get_load_state(collection_name=name, timeout=60)["state"] == LoadState.Loaded
            confirm_state(client, name, rows, sealed_ids, wait=True)
            ordinary = client.query(
                collection_name=name,
                filter="",
                output_fields=FIELDS,
                limit=row_count,
                offset=0,
                consistency_level="Strong",
                timeout=60,
            )
            assert_results(ordinary, rows, {pk: [] for pk in sealed_ids}, FIELDS, match_field=None)
        loaded = {
            segment.segment_id: segment
            for segment in client.list_loaded_segments(collection_name=indexed_name, timeout=60)
            if segment.state == SegmentState.Sealed
        }
        sealed_sizes = {sid: segment.num_rows for sid, segment in loaded.items()}
        assert sum(sealed_sizes.values()) == row_count, sealed_sizes
        if min(sealed_sizes.values()) <= 1024:
            pytest.skip(f"BLOCKED: expected loaded Sealed segments above 1024 rows: {sealed_sizes}")
        collection_id = next(iter(loaded.values())).collection_id
        log.info(
            f"Complex Grep NGRAM collection={indexed_name} collection_id={collection_id} "
            f"sealed_sizes={sealed_sizes} index={info}"
        )

        branch_args = '-e "(foo|bar).*(timeout|error)"'
        optional_args = '-e "prefix(foo|bar)?suffix"'
        branch_hits = {
            101: [detail(texts[101], 1, 0, [(0, 11)])],
            102: [detail(texts[102], 1, 0, [(0, 9)])],
            103: [detail(texts[103], 1, 0, [(0, 11)])],
            104: [detail(texts[104], 1, 0, [(0, 9)])],
            401: [detail("bar-error", 2, 7, [(0, 9)])],
            402: [detail(texts[402], 1, 0, [(0, 9)])],
        }
        optional_hits = {
            201: [detail(texts[201], 1, 0, [(0, 15)])],
            202: [detail(texts[202], 1, 0, [(0, 15)])],
            203: [detail(texts[203], 1, 0, [(0, 12)])],
            401: [detail("prefixsuffix", 3, 17, [(0, 12)])],
            402: [detail(texts[402], 1, 0, [(10, 22)])],
        }
        cases = [
            (branch_args, branch_hits),
            (optional_args, optional_hits),
            (
                '-e "(?i:foo|bar).*(错误|timeout)"',
                {
                    101: branch_hits[101],
                    103: branch_hits[103],
                    301: [detail(texts[301], 1, 0, [(11, 22)])],
                    302: [detail("BAR-错误", 2, 7, [(0, 10)])],
                },
            ),
            (
                f"{branch_args} {optional_args}",
                {
                    **branch_hits,
                    **optional_hits,
                    401: branch_hits[401] + optional_hits[401],
                    402: [detail(texts[402], 1, 0, [(0, 9), (10, 22)])],
                },
            ),
            (
                '-e "^((auth|cache)-(read|write)|storage-(open|close))(-retry)?-transaction-status-(failed|timeout)$"',
                {pk: [detail(texts[pk], 1, 0, [(0, len(texts[pk].encode("utf-8")))])] for pk in (501, 502, 503, 504)},
            ),
            ('-e "(foo|bar).*(queued|ready)"', {}),
        ]
        for args, expected in cases:
            results = {}
            for name in names:
                actual = client.grep(
                    collection_name=name,
                    text_field="content",
                    args=args,
                    filter="",
                    output_fields=FIELDS,
                    limit=2048,
                    offset=0,
                    match_field="matches",
                    consistency_level="Strong",
                    timeout=60,
                )
                log.info(f"Complex Grep collection={name} args={args!r} rows={actual!r}")
                results[name] = assert_results(actual, rows, expected, FIELDS)
            assert results[raw_name] == results[indexed_name], args

        # Pad a valid expression to exceed the default arguments budget by one byte.
        oversized_args = branch_args.ljust(65537)
        for args, error_code, reason in (
            ('-e "["', 2042, "invalid regex"),
            (oversized_args, 1102, "grep arguments too large: actual=65537 bytes, limit=65536 bytes"),
        ):
            with pytest.raises(MilvusException) as caught:
                client.grep(
                    collection_name=indexed_name,
                    text_field="content",
                    args=args,
                    filter="",
                    output_fields=FIELDS,
                    limit=2048,
                    offset=0,
                    match_field="matches",
                    consistency_level="Strong",
                    timeout=60,
                )
            error = caught.value
            log.info(f"Complex Grep rejected collection={indexed_name} code={error.code} message={error.message}")
            assert error.code == error_code and reason in error.message, error
            recovered = client.grep(
                collection_name=indexed_name,
                text_field="content",
                args=branch_args,
                filter="",
                output_fields=FIELDS,
                limit=2048,
                offset=0,
                match_field="matches",
                consistency_level="Strong",
                timeout=60,
            )
            log.info(f"Complex Grep recovery collection={indexed_name} args={branch_args!r} rows={recovered!r}")
            assert_results(recovered, rows, branch_hits, FIELDS)

    @pytest.mark.tags(CaseLabel.L2)
    def test_grep_small_pages_with_and_without_ngram(self, grep_client, request):
        """TC-L2-04. Compare sparse and filtered page sizes with and without an NGRAM index."""
        client = grep_client
        prefix = f"grep_l2_04_{uuid4().hex[:12]}"
        raw_name, indexed_name = f"{prefix}_raw", f"{prefix}_ngram"
        names = (raw_name, indexed_name)

        def drop_test_collections():
            try:
                client.drop_collection(collection_name=raw_name, timeout=60)
            finally:
                client.drop_collection(collection_name=indexed_name, timeout=60)

        request.addfinalizer(drop_test_collections)
        rows = sparse_rows()
        for row in rows:
            if row["id"] in (255, 383):
                row["path"] = f"src/core/{row['id']}.py"
        for pk in range(388, 2048):
            content = "header\n中文 needle-unique 末" if pk == 1901 else "ordinary filler"
            path = f"src/core/{pk}.py" if pk % 2 == 0 else f"docs/{pk}.md"
            rows.append(source_row(pk, content, path=path))
        random.Random(20261111).shuffle(rows)
        assert len(rows) == len({row["id"] for row in rows}) == 2048

        schema = client.create_schema(auto_id=False, enable_dynamic_field=False)
        schema.add_field("id", DataType.INT64, is_primary=True)
        schema.add_field("vector", DataType.FLOAT_VECTOR, dim=4)
        schema.add_field("content", DataType.VARCHAR, max_length=65535)
        for field in ("repo", "path", "revision"):
            schema.add_field(field, DataType.VARCHAR, max_length=256)
        vector_index = client.prepare_index_params()
        vector_index.add_index(field_name="vector", index_type="FLAT", metric_type="L2")
        for name in names:
            client.create_collection(
                collection_name=name, schema=schema, shards_num=1, consistency_level="Strong", timeout=60
            )
            inserted = client.insert(collection_name=name, data=rows, timeout=60)
            assert inserted["insert_count"] == 2048
            client.flush(collection_name=name, timeout=60)
            client.create_index(collection_name=name, index_params=vector_index, timeout=180)

        ngram = client.prepare_index_params()
        ngram.add_index(
            field_name="content",
            index_type="NGRAM",
            index_name="content_ngram",
            params={"min_gram": 3, "max_gram": 3},
        )
        client.create_index(collection_name=indexed_name, index_params=ngram, sync=False, timeout=60)
        assert client.list_indexes(collection_name=raw_name, field_name="content", timeout=60) == []
        assert client.list_indexes(collection_name=indexed_name, field_name="content", timeout=60) == ["content_ngram"]
        deadline = time.monotonic() + 120
        while True:
            info = client.describe_index(collection_name=indexed_name, index_name="content_ngram", timeout=10)
            assert info and info["field_name"] == "content" and info["index_type"] == "NGRAM", info
            assert info["min_gram"] == info["max_gram"] == "3", info
            assert info["state"] != "Failed", info
            if (
                info["state"] == "Finished"
                and info["total_rows"] == info["indexed_rows"] == 2048
                and info["pending_index_rows"] == 0
            ):
                break
            assert time.monotonic() < deadline, info
            time.sleep(1)

        for name in names:
            client.load_collection(collection_name=name, replica_number=1, timeout=180)
            load_state = client.get_load_state(collection_name=name, timeout=60)
            assert load_state["state"] == LoadState.Loaded, load_state
            deadline = time.monotonic() + 60
            while True:
                sealed = {
                    segment.segment_id: segment
                    for segment in client.list_loaded_segments(collection_name=name, timeout=60)
                    if segment.state == SegmentState.Sealed
                }
                flushed = {
                    segment.segment_id
                    for segment in client.list_persistent_segments(
                        collection_name=name, states=[ClientSegmentState.Flushed], timeout=60
                    )
                }
                sealed_sizes = {sid: segment.num_rows for sid, segment in sealed.items()}
                if set(sealed) <= flushed and sum(sealed_sizes.values()) == 2048:
                    break
                assert time.monotonic() < deadline, (name, sealed_sizes, flushed)
                time.sleep(1)
            if name == indexed_name and min(sealed_sizes.values()) <= 1024:
                pytest.skip(f"BLOCKED: expected loaded Sealed segments above 1024 rows: {sealed_sizes}")
            collection_id = next(iter(sealed.values())).collection_id
            log.info(
                f"Grep NGRAM pagination ready collection={name} collection_id={collection_id} "
                f"sealed_sizes={sealed_sizes} index={info if name == indexed_name else None}"
            )

        error_args = "-F -e error"
        all_hits = {pk: [detail("error marker", 2, 8, [(0, 5)])] for pk in (128, 255, 256, 383, 384)}
        unique_hit = {1901: [detail("中文 needle-unique 末", 2, 7, [(7, 20)])]}
        core_hits = {pk: all_hits[pk] for pk in (255, 383)}
        src_filter = 'repo == "demo" and path like "src/%"'
        core_filter = 'repo == "demo" and path like "src/core/%"'
        docs_filter = 'repo == "demo" and path like "docs/%"'
        cases = [
            ("unique", "-F -e needle-unique", "", 1, 0, unique_hit),
            ("first_match", error_args, "", 1, 0, all_hits),
            ("middle_page", error_args, "", 2, 1, all_hits),
            ("last_match", error_args, "", 2, 4, all_hits),
            ("all_matches", error_args, "", 10, 0, all_hits),
            ("exact_end", error_args, "", 1, 5, all_hits),
            ("beyond_end", error_args, "", 1, 6, all_hits),
            ("filter_all", error_args, src_filter, 2, 1, all_hits),
            ("filter_core_full", error_args, core_filter, 2, 0, core_hits),
            ("filter_core_page", error_args, core_filter, 2, 1, core_hits),
            ("filter_no_hits", error_args, docs_filter, 2, 0, {}),
        ]
        for label, args, filter_expr, limit, offset, eligible in cases:
            expected_count = min(limit, max(len(eligible) - offset, 0))
            count = None if expected_count == len(eligible) else expected_count
            results = {}
            for name in names:
                actual = client.grep(
                    collection_name=name,
                    text_field="content",
                    args=args,
                    filter=filter_expr,
                    output_fields=FIELDS,
                    limit=limit,
                    offset=offset,
                    match_field="matches",
                    consistency_level="Strong",
                    timeout=60,
                )
                results[name] = assert_results(actual, rows, eligible, FIELDS, count=count)
                log.info(
                    f"Grep NGRAM pagination result collection={name} case={label} args={args!r} "
                    f"filter={filter_expr!r} limit={limit} offset={offset} rows={results[name]!r}"
                )
            if count is None:
                assert results[raw_name] == results[indexed_name], label

    @pytest.mark.tags(CaseLabel.L2)
    def test_grep_long_varchar_with_null_and_empty_with_and_without_ngram(self, grep_client, request):
        """TC-L2-05. Check complete long VARCHAR results alongside short, NULL, and empty values."""
        client = grep_client
        prefix = f"grep_l2_05_{uuid4().hex[:12]}"
        raw_name, indexed_name = f"{prefix}_raw", f"{prefix}_ngram"
        names = (raw_name, indexed_name)

        def drop_test_collections():
            try:
                client.drop_collection(collection_name=raw_name, timeout=60)
            finally:
                client.drop_collection(collection_name=indexed_name, timeout=60)

        request.addfinalizer(drop_test_collections)
        marker = "grep-marker"
        marker_bytes = marker.encode("utf-8")
        short_text = "ok grep-marker"
        single_line = marker + "a" * (32768 - len(marker)) + marker + "b" * (65535 - 32768 - 2 * len(marker)) + marker
        first_line = "头🙂 grep-marker"
        # Keep 61,440 bytes while including three consecutive nonmatching lines.
        middle_line = "m" * 30720 + "中 grep-marker" + "n" * 30647
        last_line = "尾🙂 grep-marker"
        multi_line = "\n".join((first_line, "", "line 3 no match", "", middle_line, last_line))
        assert len(single_line.encode("utf-8")) == 65535
        assert len(multi_line.encode("utf-8")) == 61440
        filler = "ordinary filler"
        rows = []
        cases = []
        for repo, base, row_count, target_bytes in (
            ("size_192_kib", 1000, 682, 192 * 1024),
            ("size_256_kib", 2000, 683, 256 * 1024),
            ("size_320_kib", 3000, 683, 320 * 1024),
        ):
            samples = [
                source_row(base + 1, short_text, repo=repo),
                source_row(base + 2, single_line, repo=repo),
                source_row(base + 3, multi_line, repo=repo),
                source_row(base + 4, None, repo=repo),
                source_row(base + 5, "", repo=repo),
            ]
            short_fillers = [source_row(pk, filler, repo=repo) for pk in range(base + 10, base + row_count + 1)]
            short_and_match_bytes = sum(
                len(row["content"].encode("utf-8")) for row in samples + short_fillers if row["content"] is not None
            )
            long_filler_size, remainder = divmod(target_bytes - short_and_match_bytes, 4)
            assert 0 < long_filler_size < 65535
            long_fillers = [
                source_row(base + 6 + i, "z" * (long_filler_size + (i < remainder)), repo=repo) for i in range(4)
            ]
            group = samples + long_fillers + short_fillers
            total_bytes = sum(len(row["content"].encode("utf-8")) for row in group if row["content"] is not None)
            assert len(group) == row_count and total_bytes == target_bytes
            rows.extend(group)
            expected = {
                base + 1: [detail(short_text, 1, 0, [(3, 14)])],
                base + 2: [detail(single_line, 1, 0, [(0, 11), (32768, 32779), (65524, 65535)])],
                base + 3: [
                    detail(first_line, 1, 0, [(8, 19)]),
                    detail(middle_line, 5, 38, [(30724, 30735)]),
                    detail(last_line, 6, 61421, [(8, 19)]),
                ],
            }
            cases.append((repo, f'repo == "{repo}"', expected))
            log.info(
                f"Grep long VARCHAR fixture repo={repo} rows={row_count} "
                f"source_bytes={total_bytes} long_filler_bytes="
                f"{[len(row['content']) for row in long_fillers]} null_id={base + 4} empty_id={base + 5}"
            )
        random.Random(20261111).shuffle(rows)
        assert len(rows) == len({row["id"] for row in rows}) == 2048
        matched_ids = {pk for _, _, expected in cases for pk in expected}
        source_bytes = {row["id"]: row["content"].encode("utf-8") for row in rows if row["id"] in matched_ids}
        source_sha256 = {pk: hashlib.sha256(value).hexdigest() for pk, value in source_bytes.items()}

        schema = client.create_schema(auto_id=False, enable_dynamic_field=False)
        schema.add_field("id", DataType.INT64, is_primary=True)
        schema.add_field("vector", DataType.FLOAT_VECTOR, dim=4)
        schema.add_field("content", DataType.VARCHAR, max_length=65535, nullable=True)
        for field in ("repo", "path", "revision"):
            schema.add_field(field, DataType.VARCHAR, max_length=256)
        vector_index = client.prepare_index_params()
        vector_index.add_index(field_name="vector", index_type="FLAT", metric_type="L2")
        for name in names:
            client.create_collection(
                collection_name=name, schema=schema, shards_num=1, consistency_level="Strong", timeout=60
            )
            inserted = client.insert(collection_name=name, data=rows, timeout=60)
            assert inserted["insert_count"] == 2048
            client.flush(collection_name=name, timeout=60)
            client.create_index(collection_name=name, index_params=vector_index, timeout=180)

        ngram = client.prepare_index_params()
        ngram.add_index(
            field_name="content",
            index_type="NGRAM",
            index_name="content_ngram",
            params={"min_gram": 3, "max_gram": 3},
        )
        client.create_index(collection_name=indexed_name, index_params=ngram, sync=False, timeout=60)
        assert client.list_indexes(collection_name=raw_name, field_name="content", timeout=60) == []
        assert client.list_indexes(collection_name=indexed_name, field_name="content", timeout=60) == ["content_ngram"]
        deadline = time.monotonic() + 120
        while True:
            info = client.describe_index(collection_name=indexed_name, index_name="content_ngram", timeout=10)
            assert info and info["field_name"] == "content" and info["index_type"] == "NGRAM", info
            assert info["min_gram"] == info["max_gram"] == "3", info
            assert info["state"] != "Failed", info
            if (
                info["state"] == "Finished"
                and info["total_rows"] == info["indexed_rows"] == 2048
                and info["pending_index_rows"] == 0
            ):
                break
            assert time.monotonic() < deadline, info
            time.sleep(1)

        for name in names:
            client.load_collection(collection_name=name, replica_number=1, timeout=180)
            load_state = client.get_load_state(collection_name=name, timeout=60)
            assert load_state["state"] == LoadState.Loaded, load_state
            deadline = time.monotonic() + 60
            while True:
                sealed = {
                    segment.segment_id: segment
                    for segment in client.list_loaded_segments(collection_name=name, timeout=60)
                    if segment.state == SegmentState.Sealed
                }
                flushed = {
                    segment.segment_id
                    for segment in client.list_persistent_segments(
                        collection_name=name, states=[ClientSegmentState.Flushed], timeout=60
                    )
                }
                sealed_sizes = {sid: segment.num_rows for sid, segment in sealed.items()}
                if set(sealed) <= flushed and sum(sealed_sizes.values()) == 2048:
                    break
                assert time.monotonic() < deadline, (name, sealed_sizes, flushed)
                time.sleep(1)
            if name == indexed_name and min(sealed_sizes.values()) <= 1024:
                pytest.skip(f"BLOCKED: expected loaded Sealed segments above 1024 rows: {sealed_sizes}")
            collection_id = next(iter(sealed.values())).collection_id
            log.info(
                f"Grep long VARCHAR ready collection={name} collection_id={collection_id} "
                f"sealed_sizes={sealed_sizes} index={info if name == indexed_name else None}"
            )

        for repo, filter_expr, expected in cases:
            results = {}
            for name in names:
                actual = client.grep(
                    collection_name=name,
                    text_field="content",
                    args="-F -e grep-marker",
                    filter=filter_expr,
                    output_fields=FIELDS,
                    limit=100,
                    offset=0,
                    match_field="matches",
                    consistency_level="Strong",
                    timeout=60,
                )
                actual_ids = [row["id"] for row in actual]
                assert len(actual_ids) == len(expected) and set(actual_ids) == set(expected), actual_ids
                evidence = {}
                for row in actual:
                    pk = row["id"]
                    value = row["content"].encode("utf-8")
                    wanted = source_bytes[pk]
                    assert len(value) == len(wanted), (name, repo, pk, len(value), len(wanted))
                    sha256 = hashlib.sha256(value).hexdigest()
                    assert sha256 == source_sha256[pk], (name, repo, pk, sha256, source_sha256[pk])
                    assert value == wanted, (name, repo, pk)
                    coordinates = []
                    for hit in row["matches"]:
                        line_start = hit["line_start_offset"]
                        spans = list(zip(hit["starts"], hit["ends"], strict=True))
                        for start, end in spans:
                            assert value[line_start + start : line_start + end] == marker_bytes
                        coordinates.append((hit["line_number"], line_start, spans))
                    evidence[pk] = {"bytes": len(value), "sha256": sha256, "coordinates": coordinates}
                results[name] = assert_results(actual, rows, expected, FIELDS)
                log.info(
                    f"Grep long VARCHAR result collection={name} repo={repo} filter={filter_expr!r} rows={evidence}"
                )
            assert results[raw_name] == results[indexed_name], repo


class TestMilvusClientGrepIndexStateIndependent(GrepBase):
    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize("dtype", [DataType.VARCHAR, DataType.TEXT], ids=["VARCHAR", "TEXT"])
    def test_grep_data_and_index_state_transitions(self, grep_client, request, dtype):
        """TC-L2-03. Check exact results through flush, index creation, another insert, and index drop."""
        client = grep_client
        name = f"grep_l2_03_{dtype.name.lower()}_{uuid4().hex[:12]}"
        old_texts = {
            101: "header\nerror old",
            102: "x",
            103: "id=17",
            104: "begin old end",
            105: "ERROR only",
            106: "begin\nend",
        }
        new_texts = {
            201: "头🙂\n前 error 新",
            202: "a x z",
            203: "id=930 code=8",
            204: "header\nbegin 新 end",
            205: "ordinary note",
            206: "end begin",
        }
        base_rows = [source_row(pk, text, repo="initial") for pk, text in old_texts.items()]
        base_rows.extend(source_row(pk, "ordinary filler") for pk in range(1000, 3042))
        extra_rows = [source_row(pk, text, repo="appended") for pk, text in new_texts.items()]
        all_rows = base_rows + extra_rows
        base_ids = {row["id"] for row in base_rows}
        assert len(base_rows) == len(base_ids) == 2048
        cases = [
            (
                "-F -e error",
                {
                    101: [detail("error old", 2, 7, [(0, 5)])],
                    201: [detail("前 error 新", 2, 8, [(4, 9)])],
                },
            ),
            (
                "-F -e x",
                {
                    102: [detail(old_texts[102], 1, 0, [(0, 1)])],
                    202: [detail(new_texts[202], 1, 0, [(2, 3)])],
                },
            ),
            (
                '-e "[0-9]+"',
                {
                    103: [detail(old_texts[103], 1, 0, [(3, 5)])],
                    203: [detail(new_texts[203], 1, 0, [(3, 6), (12, 13)])],
                },
            ),
            (
                '-e "begin.*end"',
                {
                    104: [detail(old_texts[104], 1, 0, [(0, 13)])],
                    204: [detail("begin 新 end", 2, 7, [(0, 13)])],
                },
            ),
        ]
        schema = client.create_schema(auto_id=False, enable_dynamic_field=False)
        schema.add_field("id", DataType.INT64, is_primary=True)
        schema.add_field("vector", DataType.FLOAT_VECTOR, dim=4)
        if dtype == DataType.VARCHAR:
            schema.add_field("content", DataType.VARCHAR, max_length=65535)
        else:
            schema.add_field("content", DataType.TEXT)
        for field in ("repo", "path", "revision"):
            schema.add_field(field, DataType.VARCHAR, max_length=256)
        client.create_collection(
            collection_name=name, schema=schema, shards_num=1, consistency_level="Strong", timeout=60
        )
        request.addfinalizer(lambda: client.drop_collection(collection_name=name, timeout=60))
        vector_index = client.prepare_index_params()
        vector_index.add_index(field_name="vector", index_name="vector", index_type="FLAT", metric_type="L2")
        client.create_index(collection_name=name, index_params=vector_index, timeout=180)
        client.load_collection(collection_name=name, replica_number=1, timeout=180)
        inserted = client.insert(collection_name=name, data=base_rows, timeout=60)
        assert inserted["insert_count"] == 2048

        stages = (
            ("growing_no_ngram", base_rows, set(), False),
            ("sealed_no_ngram", base_rows, base_ids, False),
            ("sealed_ngram", base_rows, base_ids, True),
            ("mixed_ngram", all_rows, base_ids, True),
            ("after_drop_ngram", all_rows, None, False),
        )
        comparisons = {
            "sealed_no_ngram": "growing_no_ngram",
            "sealed_ngram": "sealed_no_ngram",
            "after_drop_ngram": "mixed_ngram",
        }
        by_stage = {}
        for stage, current_rows, expected_sealed, has_ngram in stages:
            if stage == "sealed_no_ngram":
                client.flush(collection_name=name, timeout=60)
            if stage == "mixed_ngram":
                inserted = client.insert(collection_name=name, data=extra_rows, timeout=60)
                assert inserted["insert_count"] == len(extra_rows)
            if stage in ("sealed_ngram", "after_drop_ngram"):
                client.release_collection(collection_name=name, timeout=60)
                deadline = time.monotonic() + 60
                while True:
                    load_state = client.get_load_state(collection_name=name, timeout=10)
                    if load_state["state"] == LoadState.NotLoad:
                        break
                    assert time.monotonic() < deadline, load_state
                    time.sleep(1)
                if stage == "sealed_ngram":
                    text_index = client.prepare_index_params()
                    text_index.add_index(
                        field_name="content",
                        index_type="NGRAM",
                        index_name="content_ngram",
                        params={"min_gram": 3, "max_gram": 3},
                    )
                    client.create_index(collection_name=name, index_params=text_index, sync=False, timeout=60)
                    deadline = time.monotonic() + 120
                    while True:
                        info = client.describe_index(collection_name=name, index_name="content_ngram", timeout=10)
                        assert info and info["field_name"] == "content" and info["index_type"] == "NGRAM", info
                        assert info["min_gram"] == info["max_gram"] == "3", info
                        assert info["state"] != "Failed", info
                        if (
                            info["state"] == "Finished"
                            and info["total_rows"] == info["indexed_rows"] == 2048
                            and info["pending_index_rows"] == 0
                        ):
                            break
                        assert time.monotonic() < deadline, info
                        time.sleep(1)
                    log.info(f"Grep state transition index built collection={name} stage={stage} index={info}")
                else:
                    client.drop_index(collection_name=name, index_name="content_ngram", timeout=60)
                client.load_collection(collection_name=name, replica_number=1, timeout=180)

            load_state = client.get_load_state(collection_name=name, timeout=60)
            assert load_state["state"] == LoadState.Loaded, load_state
            content_indexes = client.list_indexes(collection_name=name, field_name="content", timeout=60)
            assert content_indexes == (["content_ngram"] if has_ngram else []), content_indexes
            assert client.list_indexes(collection_name=name, field_name="vector", timeout=60) == ["vector"]
            current_ids = {row["id"] for row in current_rows}
            all_visible = client.query(
                collection_name=name,
                filter="",
                output_fields=["id"],
                limit=2054,
                offset=0,
                ignore_growing=False,
                consistency_level="Strong",
                timeout=60,
            )
            assert len(all_visible) == len(current_ids) and {row["id"] for row in all_visible} == current_ids
            deadline = time.monotonic() + 60
            while True:
                sealed_only = client.query(
                    collection_name=name,
                    filter="",
                    output_fields=["id"],
                    limit=2054,
                    offset=0,
                    ignore_growing=True,
                    consistency_level="Strong",
                    timeout=60,
                )
                actual_sealed = {row["id"] for row in sealed_only}
                loaded = {
                    segment.segment_id: segment.num_rows
                    for segment in client.list_loaded_segments(collection_name=name, timeout=60)
                    if segment.state == SegmentState.Sealed
                }
                flushed = {
                    segment.segment_id
                    for segment in client.list_persistent_segments(
                        collection_name=name, states=[ClientSegmentState.Flushed], timeout=60
                    )
                }
                # Re-observe the appended batch after release/load.
                if expected_sealed is None:
                    wanted_state = base_ids <= actual_sealed <= current_ids
                else:
                    wanted_state = actual_sealed == expected_sealed
                if (
                    wanted_state
                    and len(sealed_only) == len(actual_sealed)
                    and set(loaded) <= flushed
                    and sum(loaded.values()) == len(actual_sealed)
                ):
                    break
                if time.monotonic() >= deadline:
                    pytest.skip(
                        f"BLOCKED: collection={name} stage={stage} expected sealed rows="
                        f"{len(expected_sealed) if expected_sealed is not None else 'at least 2048'}, "
                        f"actual sealed rows={len(actual_sealed)} loaded={loaded} flushed={sorted(flushed)}"
                    )
                time.sleep(1)
            if has_ngram and min(loaded.values()) <= 1024:
                pytest.skip(f"BLOCKED: collection={name} stage={stage} loaded Sealed segments too small: {loaded}")
            log.info(
                f"Grep state transition collection={name} stage={stage} content_indexes={content_indexes} "
                f"visible={len(current_ids)} sealed={len(actual_sealed)} growing={len(current_ids - actual_sealed)} "
                f"sealed_segments={loaded} sealed_witnesses={sorted((set(old_texts) | set(new_texts)) & actual_sealed)}"
            )

            stage_results = {}
            for args, expected_all in cases:
                expected = {pk: hits for pk, hits in expected_all.items() if pk in current_ids}
                actual = client.grep(
                    collection_name=name,
                    text_field="content",
                    args=args,
                    filter="",
                    output_fields=FIELDS,
                    limit=2054,
                    offset=0,
                    match_field="matches",
                    consistency_level="Strong",
                    timeout=60,
                )
                stage_results[args] = assert_results(actual, current_rows, expected, FIELDS)
                log.info(f"Grep state transition result collection={name} stage={stage} args={args!r} rows={actual!r}")
            reference_stage = comparisons.get(stage)
            if reference_stage is not None:
                assert stage_results == by_stage[reference_stage], (stage, reference_stage)
            by_stage[stage] = stage_results


class TestMilvusClientGrepMaxCountSealedAndGrowing(GrepBase):
    """Match-line limits in growing, sealed, and indexed sealed collections."""

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize("dtype", [DataType.VARCHAR, DataType.TEXT], ids=["VARCHAR", "TEXT"])
    @pytest.mark.parametrize("state", ["growing", "sealed", "sealed-ngram"])
    def test_grep_max_count_and_record_pagination(self, grep_client, request, dtype, state):
        """TC-L1-17. Cap matching lines per record and paginate matching records."""
        client = grep_client
        name = f"grep_l1_17_{dtype.name.lower()}_{state.replace('-', '_')}_{uuid4().hex[:12]}"

        def drop_test_collection():
            if client.has_collection(collection_name=name, timeout=60):
                client.drop_collection(collection_name=name, timeout=60)

        request.addfinalizer(drop_test_collection)
        texts = {
            401: "error error\ninfo\nerror\nerror",
            402: "info\nerror\nerror error",
            403: "error\nerror\nerror",
            404: "info only",
        }
        if state == "growing":
            rows = [source_row(pk, content) for pk, content in texts.items()]
        else:
            rows = [source_row(pk, texts.get(pk, "ordinary filler")) for pk in range(1, 2049)]

        schema = client.create_schema(auto_id=False, enable_dynamic_field=False)
        schema.add_field("id", DataType.INT64, is_primary=True)
        schema.add_field("vector", DataType.FLOAT_VECTOR, dim=4)
        schema.add_field("content", dtype, **({"max_length": 65535} if dtype == DataType.VARCHAR else {}))
        for field in ("repo", "path", "revision"):
            schema.add_field(field, DataType.VARCHAR, max_length=256)
        client.create_collection(
            collection_name=name, schema=schema, shards_num=1, consistency_level="Strong", timeout=60
        )

        sealed_ids = set()
        if state != "growing":
            inserted = client.insert(collection_name=name, data=rows, timeout=60)
            assert inserted["insert_count"] == len(rows)
            client.flush(collection_name=name, timeout=60)
            sealed_ids = {row["id"] for row in rows}
        indexes = client.prepare_index_params()
        indexes.add_index(field_name="vector", index_type="FLAT", metric_type="L2")
        if state == "sealed-ngram":
            indexes.add_index(
                field_name="content",
                index_type="NGRAM",
                index_name="content_ngram",
                params={"min_gram": 3, "max_gram": 3},
            )
        client.create_index(collection_name=name, index_params=indexes, timeout=180)
        client.load_collection(collection_name=name, replica_number=1, timeout=180)
        if state == "growing":
            inserted = client.insert(collection_name=name, data=rows, timeout=60)
            assert inserted["insert_count"] == len(rows)
        confirm_state(client, name, rows, sealed_ids, wait=True)

        scalar_indexes = client.list_indexes(collection_name=name, field_name="content", timeout=60)
        if state == "sealed-ngram":
            assert scalar_indexes == ["content_ngram"]
            segments = {
                segment.segment_id: segment.num_rows
                for segment in client.list_loaded_segments(collection_name=name, timeout=60)
                if segment.state == SegmentState.Sealed
            }
            assert sum(segments.values()) == len(rows), segments
            if min(segments.values()) <= 1024:
                pytest.skip(f"BLOCKED: expected loaded Sealed segments above 1024 rows: {segments}")

            deadline = time.monotonic() + 120
            while True:
                info = client.describe_index(collection_name=name, index_name="content_ngram", timeout=60)
                assert info is not None and info["field_name"] == "content" and info["index_type"] == "NGRAM", info
                assert info["min_gram"] == info["max_gram"] == "3", info
                assert info["state"] != "Failed", info
                if (
                    info["state"] == "Finished"
                    and info["total_rows"] == info["indexed_rows"] == len(rows)
                    and info["pending_index_rows"] == 0
                ):
                    break
                assert time.monotonic() < deadline, info
                time.sleep(1)
            log.info(f"Grep max-count index ready collection={name} sealed_sizes={segments} index={info}")
        else:
            assert scalar_indexes == []

        all_matches = {
            401: [
                detail("error error", 1, 0, [(0, 5), (6, 11)]),
                detail("error", 3, 17, [(0, 5)]),
                detail("error", 4, 23, [(0, 5)]),
            ],
            402: [
                detail("error", 2, 5, [(0, 5)]),
                detail("error error", 3, 11, [(0, 5), (6, 11)]),
            ],
            403: [
                detail("error", 1, 0, [(0, 5)]),
                detail("error", 2, 6, [(0, 5)]),
                detail("error", 3, 12, [(0, 5)]),
            ],
        }
        capped = {count: {pk: lines[:count] for pk, lines in all_matches.items()} for count in (1, 2)}
        capped[0] = {}
        capped[None] = all_matches
        cases = [
            ("-F -e error", None, 100, 0, None),
            ("-F -m 0 -e error", 0, 100, 0, None),
            ("-F -m 1 -e error", 1, 100, 0, None),
            ("-F -m 2 -e error", 2, 100, 0, None),
            ("-F -m2 -e error", 2, 100, 0, None),
            ("-F --max-count=2 -e error", 2, 100, 0, None),
            ("-F --max-count 2 -e error", 2, 100, 0, None),
            ("-F -m 1 -m 2 -e error", 2, 100, 0, None),
            ("-F -m 2 -m 1 -e error", 1, 100, 0, None),
            ("-F -m 1 -e error", 1, 2, 0, 2),
            ("-F -m 1 -e error", 1, 2, 1, 2),
            ("-F -m 2 -e error", 2, 2, 2, 1),
            ("-F -m 1 -e error", 1, 2, 3, 0),
        ]
        for args, max_count, limit, offset, page_size in cases:
            grep_rows = client.grep(
                collection_name=name,
                text_field="content",
                args=args,
                filter="",
                output_fields=FIELDS,
                limit=limit,
                offset=offset,
                match_field="matches",
                consistency_level="Strong",
                timeout=60,
            )
            chain = FunctionChain(FunctionChainStage.QUERY_SCAN, name="grep", output_columns=["matches"])
            chain.grep("content", arguments=args, output="matches")
            query_rows = client.query(
                collection_name=name,
                filter="",
                output_fields=FIELDS,
                limit=limit,
                offset=offset,
                function_chains=chain,
                consistency_level="Strong",
                timeout=60,
            )
            log.info(
                f"Grep max-count collection={name} args={args!r} limit={limit} offset={offset} "
                f"grep={list(grep_rows)!r} query={list(query_rows)!r}"
            )
            expected = capped[max_count]
            grep_by_id = assert_results(grep_rows, rows, expected, FIELDS, count=page_size)
            query_by_id = assert_results(query_rows, rows, expected, FIELDS, count=page_size)
            if page_size is None:
                assert grep_by_id == query_by_id

        for max_count in (1, 2, 0):
            args = f"-F -m {max_count} -e error"
            chain = FunctionChain(FunctionChainStage.QUERY_SCAN, name="grep", output_columns=[])
            chain.grep("content", arguments=args, output="matches")
            query_rows = client.query(
                collection_name=name,
                filter="",
                output_fields=FIELDS,
                limit=100,
                offset=0,
                function_chains=chain,
                consistency_level="Strong",
                timeout=60,
            )
            log.info(f"Grep max-count no details collection={name} args={args!r} query={list(query_rows)!r}")
            assert_results(query_rows, rows, capped[max_count], FIELDS, match_field=None)

        with pytest.raises(MilvusException) as grep_error:
            client.grep(
                collection_name=name,
                text_field="content",
                args="-m 0 -e '['",
                filter="",
                output_fields=FIELDS,
                limit=100,
                offset=0,
                match_field="matches",
                consistency_level="Strong",
                timeout=60,
            )
        chain = FunctionChain(FunctionChainStage.QUERY_SCAN, name="grep", output_columns=["matches"])
        chain.grep("content", arguments="-m 0 -e '['", output="matches")
        with pytest.raises(MilvusException) as query_error:
            client.query(
                collection_name=name,
                filter="",
                output_fields=FIELDS,
                limit=100,
                offset=0,
                function_chains=chain,
                consistency_level="Strong",
                timeout=60,
            )
        for entry, error in (("grep", grep_error.value), ("query", query_error.value)):
            log.info(f"Grep max-count invalid regex collection={name} entry={entry} code={error.code} {error.message}")
            assert not isinstance(error, ParamError), error
            assert error.code == 2042 and "invalid regex" in error.message, error
        confirm_state(client, name, rows, sealed_ids)


class TestMilvusClientGrepMaxCountIndependent(GrepBase):
    """Numeric max-count boundaries on a small, visible text collection."""

    @pytest.mark.tags(CaseLabel.L1)
    def test_grep_max_count_argument_boundaries_and_recovery(self, grep_client, request):
        """TC-L1-18. Check exact uint64 boundaries, token parsing, and recovery through both SDK entries."""
        client = grep_client
        name = f"grep_l1_18_{uuid4().hex[:12]}"

        def drop_test_collection():
            if client.has_collection(collection_name=name, timeout=60):
                client.drop_collection(collection_name=name, timeout=60)

        request.addfinalizer(drop_test_collection)
        rows = [
            source_row(501, "error error\ninfo\nerror\nerror"),
            source_row(502, "ERROR\nerror\nERROR"),
            source_row(503, "flag -m here"),
            source_row(504, "ordinary"),
        ]
        schema = client.create_schema(auto_id=False, enable_dynamic_field=False)
        schema.add_field("id", DataType.INT64, is_primary=True)
        schema.add_field("vector", DataType.FLOAT_VECTOR, dim=4)
        schema.add_field("content", DataType.VARCHAR, max_length=65535)
        for field in ("repo", "path", "revision"):
            schema.add_field(field, DataType.VARCHAR, max_length=256)
        client.create_collection(
            collection_name=name, schema=schema, shards_num=1, consistency_level="Strong", timeout=60
        )
        indexes = client.prepare_index_params()
        indexes.add_index(field_name="vector", index_type="FLAT", metric_type="L2")
        client.create_index(collection_name=name, index_params=indexes, timeout=60)
        client.load_collection(collection_name=name, replica_number=1, timeout=60)
        inserted = client.insert(collection_name=name, data=rows, timeout=60)
        assert inserted["insert_count"] == len(rows)

        first_501 = detail("error error", 1, 0, [(0, 5), (6, 11)])
        third_501 = detail("error", 3, 17, [(0, 5)])
        second_502 = detail("error", 2, 6, [(0, 5)])
        one_line = {501: [first_501], 502: [second_502]}
        two_lines = {501: [first_501, third_501], 502: [second_502]}
        all_lines = {501: [first_501, third_501, detail("error", 4, 23, [(0, 5)])], 502: [second_502]}
        folded_two_lines = {
            **two_lines,
            502: [detail("ERROR", 1, 0, [(0, 5)]), second_502],
        }
        valid_cases = [
            ("-e error", all_lines),
            ("-m 0 -e error", {}),
            ("-m 1 -e error", one_line),
            ("-m 2 -e error", two_lines),
            ("-m 18446744073709551615 -e error", all_lines),
            ("-im2 -e error", folded_two_lines),
            ("-i -m 2 -e error", folded_two_lines),
            ("-e -m", {503: [detail("flag -m here", 1, 0, [(5, 7)])]}),
        ]
        for args, expected in valid_cases:
            grep_rows = client.grep(
                collection_name=name,
                text_field="content",
                args=args,
                filter="",
                output_fields=FIELDS,
                limit=100,
                offset=0,
                match_field="matches",
                consistency_level="Strong",
                timeout=60,
            )
            chain = FunctionChain(FunctionChainStage.QUERY_SCAN, name="grep", output_columns=["matches"])
            chain.grep("content", arguments=args, output="matches")
            query_rows = client.query(
                collection_name=name,
                filter="",
                output_fields=FIELDS,
                limit=100,
                offset=0,
                function_chains=chain,
                consistency_level="Strong",
                timeout=60,
            )
            log.info(f"Grep max-count valid args={args!r} grep={list(grep_rows)!r} query={list(query_rows)!r}")
            assert_results(grep_rows, rows, expected, FIELDS)
            assert_results(query_rows, rows, expected, FIELDS)

        invalid_count = "grep max-count must be an unsigned decimal integer in [0, 18446744073709551615]"
        invalid_cases = [
            ("-e error -m", 1101, "grep option -m requires a count"),
            ("-e error --max-count", 1101, "grep option --max-count requires a count"),
            (
                "--max-count=18446744073709551616 -e error",
                1100,
                f'{invalid_count}, got "18446744073709551616"',
            ),
            ('-m "" -e error', 1100, f'{invalid_count}, got ""'),
            ("--max-count= -e error", 1100, f'{invalid_count}, got ""'),
            ("-m -1 -e error", 1100, f'{invalid_count}, got "-1"'),
            ("-m +1 -e error", 1100, f'{invalid_count}, got "+1"'),
            ("-m 1.5 -e error", 1100, f'{invalid_count}, got "1.5"'),
            ("-m abc -e error", 1100, f'{invalid_count}, got "abc"'),
            ("-m 0x10 -e error", 1100, f'{invalid_count}, got "0x10"'),
            ("-e error -m -e", 1100, f'{invalid_count}, got "-e"'),
        ]
        for args, expected_code, expected_reason in invalid_cases:
            try:
                client.grep(
                    collection_name=name,
                    text_field="content",
                    args=args,
                    filter="",
                    output_fields=FIELDS,
                    limit=100,
                    offset=0,
                    match_field="matches",
                    consistency_level="Strong",
                    timeout=60,
                )
            except MilvusException as error:
                log.info(f"Grep max-count rejected args={args!r} entry=grep code={error.code} {error.message}")
                assert not isinstance(error, ParamError), error
                assert error.code == expected_code, error
                assert expected_reason in error.message, error
                assert error.is_input_error and not error.retriable, error
            else:
                pytest.fail(f"grep accepted {args!r}; expected server error {expected_code}")

            grep_rows = client.grep(
                collection_name=name,
                text_field="content",
                args="-m 1 -e error",
                filter="",
                output_fields=FIELDS,
                limit=100,
                offset=0,
                match_field="matches",
                consistency_level="Strong",
                timeout=60,
            )
            log.info(f"Grep max-count recovery after={args!r} entry=grep rows={list(grep_rows)!r}")
            assert_results(grep_rows, rows, one_line, FIELDS)

            chain = FunctionChain(FunctionChainStage.QUERY_SCAN, name="grep", output_columns=["matches"])
            chain.grep("content", arguments=args, output="matches")
            try:
                client.query(
                    collection_name=name,
                    filter="",
                    output_fields=FIELDS,
                    limit=100,
                    offset=0,
                    function_chains=chain,
                    consistency_level="Strong",
                    timeout=60,
                )
            except MilvusException as error:
                log.info(f"Grep max-count rejected args={args!r} entry=query code={error.code} {error.message}")
                assert not isinstance(error, ParamError), error
                assert error.code == expected_code, error
                assert expected_reason in error.message, error
                assert error.is_input_error and not error.retriable, error
            else:
                pytest.fail(f"query accepted {args!r}; expected server error {expected_code}")

            recovery_chain = FunctionChain(FunctionChainStage.QUERY_SCAN, name="grep", output_columns=["matches"])
            recovery_chain.grep("content", arguments="-m 1 -e error", output="matches")
            query_rows = client.query(
                collection_name=name,
                filter="",
                output_fields=FIELDS,
                limit=100,
                offset=0,
                function_chains=recovery_chain,
                consistency_level="Strong",
                timeout=60,
            )
            log.info(f"Grep max-count recovery after={args!r} entry=query rows={list(query_rows)!r}")
            assert_results(query_rows, rows, one_line, FIELDS)


class TestMilvusClientGrepNgramAdmissionIndependent(GrepBase):
    """TC-L1-19. Validate explicit TEXT NGRAM creation and VARCHAR policy compatibility."""

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize("state", ["growing", "sealed"])
    @pytest.mark.parametrize("min_gram,max_gram", [(1, 1), (2, 3), (3, 4)], ids=["1-1", "2-3", "3-4"])
    def test_create_text_ngram(self, grep_client, request, state, min_gram, max_gram):
        client = grep_client
        name = f"grep_l1_19_text_{state}_{min_gram}_{max_gram}_{uuid4().hex[:12]}"
        request.addfinalizer(lambda: client.drop_collection(collection_name=name, timeout=60))
        schema = client.create_schema(auto_id=False, enable_dynamic_field=False)
        schema.add_field("id", DataType.INT64, is_primary=True)
        schema.add_field("vector", DataType.FLOAT_VECTOR, dim=4)
        schema.add_field("content", DataType.TEXT, nullable=True)
        client.create_collection(
            collection_name=name, schema=schema, shards_num=1, consistency_level="Strong", timeout=60
        )
        vector_indexes = client.prepare_index_params()
        vector_indexes.add_index(field_name="vector", index_type="FLAT", metric_type="L2")
        client.create_index(collection_name=name, index_params=vector_indexes, timeout=60)
        client.load_collection(collection_name=name, replica_number=1, timeout=60)
        rows = [
            {"id": 1, "content": "error timeout", "vector": [0.25, 0.5, 0.75, 1.0]},
            {"id": 2, "content": "header\n中文🙂 error", "vector": [0.25, 0.5, 0.75, 1.0]},
            {"id": 3, "content": "", "vector": [0.25, 0.5, 0.75, 1.0]},
            {"id": 4, "content": None, "vector": [0.25, 0.5, 0.75, 1.0]},
        ]
        inserted = client.insert(collection_name=name, data=rows, timeout=60)
        assert inserted["insert_count"] == len(rows)
        sealed_ids = set()
        if state == "sealed":
            client.flush(collection_name=name, timeout=60)
            sealed_ids = {row["id"] for row in rows}

        indexes = client.prepare_index_params()
        indexes.add_index(
            field_name="content",
            index_type="NGRAM",
            index_name="content_ngram",
            params={"min_gram": min_gram, "max_gram": max_gram},
        )
        assert client.list_indexes(collection_name=name, field_name="content", timeout=60) == []
        confirm_state(client, name, rows, sealed_ids, wait=True)
        client.create_index(collection_name=name, index_params=indexes, sync=False, timeout=60)
        info = client.describe_index(collection_name=name, index_name="content_ngram", timeout=60)
        after = sorted(visible_ids(client, name, ignore_growing=True))
        log.info(f"TEXT NGRAM create collection={name} state_before={state} sealed_ids_after={after} index={info}")
        assert info is not None and info["field_name"] == "content" and info["index_type"] == "NGRAM", info
        assert info["min_gram"] == str(min_gram) and info["max_gram"] == str(max_gram), info
        assert client.list_indexes(collection_name=name, field_name="content", timeout=60) == ["content_ngram"]

    @pytest.mark.tags(CaseLabel.L1)
    def test_text_ngram_rejects_invalid_parameters(self, grep_client, request):
        client = grep_client
        name = f"grep_l1_19_text_params_{uuid4().hex[:12]}"
        request.addfinalizer(lambda: client.drop_collection(collection_name=name, timeout=60))
        schema = client.create_schema(auto_id=False, enable_dynamic_field=False)
        schema.add_field("id", DataType.INT64, is_primary=True)
        schema.add_field("vector", DataType.FLOAT_VECTOR, dim=4)
        schema.add_field("content", DataType.TEXT)
        client.create_collection(
            collection_name=name, schema=schema, shards_num=1, consistency_level="Strong", timeout=60
        )
        inserted = client.insert(
            collection_name=name,
            data=[{"id": 1, "content": "中文🙂 error", "vector": [0.25, 0.5, 0.75, 1.0]}],
            timeout=60,
        )
        assert inserted["insert_count"] == 1

        explicit = "TEXT fields require an explicit NGRAM index"
        missing = "Ngram index must specify both min_gram and max_gram"
        invalid_range = "invalid min_gram or max_gram value for Ngram index"
        cases = [
            ("omitted_type", {"params": {"min_gram": 2, "max_gram": 3}}, 1100, explicit),
            ("empty_type", {"index_type": "", "params": {"min_gram": 2, "max_gram": 3}}, 1100, explicit),
            ("autoindex", {"index_type": "AUTOINDEX"}, 1100, explicit),
            ("inverted", {"index_type": "INVERTED"}, 1100, explicit),
            ("missing_both", {"index_type": "NGRAM"}, 1101, missing),
            ("missing_min", {"index_type": "NGRAM", "params": {"max_gram": 3}}, 1101, missing),
            ("missing_max", {"index_type": "NGRAM", "params": {"min_gram": 2}}, 1101, missing),
            (
                "fractional_min",
                {"index_type": "NGRAM", "params": {"min_gram": 1.5, "max_gram": 3}},
                1100,
                "min_gram for Ngram index must be an integer",
            ),
            (
                "nonnumeric_max",
                {"index_type": "NGRAM", "params": {"min_gram": 2, "max_gram": "abc"}},
                1100,
                "max_gram for Ngram index must be an integer",
            ),
            ("zero_min", {"index_type": "NGRAM", "params": {"min_gram": 0, "max_gram": 3}}, 1100, invalid_range),
            ("zero_max", {"index_type": "NGRAM", "params": {"min_gram": 2, "max_gram": 0}}, 1100, invalid_range),
            ("negative_min", {"index_type": "NGRAM", "params": {"min_gram": -1, "max_gram": 3}}, 1100, invalid_range),
            ("negative_max", {"index_type": "NGRAM", "params": {"min_gram": 2, "max_gram": -1}}, 1100, invalid_range),
            ("reversed", {"index_type": "NGRAM", "params": {"min_gram": 4, "max_gram": 3}}, 1100, invalid_range),
        ]
        for label, options, code, reason in cases:
            assert client.list_indexes(collection_name=name, field_name="content", timeout=60) == []
            indexes = client.prepare_index_params()
            indexes.add_index(field_name="content", index_name="content_ngram", **options)
            try:
                client.create_index(collection_name=name, index_params=indexes, sync=False, timeout=60)
            except MilvusException as error:
                log.info(
                    f"TEXT NGRAM rejected collection={name} case={label} params={options} code={error.code} {error.message}"
                )
                assert not isinstance(error, ParamError), error
                assert error.code == code and reason in error.message, error
                assert error.is_input_error and not error.retriable, error
            else:
                pytest.fail(f"TEXT index creation accepted {label}: {options!r}")
            assert client.list_indexes(collection_name=name, field_name="content", timeout=60) == []

            recovery = client.prepare_index_params()
            recovery.add_index(
                field_name="content",
                index_type="NGRAM",
                index_name="content_ngram",
                params={"min_gram": 2, "max_gram": 3},
            )
            client.create_index(collection_name=name, index_params=recovery, sync=False, timeout=60)
            info = client.describe_index(collection_name=name, index_name="content_ngram", timeout=60)
            log.info(f"TEXT NGRAM recovery collection={name} after={label} index={info}")
            assert info is not None and info["field_name"] == "content" and info["index_type"] == "NGRAM", info
            assert info["min_gram"] == "2" and info["max_gram"] == "3", info
            client.drop_index(collection_name=name, index_name="content_ngram", timeout=60)
        assert client.list_indexes(collection_name=name, field_name="content", timeout=60) == []

    @pytest.mark.tags(CaseLabel.L1)
    def test_create_varchar_ngram_and_autoindex(self, grep_client, request):
        client = grep_client
        name = f"grep_l1_19_varchar_{uuid4().hex[:12]}"
        request.addfinalizer(lambda: client.drop_collection(collection_name=name, timeout=60))
        schema = client.create_schema(auto_id=False, enable_dynamic_field=False)
        schema.add_field("id", DataType.INT64, is_primary=True)
        schema.add_field("vector", DataType.FLOAT_VECTOR, dim=4)
        schema.add_field("content", DataType.VARCHAR, max_length=256)
        client.create_collection(
            collection_name=name, schema=schema, shards_num=1, consistency_level="Strong", timeout=60
        )
        inserted = client.insert(
            collection_name=name,
            data=[{"id": 1, "content": "中文🙂 error", "vector": [0.25, 0.5, 0.75, 1.0]}],
            timeout=60,
        )
        assert inserted["insert_count"] == 1
        client.flush(collection_name=name, timeout=60)

        indexes = client.prepare_index_params()
        indexes.add_index(
            field_name="content",
            index_type="NGRAM",
            index_name="content_ngram",
            params={"min_gram": 2, "max_gram": 3},
        )
        client.create_index(collection_name=name, index_params=indexes, sync=False, timeout=60)
        info = client.describe_index(collection_name=name, index_name="content_ngram", timeout=60)
        log.info(f"VARCHAR NGRAM request collection={name} index={info}")
        assert info is not None and info["field_name"] == "content" and info["index_type"] == "NGRAM", info
        assert info["min_gram"] == "2" and info["max_gram"] == "3", info
        client.drop_index(collection_name=name, index_name="content_ngram", timeout=60)
        assert client.list_indexes(collection_name=name, field_name="content", timeout=60) == []

        indexes = client.prepare_index_params()
        indexes.add_index(field_name="content", index_type="AUTOINDEX", index_name="content_auto")
        client.create_index(collection_name=name, index_params=indexes, sync=False, timeout=60)
        info = client.describe_index(collection_name=name, index_name="content_auto", timeout=60)
        log.info(f"VARCHAR AUTOINDEX request collection={name} index={info}")
        assert info is not None and info["field_name"] == "content" and info["index_type"] == "AUTOINDEX", info
        assert client.list_indexes(collection_name=name, field_name="content", timeout=60) == ["content_auto"]


class TestMilvusClientGrepTextNgramSealedOnly(GrepBase):
    """TC-L1-20. Compare loaded and reloaded TEXT NGRAM against the source and raw scan."""

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize("create_before_flush", [True, False], ids=["create-then-flush", "flush-then-create"])
    def test_text_ngram_load_and_reload(self, grep_client, request, create_before_flush):
        client = grep_client
        prefix = f"grep_l1_20_{uuid4().hex[:12]}"
        raw_name = f"{prefix}_raw"
        indexed_name = f"{prefix}_ngram"
        row_count = 2048
        multiline = "header\n中文🙂 error error\ntrailer"
        long_text = "header\n" + "x" * (70 * 1024) + "\n尾🙂 error"
        samples = [
            (301, "src/ready.py", "error ready"),
            (302, "src/main.py", multiline),
            (303, "src/copy.py", multiline),
            (304, "logs/long.log", long_text),
            (305, "logs/null.log", None),
            (306, "logs/empty.log", ""),
            (307, "logs/near.log", "erro rror"),
        ]
        samples.extend((pk, "logs/filler.log", "ordinary filler") for pk in range(1000, 3041))
        rows = [
            {"id": pk, "path": path, "content": content, "vector": [0.25, 0.5, 0.75, 1.0]}
            for pk, path, content in samples
        ]
        assert len(rows) == row_count
        expected = {
            301: [detail("error ready", 1, 0, [(0, 5)])],
            302: [detail("中文🙂 error error", 2, 7, [(11, 16), (17, 22)])],
            303: [detail("中文🙂 error error", 2, 7, [(11, 16), (17, 22)])],
            304: [detail("尾🙂 error", 3, 71688, [(8, 13)])],
        }
        schema = client.create_schema(auto_id=False, enable_dynamic_field=False)
        schema.add_field("id", DataType.INT64, is_primary=True)
        schema.add_field("vector", DataType.FLOAT_VECTOR, dim=4)
        schema.add_field("content", DataType.TEXT, nullable=True)
        schema.add_field("path", DataType.VARCHAR, max_length=256)
        vector_indexes = client.prepare_index_params()
        vector_indexes.add_index(field_name="vector", index_type="FLAT", metric_type="L2")
        text_indexes = client.prepare_index_params()
        text_indexes.add_index(
            field_name="content",
            index_type="NGRAM",
            index_name="content_ngram",
            params={"min_gram": 3, "max_gram": 3},
        )

        for name in (raw_name, indexed_name):
            client.create_collection(
                collection_name=name, schema=schema, shards_num=1, consistency_level="Strong", timeout=60
            )
            request.addfinalizer(lambda name=name: client.drop_collection(collection_name=name, timeout=60))
            client.create_index(collection_name=name, index_params=vector_indexes, timeout=60)
            if name == indexed_name and create_before_flush:
                # Load the vector-only collection for ordinary queries that verify Growing visibility.
                client.load_collection(collection_name=name, replica_number=1, timeout=180)
            inserted = client.insert(collection_name=name, data=rows, timeout=60)
            assert inserted["insert_count"] == row_count

            if name == indexed_name and create_before_flush:
                visible = client.query(
                    collection_name=name,
                    filter="",
                    output_fields=["id"],
                    limit=2048,
                    consistency_level="Strong",
                    ignore_growing=False,
                    timeout=60,
                )
                assert len(visible) == row_count and {row["id"] for row in visible} == {row["id"] for row in rows}
                sealed_only = client.query(
                    collection_name=name,
                    filter="",
                    output_fields=["id"],
                    limit=2048,
                    consistency_level="Strong",
                    ignore_growing=True,
                    timeout=60,
                )
                if sealed_only:
                    pytest.skip(f"BLOCKED: {name} already had {len(sealed_only)} Sealed rows before creating NGRAM")
                log.info(f"TEXT NGRAM before Flush collection={name} visible={len(visible)} sealed={len(sealed_only)}")
                client.create_index(collection_name=name, index_params=text_indexes, sync=False, timeout=60)
                client.release_collection(collection_name=name, timeout=60)
                assert client.get_load_state(collection_name=name, timeout=60)["state"] == LoadState.NotLoad

            client.flush(collection_name=name, timeout=60)
            flushed = client.list_persistent_segments(
                collection_name=name, states=[ClientSegmentState.Flushed], timeout=60
            )
            flushed_sizes = {segment.segment_id: segment.num_rows for segment in flushed}
            assert sum(flushed_sizes.values()) == row_count, flushed_sizes
            if min(flushed_sizes.values()) <= 1024:
                pytest.skip(f"BLOCKED: expected every flushed segment above 1024 rows on {name}: {flushed_sizes}")
            log.info(f"TEXT NGRAM after Flush collection={name} segments={flushed_sizes}")
            if name == indexed_name and not create_before_flush:
                client.create_index(collection_name=name, index_params=text_indexes, sync=False, timeout=60)

        assert client.list_indexes(collection_name=raw_name, field_name="content", timeout=60) == []
        assert client.list_indexes(collection_name=indexed_name, field_name="content", timeout=60) == ["content_ngram"]
        deadline = time.monotonic() + 180
        while True:
            info = client.describe_index(collection_name=indexed_name, index_name="content_ngram", timeout=10)
            assert info is not None and info["field_name"] == "content" and info["index_type"] == "NGRAM", info
            assert (info["min_gram"], info["max_gram"]) == ("3", "3"), info
            assert info["state"] != "Failed", info
            if (
                info["state"] == "Finished"
                and info["total_rows"] == info["indexed_rows"] == row_count
                and info["pending_index_rows"] == 0
            ):
                break
            assert time.monotonic() < deadline, info
            time.sleep(1)
        log.info(f"TEXT NGRAM build finished collection={indexed_name} index={info}")

        fields = ["id", "path", "content"]
        results = {}
        for stage, name in (("scan", raw_name), ("indexed", indexed_name), ("reloaded", indexed_name)):
            if stage == "reloaded":
                client.release_collection(collection_name=name, timeout=60)
                deadline = time.monotonic() + 60
                while True:
                    load_state = client.get_load_state(collection_name=name, timeout=10)
                    if load_state["state"] == LoadState.NotLoad:
                        break
                    assert time.monotonic() < deadline, load_state
                    time.sleep(1)
                log.info(f"TEXT NGRAM collection released collection={name} state={load_state}")

            client.load_collection(collection_name=name, replica_number=1, timeout=180)
            load_state = client.get_load_state(collection_name=name, timeout=60)
            assert load_state["state"] == LoadState.Loaded, load_state
            loaded = client.list_loaded_segments(collection_name=name, timeout=60)
            sealed = {segment.segment_id: segment for segment in loaded if segment.state == SegmentState.Sealed}
            sealed_sizes = {segment_id: segment.num_rows for segment_id, segment in sealed.items()}
            assert sum(sealed_sizes.values()) == row_count, sealed_sizes
            assert min(sealed_sizes.values()) > 1024, sealed_sizes
            collection_id = next(iter(sealed.values())).collection_id
            log.info(
                f"TEXT NGRAM collection loaded stage={stage} collection={name} collection_id={collection_id} "
                f"state={load_state} sealed_segments={sealed_sizes}"
            )

            actual = client.grep(
                collection_name=name,
                text_field="content",
                args="-F -e error",
                filter="",
                output_fields=fields,
                limit=100,
                offset=0,
                match_field="matches",
                consistency_level="Strong",
                timeout=60,
            )
            results[stage] = assert_results(actual, rows, expected, fields)
            hit_summary = {
                pk: {
                    "path": row["path"],
                    "content_bytes": len(row["content"].encode("utf-8")),
                    "matches": row["matches"],
                }
                for pk, row in results[stage].items()
            }
            log.info(f"TEXT NGRAM Grep result stage={stage} collection={name} hits={hit_summary}")

        assert results["indexed"] == results["scan"]
        assert results["reloaded"] == results["indexed"]


class TestMilvusClientGrepExternalIndependent(GrepBase):
    """When to add tests here: Grep on an isolated external source.

    Fixture: grep_client, one private Parquet object per scenario.
    """

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize("dtype", [DataType.VARCHAR, DataType.TEXT], ids=["varchar", "text"])
    def test_external_parquet_grep_and_reload(self, grep_client, request, dtype):
        """TC-L1-23.
        target: Grep mapped text, filters and returned fields after load and reload.
        method: Refresh a Parquet-backed collection once and run literal and line-start searches.
        expected: Exact source rows, virtual primary keys and UTF-8 match details.
        """
        import pyarrow as pa
        import pyarrow.parquet as pq
        from common.external_table_common import (
            build_external_source,
            build_external_spec,
            get_minio_config,
            new_minio_client,
            upload_parquet,
        )

        client = grep_client
        name = f"grep_l1_23_{dtype.name.lower()}_{uuid4().hex[:12]}"
        repeated = "header\nerror 中文🙂"
        documents = [
            ("doc-a", "demo", "src/a.py", "r1", repeated),
            ("doc-b", "demo", "src/b.py", "r2", repeated),
            ("doc-repo", "other", "src/c.py", "r3", "error outside repo"),
            ("doc-path", "demo", "docs/d.md", "r4", "error outside path"),
            ("doc-null", "demo", "src/null.py", "r5", None),
            ("doc-empty", "demo", "src/empty.py", "r6", ""),
            ("doc-inline", "demo", "src/inline.py", "r7", "中文🙂 error only"),
            ("doc-ok", "demo", "src/ok.py", "r8", "all good"),
        ]
        if dtype == DataType.TEXT:
            long_text = "header\n" + "x" * (70 * 1024) + "\nerror 尾🙂"
            documents.append(("doc-long", "demo", "src/long.log", "r9", long_text))
        source_by_doc = {
            key: {"doc_key": key, "repo": repo, "path": path, "revision": revision, "content": content}
            for key, repo, path, revision, content in documents
        }
        anchored = {
            "doc-a": [detail("error 中文🙂", 2, 7, [(0, 5)])],
            "doc-b": [detail("error 中文🙂", 2, 7, [(0, 5)])],
            "doc-repo": [detail("error outside repo", 1, 0, [(0, 5)])],
            "doc-path": [detail("error outside path", 1, 0, [(0, 5)])],
        }
        if dtype == DataType.TEXT:
            anchored["doc-long"] = [detail("error 尾🙂", 3, 71688, [(0, 5)])]
        literal = {**anchored, "doc-inline": [detail("中文🙂 error only", 1, 0, [(11, 16)])]}
        matches_filter = {"doc-a", "doc-b", "doc-inline", "doc-long"}
        repo_path_filter = 'repo == "demo" and path like "src/%"'
        cases = [
            ("literal_all", "-F -e error", "", literal),
            (
                "literal_filtered",
                "-F -e error",
                repo_path_filter,
                {key: hits for key, hits in literal.items() if key in matches_filter},
            ),
            ("line_start_all", '-e "^error"', "", anchored),
            (
                "line_start_filtered",
                '-e "^error"',
                repo_path_filter,
                {key: hits for key, hits in anchored.items() if key in matches_filter},
            ),
        ]

        table = pa.table(
            {
                "src_key": pa.array([doc[0] for doc in documents], type=pa.string()),
                "src_repo": pa.array([doc[1] for doc in documents], type=pa.string()),
                "src_path": pa.array([doc[2] for doc in documents], type=pa.string()),
                "src_revision": pa.array([doc[3] for doc in documents], type=pa.string()),
                "src_text": pa.array([doc[4] for doc in documents], type=pa.string()),
                "src_embedding": pa.array([[0.25, 0.5, 0.75, 1.0]] * len(documents), type=pa.list_(pa.float32(), 4)),
            }
        )
        parquet = io.BytesIO()
        pq.write_table(table, parquet, compression="snappy")
        parquet_bytes = parquet.getvalue()
        cfg = get_minio_config(
            minio_host=request.config.getoption("--minio_host"),
            minio_bucket=request.config.getoption("--minio_bucket"),
        )
        cfg["access_key"] = os.getenv("MILVUS_EXTERNAL_SOURCE_MINIO_ACCESS_KEY", cfg["access_key"])
        cfg["secret_key"] = os.getenv("MILVUS_EXTERNAL_SOURCE_MINIO_SECRET_KEY", cfg["secret_key"])
        minio = new_minio_client(cfg)
        prefix = f"grep-e2e/{name}"
        key = f"{prefix}/data.parquet"
        request.addfinalizer(lambda: minio.remove_object(cfg["bucket"], key))
        upload_parquet(minio, cfg["bucket"], key, parquet_bytes)
        log.info(
            f"External Grep source collection={name} object={key} rows={len(documents)} "
            f"bytes={len(parquet_bytes)} sha256={hashlib.sha256(parquet_bytes).hexdigest()}"
        )

        schema = client.create_schema(
            auto_id=False,
            enable_dynamic_field=False,
            external_source=build_external_source(cfg, prefix),
            external_spec=build_external_spec(cfg, fmt="parquet"),
        )
        schema.add_field("doc_key", DataType.VARCHAR, max_length=64, external_field="src_key")
        schema.add_field("repo", DataType.VARCHAR, max_length=64, external_field="src_repo")
        schema.add_field("path", DataType.VARCHAR, max_length=256, external_field="src_path")
        schema.add_field("revision", DataType.VARCHAR, max_length=64, external_field="src_revision")
        if dtype == DataType.VARCHAR:
            schema.add_field("content", DataType.VARCHAR, max_length=512, nullable=True, external_field="src_text")
        else:
            schema.add_field("content", DataType.TEXT, nullable=True, external_field="src_text")
        schema.add_field("embedding", DataType.FLOAT_VECTOR, dim=4, external_field="src_embedding")
        client.create_collection(
            collection_name=name, schema=schema, shards_num=1, consistency_level="Strong", timeout=60
        )
        request.addfinalizer(lambda: client.drop_collection(collection_name=name, timeout=60))
        job_id = client.refresh_external_collection(collection_name=name, timeout=60)
        deadline = time.monotonic() + 180
        while True:
            refresh = client.get_refresh_external_collection_progress(job_id=job_id, timeout=10)
            assert refresh.state != "RefreshFailed", (job_id, refresh.reason)
            if refresh.state == "RefreshCompleted":
                break
            assert time.monotonic() < deadline, (job_id, refresh.state, refresh.progress)
            time.sleep(1)
        log.info(f"External Grep refreshed collection={name} job={job_id} state={refresh.state}")
        indexes = client.prepare_index_params()
        indexes.add_index(field_name="embedding", index_type="FLAT", metric_type="L2")
        client.create_index(collection_name=name, index_params=indexes, timeout=120)

        fields = ["__virtual_pk__", "doc_key", "repo", "path", "revision", "content"]
        initial_primary_keys = None
        for stage in ("loaded", "reloaded"):
            if stage == "reloaded":
                client.release_collection(collection_name=name, timeout=60)
                deadline = time.monotonic() + 60
                while True:
                    load = client.get_load_state(collection_name=name, timeout=10)
                    if load["state"] == LoadState.NotLoad:
                        break
                    assert time.monotonic() < deadline, load
                    time.sleep(1)
            client.load_collection(collection_name=name, replica_number=1, timeout=120)
            assert client.get_load_state(collection_name=name, timeout=60)["state"] == LoadState.Loaded
            ordinary = client.query(
                collection_name=name, filter="", output_fields=fields, limit=100, consistency_level="Strong", timeout=60
            )
            primary_keys = {row["doc_key"]: row["__virtual_pk__"] for row in ordinary}
            assert len(ordinary) == len(primary_keys) == len(set(primary_keys.values())) == len(documents)
            assert set(primary_keys) == set(source_by_doc)
            assert all(type(pk) is int for pk in primary_keys.values())
            for row in ordinary:
                reference = {"__virtual_pk__": primary_keys[row["doc_key"]], **source_by_doc[row["doc_key"]]}
                assert dict(row) == reference
            if stage == "loaded":
                initial_primary_keys = primary_keys
            else:
                assert primary_keys == initial_primary_keys
            log.info(f"External Grep baseline collection={name} stage={stage} primary_keys={primary_keys}")

            for label, args, filter_expr, expected in cases:
                result = client.grep(
                    collection_name=name,
                    text_field="content",
                    args=args,
                    filter=filter_expr,
                    output_fields=fields,
                    limit=100,
                    offset=0,
                    match_field="matches",
                    consistency_level="Strong",
                    timeout=60,
                )
                actual = {row["doc_key"]: dict(row) for row in result}
                assert len(result) == len(actual)
                assert set(actual) == set(expected), (dtype, stage, label, set(actual), set(expected))
                for doc_key, matches in expected.items():
                    reference = {"__virtual_pk__": primary_keys[doc_key], **source_by_doc[doc_key], "matches": matches}
                    assert actual[doc_key] == reference, (stage, label, doc_key)
                evidence = {
                    doc_key: {
                        "pk": row["__virtual_pk__"],
                        "path": row["path"],
                        "content_bytes": len(row["content"].encode("utf-8")),
                        "matches": row["matches"],
                    }
                    for doc_key, row in actual.items()
                }
                log.info(f"External Grep result collection={name} stage={stage} case={label} hits={evidence}")


class TestMilvusClientGrepRepeatedIndependent(GrepBase):
    """When to add tests here: bounded sequential/concurrent request independence.

    Fixture: grep_client, isolated static collections; no general mixed workload.
    """

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.asyncio
    @pytest.mark.parametrize("string_pk", [False, True], ids=["int64-pk", "varchar-pk"])
    async def test_repeated_queries_and_sdk_decoding(self, grep_client, request, string_pk):
        """TC-L2-01.
        target: Old response objects survive later requests and collection reload.
        method: Two PK types, complex projections and representative sync/async decoding.
        expected: Source rows and every detail remain unchanged and independently correct.
        """
        client = grep_client
        name = f"grep_repeated_{uuid4().hex[:12]}"
        fields = FIELDS + ["meta", "tags", "score", "vector"]
        rows = [{field: row[field] for field in fields} for row in projected_rows()]
        rows[1]["content"] = "中文 error error"
        for pk in range(400, 416):
            rows.append(
                {
                    **source_row(pk, "ordinary filler"),
                    "meta": {"text": "filler", "pk": pk},
                    "tags": ["filler", str(pk)],
                    "score": pk,
                    "vector": [pk + 0.25, pk + 0.5, pk + 0.75, pk + 1.0],
                }
            )
        if string_pk:
            for row in rows:
                row["id"] = str(row["id"])
        # Keep the three matching rows in distinct write batches.
        random.Random(77).shuffle(rows)
        first_batch, second_batch, third_batch = rows[:6], rows[6:12], rows[12:]

        def cleanup():
            if client.has_collection(collection_name=name, timeout=60):
                client.drop_collection(collection_name=name, timeout=60)

        request.addfinalizer(cleanup)
        schema = client.create_schema(auto_id=False, enable_dynamic_field=False)
        if string_pk:
            schema.add_field("id", DataType.VARCHAR, is_primary=True, max_length=64)
        else:
            schema.add_field("id", DataType.INT64, is_primary=True)
        schema.add_field("content", DataType.VARCHAR, max_length=65535, nullable=True)
        for field in ("repo", "path", "revision"):
            schema.add_field(field, DataType.VARCHAR, max_length=256)
        schema.add_field("meta", DataType.JSON)
        schema.add_field("tags", DataType.ARRAY, element_type=DataType.VARCHAR, max_capacity=8, max_length=64)
        schema.add_field("score", DataType.INT64, nullable=True)
        schema.add_field("vector", DataType.FLOAT_VECTOR, dim=4)
        indexes = client.prepare_index_params()
        indexes.add_index(field_name="vector", index_type="FLAT", metric_type="L2")
        client.create_collection(
            collection_name=name, schema=schema, shards_num=1, consistency_level="Strong", timeout=60
        )
        client.create_index(collection_name=name, index_params=indexes, timeout=60)
        client.load_collection(collection_name=name, replica_number=1, timeout=120)
        client.insert(collection_name=name, data=first_batch, timeout=60)
        client.flush(collection_name=name, timeout=60)
        client.insert(collection_name=name, data=second_batch, timeout=60)
        client.flush(collection_name=name, timeout=60)
        client.insert(collection_name=name, data=third_batch, timeout=60)

        ordinary = client.query(
            collection_name=name,
            filter="",
            output_fields=fields,
            limit=100,
            offset=0,
            consistency_level="Strong",
            timeout=60,
        )
        assert_results(ordinary, rows, {row["id"]: [] for row in rows}, fields, match_field=None)
        patterns = ("error", "info", "__GREP_NO_MATCH__")
        expected_by_pattern = {pattern: expected_literal(rows, pattern) for pattern in patterns}
        expected_error = expected_by_pattern["error"]

        grep_raw = client.grep(
            collection_name=name,
            text_field="content",
            args="-F -e error",
            filter="",
            output_fields=fields,
            limit=100,
            offset=0,
            consistency_level="Strong",
            timeout=60,
        )
        grep_snapshot = deepcopy(assert_results(grep_raw, rows, expected_error, fields))

        def assert_retained(checkpoint):
            assert assert_results(grep_raw, rows, expected_error, fields) == grep_snapshot, checkpoint
            log.info(f"Repeated Grep original response verified collection={name} checkpoint={checkpoint}")

        log.info(f"Repeated Grep initial collection={name} rows={grep_snapshot}")
        for round_number in range(1, 7):
            if round_number % 2:
                projection = fields[::-1]
                match_field = "text_hits"
            else:
                projection = ["id", "tags", "content", "meta"]
                match_field = "matches"
            for pattern in patterns:
                result = client.grep(
                    collection_name=name,
                    text_field="content",
                    args=f"-F -e {pattern}",
                    filter="",
                    output_fields=projection,
                    limit=100,
                    offset=0,
                    match_field=match_field,
                    consistency_level="Strong",
                    timeout=60,
                )
                actual = assert_results(result, rows, expected_by_pattern[pattern], projection, match_field=match_field)
                log.info(f"Repeated Grep round={round_number} pattern={pattern} field={match_field} rows={actual}")
                assert_retained(f"round {round_number}, {pattern}")
            page = client.grep(
                collection_name=name,
                text_field="content",
                args="-F -e error",
                filter="",
                output_fields=fields,
                limit=1,
                offset=1,
                match_field="matches",
                consistency_level="Strong",
                timeout=60,
            )
            actual = assert_results(page, rows, expected_error, fields, count=1)
            log.info(f"Repeated Grep round={round_number} limit=1 offset=1 rows={actual}")
            assert_retained(f"round {round_number}, page")

        client.release_collection(collection_name=name, timeout=60)
        deadline = time.monotonic() + 60
        while True:
            load_state = client.get_load_state(collection_name=name, timeout=10)
            if load_state["state"] == LoadState.NotLoad:
                break
            assert time.monotonic() < deadline, load_state
            time.sleep(0.5)
        assert_retained("after release")
        client.load_collection(collection_name=name, replica_number=1, timeout=120)
        assert client.get_load_state(collection_name=name, timeout=60)["state"] == LoadState.Loaded
        assert_retained("after reload, before fresh Grep")
        fresh = client.grep(
            collection_name=name,
            text_field="content",
            args="-F -e error",
            filter="",
            output_fields=fields,
            limit=100,
            offset=0,
            match_field="matches",
            consistency_level="Strong",
            timeout=60,
        )
        actual = assert_results(fresh, rows, expected_error, fields)
        assert actual == grep_snapshot
        log.info(f"Repeated Grep fresh after reload collection={name} rows={actual}")
        assert_retained("after fresh Grep")

        self.init_async_milvus_client()
        async_client = self.async_milvus_client_wrap.async_milvus_client
        try:
            for match_field in ("matches", "text_hits"):
                result = await async_client.grep(
                    collection_name=name,
                    text_field="content",
                    args="-F -e error",
                    filter="",
                    output_fields=fields,
                    limit=100,
                    offset=0,
                    match_field=match_field,
                    consistency_level="Strong",
                    timeout=60,
                )
                actual = assert_results(result, rows, expected_error, fields, match_field=match_field)
                log.info(f"Repeated Grep async collection={name} field={match_field} rows={actual}")
        finally:
            await async_client.close()

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize("workers", [4, 8])
    def test_concurrent_grep_independent_results(self, grep_client, request, workers):
        """TC-L2-14. Verify four different Grep requests over 20 concurrent rounds."""
        name = f"grep_l2_14_{workers}_{uuid4().hex[:12]}"
        fields = ["id", "content", "repo", "path", "revision"]
        rows = [
            source_row(101, "error alpha", repo="demo-a"),
            source_row(102, "info\nERROR beta", repo="demo-a"),
            source_row(201, "warning gamma", repo="demo-b"),
            source_row(202, "info\nerror delta", repo="demo-b"),
            source_row(301, "info only", repo="demo-a"),
        ]
        schema = grep_client.create_schema(auto_id=False, enable_dynamic_field=False)
        schema.add_field("id", DataType.INT64, is_primary=True)
        schema.add_field("vector", DataType.FLOAT_VECTOR, dim=4)
        schema.add_field("content", DataType.VARCHAR, max_length=65535)
        for field in ("repo", "path", "revision"):
            schema.add_field(field, DataType.VARCHAR, max_length=256)
        indexes = grep_client.prepare_index_params()
        indexes.add_index(field_name="vector", index_type="FLAT", metric_type="L2")

        def cleanup():
            if grep_client.has_collection(collection_name=name, timeout=60):
                grep_client.drop_collection(collection_name=name, timeout=60)

        request.addfinalizer(cleanup)
        grep_client.create_collection(
            collection_name=name,
            schema=schema,
            shards_num=1,
            consistency_level="Strong",
            timeout=60,
        )
        inserted = grep_client.insert(collection_name=name, data=rows, timeout=60)
        assert inserted["insert_count"] == len(rows)
        grep_client.flush(collection_name=name, timeout=60)
        grep_client.create_index(collection_name=name, index_params=indexes, timeout=120)
        grep_client.load_collection(collection_name=name, replica_number=1, timeout=120)
        assert grep_client.get_load_state(collection_name=name, timeout=60)["state"] == LoadState.Loaded

        hits = {
            101: [detail("error alpha", 1, 0, [(0, 5)])],
            102: [detail("ERROR beta", 2, 5, [(0, 5)])],
            201: [detail("warning gamma", 1, 0, [(0, 7)])],
            202: [detail("error delta", 2, 5, [(0, 5)])],
        }
        groups = [
            ("A", 'repo == "demo-a"', "-F -e error", {101: hits[101]}),
            ("B", 'repo == "demo-b"', "-F -e warning", {201: hits[201]}),
            ("C", "", "-i -F -e error", {pk: hits[pk] for pk in (101, 102, 202)}),
            ("D", "", "-F -e __GREP_NO_MATCH__", {}),
        ]

        def serial_check(phase):
            for group, expr, args, expected in groups:
                result = grep_client.grep(
                    collection_name=name,
                    text_field="content",
                    args=args,
                    filter=expr,
                    output_fields=fields,
                    limit=100,
                    offset=0,
                    match_field="matches",
                    consistency_level="Strong",
                    timeout=60,
                )
                actual = assert_results(result, rows, expected, fields)
                log.info(f"Grep serial phase={phase} workers={workers} group={group} rows={actual}")

        serial_check("before")
        clients = []
        observations = []
        barrier = threading.Barrier(workers, timeout=90)
        try:
            for _ in range(workers):
                clients.append(new_client())

            def worker(index):
                client = clients[index]
                responses = []
                try:
                    for round_number in range(20):
                        group, expr, args, expected = groups[(index + round_number) % len(groups)]
                        barrier.wait()
                        start = time.monotonic()
                        result = client.grep(
                            collection_name=name,
                            text_field="content",
                            args=args,
                            filter=expr,
                            output_fields=fields,
                            limit=100,
                            offset=0,
                            match_field="matches",
                            consistency_level="Strong",
                            timeout=60,
                        )
                        end = time.monotonic()
                        actual = assert_results(result, rows, expected, fields)
                        responses.append(
                            {
                                "worker": index,
                                "round": round_number,
                                "group": group,
                                "args": args,
                                "filter": expr,
                                "start": start,
                                "end": end,
                                "rows": list(actual.values()),
                            }
                        )
                except Exception:
                    barrier.abort()
                    raise
                return responses

            with ThreadPoolExecutor(max_workers=workers) as pool:
                futures = [pool.submit(worker, index) for index in range(workers)]
                for future in futures:
                    observations.extend(future.result())
        finally:
            for client in clients:
                client.close()
        assert len(observations) == workers * 20
        for observation in observations:
            log.info(f"Grep concurrent request workers={workers} {json.dumps(observation)}")
        overlapping_groups = {
            a["group"]
            for a in observations
            if any(
                a["round"] == b["round"]
                and a["group"] != b["group"]
                and a["start"] < b["end"]
                and b["start"] < a["end"]
                for b in observations
            )
        }
        assert overlapping_groups == {"A", "B", "C", "D"}, overlapping_groups
        log.info(f"Grep concurrent overlap workers={workers} groups={sorted(overlapping_groups)}")
        serial_check("after")


class TestMilvusClientGrepDuplicatesSealedAndGrowing(GrepBase):
    """Check duplicate source text under distinct primary keys across collection states."""

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize("dtype", [DataType.VARCHAR, DataType.TEXT], ids=["VARCHAR", "TEXT"])
    def test_duplicate_text_and_sparse_filters(self, grep_client, request, dtype):
        """TC-L2-06. Compare Growing and two-partition Sealed results with unique row identities."""
        client = grep_client
        prefix = f"grep_l2_06_{dtype.name.lower()}_{uuid4().hex[:12]}"
        samples = {
            "left": [
                source_row(101, "error", path="src/a.py"),
                source_row(309, "error", repo="other", path="src/c.py"),
                source_row(511, "info only", path="src/e.py"),
                source_row(601, "prefix\nerror", repo="controls"),
            ],
            "right": [
                source_row(205, "error", path="src/b.py"),
                source_row(410, "error", path="docs/readme.md"),
                source_row(602, "中文 error error", repo="controls"),
            ],
        }
        matched = {pk: [detail("error", 1, 0, [(0, 5)])] for pk in (101, 205, 309, 410)}
        matched[601] = [detail("error", 2, 7, [(0, 5)])]
        matched[602] = [detail("中文 error error", 1, 0, [(7, 12), (13, 18)])]
        cases = (
            ("all", "", 100, 0, {101, 205, 309, 410, 601, 602}, None),
            ("duplicates", "id < 600", 100, 0, {101, 205, 309, 410}, None),
            ("repo_path", 'repo == "demo" and path like "src/%"', 100, 0, {101, 205}, None),
            ("ids", "id in [101,309,511]", 100, 0, {101, 309}, None),
            ("range", "id >= 200 and id < 400", 100, 0, {205, 309}, None),
            ("page", "", 2, 1, {101, 205, 309, 410, 601, 602}, 2),
        )

        schema = client.create_schema(auto_id=False, enable_dynamic_field=False)
        schema.add_field("id", DataType.INT64, is_primary=True)
        schema.add_field("vector", DataType.FLOAT_VECTOR, dim=4)
        if dtype == DataType.VARCHAR:
            schema.add_field("content", DataType.VARCHAR, max_length=65535)
        else:
            schema.add_field("content", DataType.TEXT)
        for field in ("repo", "path", "revision"):
            schema.add_field(field, DataType.VARCHAR, max_length=256)
        vector_index = client.prepare_index_params()
        vector_index.add_index(field_name="vector", index_type="FLAT", metric_type="L2")

        full_results = {}
        for state in ("growing", "sealed", "sealed_ngram"):
            name = f"{prefix}_{state}"
            client.create_collection(
                collection_name=name,
                schema=schema,
                shards_num=1,
                consistency_level="Strong",
                timeout=60,
            )
            request.addfinalizer(lambda name=name: client.drop_collection(collection_name=name, timeout=60))
            for partition in samples:
                client.create_partition(collection_name=name, partition_name=partition, timeout=60)
            client.create_index(collection_name=name, index_params=vector_index, timeout=180)
            if state == "growing":
                client.load_collection(collection_name=name, replica_number=1, timeout=180)

            rows = []
            for partition, filler_start in (("left", 10000), ("right", 20000)):
                fixed_rows = samples[partition]
                filler_count = 250 if state == "growing" else 2048 - len(fixed_rows)
                batch = fixed_rows + [
                    source_row(pk, "ordinary text") for pk in range(filler_start, filler_start + filler_count)
                ]
                random.Random(20261111).shuffle(batch)
                inserted = client.insert(collection_name=name, partition_name=partition, data=batch, timeout=60)
                assert inserted["insert_count"] == len(batch)
                rows.extend(batch)
            row_count = 507 if state == "growing" else 4096
            assert len(rows) == len({row["id"] for row in rows}) == row_count

            info = None
            if state != "growing":
                client.flush(collection_name=name, timeout=60)
                if state == "sealed_ngram":
                    ngram = client.prepare_index_params()
                    ngram.add_index(
                        field_name="content",
                        index_type="NGRAM",
                        index_name="content_ngram",
                        params={"min_gram": 3, "max_gram": 3},
                    )
                    client.create_index(collection_name=name, index_params=ngram, sync=False, timeout=60)
                    deadline = time.monotonic() + 120
                    while True:
                        info = client.describe_index(collection_name=name, index_name="content_ngram", timeout=10)
                        assert info and info["field_name"] == "content" and info["index_type"] == "NGRAM", info
                        assert info["min_gram"] == info["max_gram"] == "3", info
                        assert info["state"] != "Failed", info
                        if (
                            info["state"] == "Finished"
                            and info["total_rows"] == info["indexed_rows"] == 4096
                            and info["pending_index_rows"] == 0
                        ):
                            break
                        assert time.monotonic() < deadline, info
                        time.sleep(1)
                client.load_collection(collection_name=name, replica_number=1, timeout=180)

            load_state = client.get_load_state(collection_name=name, timeout=60)
            assert load_state["state"] == LoadState.Loaded, load_state
            content_indexes = client.list_indexes(collection_name=name, field_name="content", timeout=60)
            assert content_indexes == (["content_ngram"] if state == "sealed_ngram" else []), content_indexes
            if state == "growing":
                visible = client.grep(
                    collection_name=name,
                    text_field="content",
                    args='-e "^."',
                    filter="",
                    output_fields=["id"],
                    limit=507,
                    offset=0,
                    match_field="matches",
                    consistency_level="Strong",
                    timeout=60,
                )
                assert len(visible) == row_count and {row["id"] for row in visible} == {row["id"] for row in rows}
                sealed = [
                    segment.segment_id
                    for segment in client.list_loaded_segments(collection_name=name, timeout=60)
                    if segment.state == SegmentState.Sealed
                ]
                flushed = client.list_persistent_segments(
                    collection_name=name,
                    states=[ClientSegmentState.Flushed],
                    timeout=60,
                )
                if sealed or flushed:
                    pytest.skip(
                        f"BLOCKED: Growing data already sealed collection={name} sealed={sealed} flushed={flushed}"
                    )
                log.info(f"Grep duplicate text ready collection={name} state={state} visible=507 sealed=0")
            else:
                deadline = time.monotonic() + 60
                while True:
                    sealed = {
                        segment.segment_id: segment
                        for segment in client.list_loaded_segments(collection_name=name, timeout=60)
                        if segment.state == SegmentState.Sealed
                    }
                    flushed = {
                        segment.segment_id
                        for segment in client.list_persistent_segments(
                            collection_name=name,
                            states=[ClientSegmentState.Flushed],
                            timeout=60,
                        )
                    }
                    partition_rows = {}
                    for segment in sealed.values():
                        partition_id = segment.partition_id
                        partition_rows[partition_id] = partition_rows.get(partition_id, 0) + segment.num_rows
                    if set(sealed) <= flushed and sorted(partition_rows.values()) == [2048, 2048]:
                        break
                    if time.monotonic() >= deadline:
                        pytest.skip(
                            f"BLOCKED: expected 2048 loaded Sealed rows in each partition collection={name} "
                            f"partition_rows={partition_rows} sealed={sorted(sealed)} flushed={sorted(flushed)}"
                        )
                    time.sleep(1)
                if state == "sealed_ngram" and min(segment.num_rows for segment in sealed.values()) <= 1024:
                    pytest.skip(f"BLOCKED: loaded Sealed segments too small for NGRAM collection={name}: {sealed}")
                collection_id = next(iter(sealed.values())).collection_id
                layout = {sid: (s.partition_id, s.num_rows) for sid, s in sealed.items()}
                log.info(
                    f"Grep duplicate text ready collection={name} state={state} collection_id={collection_id} "
                    f"sealed_segments={layout} index={info}"
                )

            full_results[state] = {}
            for label, filter_expr, limit, offset, ids, count in cases:
                expected = {pk: matched[pk] for pk in ids}
                actual = client.grep(
                    collection_name=name,
                    text_field="content",
                    args="-F -e error",
                    filter=filter_expr,
                    output_fields=FIELDS,
                    limit=limit,
                    offset=offset,
                    match_field="matches",
                    consistency_level="Strong",
                    timeout=60,
                )
                by_pk = assert_results(actual, rows, expected, FIELDS, count=count)
                log.info(
                    f"Grep duplicate text result collection={name} state={state} case={label} "
                    f"filter={filter_expr!r} limit={limit} offset={offset} rows={by_pk!r}"
                )
                if count is None:
                    full_results[state][label] = by_pk
        assert full_results["growing"] == full_results["sealed"] == full_results["sealed_ngram"]

"""T03/T09 native SDK coverage against two live Proxy endpoints."""

from time import monotonic, sleep
from uuid import uuid4

import pytest
from running_requests_l1_support import (
    assert_search_baseline,
)
from test_running_requests_native_sdk import canceled, listed, start, terminal

pytestmark = pytest.mark.tags("L1")


def snapshot(worker, search_data, jobs):
    deadline = monotonic() + 30
    while monotonic() < deadline:
        rows = [
            row for row in listed(worker, Collection=search_data["Collection"], User=worker.user) if row["expr"] in jobs
        ]
        assert len(rows) == len({row["expr"] for row in rows}), rows
        if len(rows) == len(jobs):
            assert len({row["request_id"] for row in rows}) == len(jobs), rows
            return {row["expr"]: row for row in rows}
        for call in jobs.values():
            assert not worker.ok("poll", Call=call)["done"], "Search ended before the shared List snapshot"
        sleep(0.02)
    pytest.fail("List did not observe all target and control requests together")


def test_native_t09_batch_across_proxies(native, data, deployment, workload_baseline):
    """Cancel one target on each Proxy and keep both concurrent controls alive."""
    uris, _, _, _ = deployment
    targets, controls = {}, {}
    for client, proxy_id in zip(("a", "b"), uris):
        call, expr = start(native, data, client=client)
        controls[expr] = (call, proxy_id)
    for client, proxy_id in zip(("a", "b"), uris):
        call, expr = start(native, dict(data, NQ=data["NQ"] - 1), client=client)
        targets[expr] = (call, proxy_id)

    all_jobs = targets | controls
    rows = snapshot(native, data, {expr: call for expr, (call, _) in all_jobs.items()})
    for expr, (_, proxy_id) in all_jobs.items():
        row = rows[expr]
        assert row["proxy_id"] == proxy_id and row["type"] == "Search" and row["cancellable"], row
        assert row["nq"] == data["NQ"] - int(expr in targets), row
        assert row["topk"] == data["Limit"], row

    target_ids = [rows[expr]["request_id"] for expr in targets]
    reason = "T09-native-" + uuid4().hex
    canceled(native, target_ids, reason)
    control_ids = {rows[expr]["request_id"] for expr in controls}
    active = {row["request_id"] for row in listed(native, Collection=data["Collection"])}
    for call, _ in targets.values():
        terminal(native, call, reason)
    baseline = workload_baseline()
    for call, _ in controls.values():
        outcome = native.ok("await", Call=call)
        assert_search_baseline(outcome["hits"], baseline, data["NQ"])
    remaining = {row["request_id"] for row in listed(native, Collection=data["Collection"])}
    assert not (set(target_ids) | control_ids) & remaining
    assert control_ids <= active, "Control searches finished before overlap with Cancel could be verified"


def test_native_t03_list_two_same_name_collections(native, data, deployment, second_database):
    """An explicit database filter must separate two live searches by ID."""
    uris, token, _, _ = deployment
    database, _ = second_database
    native.ok("connect", "other-db", URI=next(iter(uris.values())), Token=token, DB=database)
    default_call, default_expr = start(native, data)
    other_call, other_expr = start(native, data, client="other-db")
    rows = snapshot(native, data, {default_expr: default_call, other_expr: other_call})
    ids = {}
    for db, expr in (("default", default_expr), (database, other_expr)):
        row = rows[expr]
        assert row["db_name"] == db and row["proxy_id"] == native.owner, row
        assert row["type"] == "Search" and row["cancellable"], row
        ids[db] = row["request_id"]

    for db, request_id in ids.items():
        filtered = listed(native, DB=db, Collection=data["Collection"], User=native.user)
        matched = {row["request_id"] for row in filtered} & set(ids.values())
        assert matched == {request_id}, filtered
    reason = "T03-native-" + uuid4().hex
    canceled(native, list(ids.values()), reason)
    for call in (default_call, other_call):
        terminal(native, call, reason)
    remaining = {row["request_id"] for row in listed(native, Collection=data["Collection"])}
    assert not set(ids.values()) & remaining

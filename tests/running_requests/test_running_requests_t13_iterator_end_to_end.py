"""Cancel a real page RPC from the Java and Go native iterators."""

from contextlib import ExitStack
from time import monotonic, sleep
from uuid import uuid4

import numpy as np
import pytest
from test_running_requests_native_sdk import canceled, listed, terminal
from test_running_requests_t13_extended import iterator_finish_checkpoint

pytestmark = pytest.mark.tags("L1")


@pytest.mark.parametrize("kind", ["Search", "Query"])
def test_t13_native_iterator_batch_cancel(kind, native, workload, tmp_path):
    _, _, collection, vectors = workload
    vector_file = tmp_path / "iterator-vector.f32"
    np.asarray(vectors[:1], dtype="<f4").tofile(vector_file)
    page_size = 8192 if kind == "Search" else 16384
    base = dict(
        Kind=kind, Collection=collection, Vectors=str(vector_file), NQ=1, BatchSize=page_size, Limit=page_size * 3
    )
    opened = []
    outstanding = None

    def open_iterator(client):
        iterator = uuid4().hex
        expr = f"id >= -{int(iterator[:7], 16) + 1}"
        native.ok("iterator-open", client, **base, Call=iterator, Expr=expr)
        opened.append(iterator)
        return iterator, expr

    def submit_page(iterator):
        call = uuid4().hex
        native.ok("iterator-next", Call=call, RequestID=iterator, Kind=kind)
        return call

    def await_page(iterator):
        result = native.ok("await", Call=submit_page(iterator))
        ids = result["ids"]
        assert len(ids) == page_size and len(set(ids)) == page_size
        return set(ids)

    def rpc_count():
        return native.ok("counts", "a")["counts"].get(kind, 0)

    def records():
        return listed(native, Collection=collection, User=native.user)

    held = ExitStack()
    try:
        target, expr = open_iterator("a")
        control, _ = open_iterator("b")
        # Drain the first page before measuring. Go QueryIterator fetches it
        # at construction and can serve the first Next from its local cache.
        await_page(target)
        first_control = await_page(control)
        before = rpc_count()
        checkpoint = held.enter_context(iterator_finish_checkpoint(collection, kind))
        outstanding = submit_page(target)
        hit = checkpoint.wait(timeout=15) if checkpoint else None
        deadline = monotonic() + 30
        while monotonic() < deadline:
            matches = [
                row
                for row in records()
                if row["type"] == kind and row["proxy_id"] == native.owner and expr in row["expr"]
            ]
            if matches:
                assert len(matches) == 1, matches
                row = matches[0]
                break
            if native.ok("poll", Call=outstanding)["done"]:
                pytest.fail("OBSERVATION_WINDOW: native iterator batch ended before List captured it")
            sleep(0.02)
        else:
            pytest.fail("OBSERVATION_WINDOW: no native iterator batch captured")
        assert row["cancellable"] and row["state"] in {"Queued", "Running"}, row
        assert rpc_count() == before + 1
        request_id = row["request_id"]
        if hit:
            assert request_id == hit.request_id
        reason = "t13-native-iterator-" + uuid4().hex
        result = canceled(native, [request_id], reason)
        assert [item["request_id"] for item in result["canceled"]] == [request_id], result
        held.close()
        call, outstanding = outstanding, None
        terminal(native, call, reason)
        assert rpc_count() == before + 1, "Canceled iterator batch was retransmitted"
        assert request_id not in {item["request_id"] for item in records()}
        assert first_control.isdisjoint(await_page(control)), "Control iterator repeated IDs"
    finally:
        held.close()
        if outstanding is not None:
            native.call("await", Call=outstanding)
        for iterator in reversed(opened):
            native.ok("iterator-close", RequestID=iterator)

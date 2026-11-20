"""T04 expression byte boundaries through native Python and REST List."""

from concurrent.futures import ThreadPoolExecutor
from uuid import uuid4

import pytest
from running_requests_l1_rest_support import post_rest
from running_requests_l1_support import (
    QUERY_TIMEOUT,
    TOPK,
    assert_canceled,
    assert_unregistered,
    cancel_exact,
    list_complete,
    wait_for_records,
)

pytestmark = pytest.mark.tags("L1")


@pytest.mark.parametrize("boundary", [255, 256, 257, "utf8-at-254", "utf8-at-255"])
def test_t04_expression_prefix_bytes(boundary, deployment, workload):
    """Preserve <=256 bytes; truncate longer UTF-8 at a character boundary."""
    uris, token, user, connect = deployment
    _, proxy_b = uris
    app, _, collection, vectors = workload
    operator = connect(proxy_b)
    if isinstance(boundary, int):
        expr = "id >= 0" + " " * (boundary - len("id >= 0"))
        assert len(expr.encode()) == boundary
    else:
        offset = int(boundary.rsplit("-", 1)[1])
        prefix = "id >= 0 and "
        expr = prefix + " " * (offset - len(prefix) - 1) + '"你" == "你"'
        assert expr.encode().index("你".encode()) == offset
    expected = expr.encode("utf-8")[:256].decode("utf-8", errors="ignore")

    def records():
        return list_complete(operator, set(uris), collection_name=collection, user=user)

    reason = f"T04-expression-{uuid4().hex}"
    with ThreadPoolExecutor(max_workers=1) as pool:
        future = pool.submit(
            app.search,
            collection,
            vectors,
            anns_field="vector",
            filter=expr,
            limit=TOPK,
            search_params={"metric_type": "L2"},
            timeout=QUERY_TIMEOUT,
        )
        row = wait_for_records(records, {"Search"}, lambda item: item.type, [future], exact=True)["Search"]
        assert row.expr == expected, (row.expr, expected)
        body = post_rest(
            uris[proxy_b],
            token,
            set(uris),
            "list",
            {
                "dbName": "default",
                "collectionName": collection,
                "user": user,
            },
        )
        matches = [item for item in body["requests"] or [] if item["requestId"] == row.request_id]
        assert len(matches) == 1, "OBSERVATION_WINDOW: target exited before REST snapshot"
        assert matches[0]["expr"] == expected
        assert len(matches[0]["expr"].encode("utf-8")) <= 256
        cancel_exact(operator, set(uris), [row.request_id], reason)
        assert_canceled(future, reason)
    assert_unregistered(records, {row.request_id})

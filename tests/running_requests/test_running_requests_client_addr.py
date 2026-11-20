"""Compare API client_addr with an independently observed Proxy TCP peer."""

import ipaddress
import os
from concurrent.futures import ThreadPoolExecutor
from contextlib import nullcontext
from time import monotonic, sleep

import pytest
from pymilvus import MilvusClient
from running_requests_l1_support import (
    assert_canceled,
    list_complete,
    search,
    wait_for_records,
)
from running_requests_topology import ProxyAuthorizationTopology, forward, run

pytestmark = pytest.mark.tags("L1")


def test_client_addr_matches_proxy_socket(deployment, workload):
    check_client_addr_matches_proxy_socket(deployment, workload)


def check_client_addr_matches_proxy_socket(deployment, workload, *, operator=None):
    uris, token, user, connect = deployment
    a, b = uris
    _, _, collection, vectors = workload
    operator = connect(b) if operator is None else operator
    local_proxy = os.environ.get("RR_LOCAL_T04_PROXY_A")
    if local_proxy:
        command = ["docker", "exec", local_proxy]
        connection = nullcontext(uris[a])
    else:
        topology = ProxyAuthorizationTopology()
        resource = topology.proxy_pods()[a]
        command = [*topology.kube, "exec", resource, "-c", "proxy", "--"]
        connection = forward(topology.kube, resource)

    def peers():
        raw = run([*command, "cat", "/proc/net/tcp", "/proc/net/tcp6"])
        result = set()
        for line in raw.splitlines():
            parts = line.split()
            if len(parts) < 4 or parts[3] != "01" or int(parts[1].split(":")[1], 16) != 19530:
                continue
            host, port = parts[2].split(":")
            buf = bytes.fromhex(host)
            buf = b"".join(buf[i : i + 4][::-1] for i in range(0, len(buf), 4))
            ip = ipaddress.ip_address(buf)
            if isinstance(ip, ipaddress.IPv6Address) and ip.ipv4_mapped:
                ip = ip.ipv4_mapped
            result.add((str(ip), int(port, 16)))
        return result

    before = peers()
    with connection as uri:
        app = MilvusClient(uri=uri, token=token, dedicated=True, grpc_options={"grpc.use_local_subchannel_pool": 1})
        try:
            deadline = monotonic() + 5
            while monotonic() < deadline:
                added = peers() - before
                if added:
                    break
                sleep(0.05)
            assert len(added) == 1, f"Ambiguous independently observed peer set: {added}"
            expected = added.pop()
            with ThreadPoolExecutor(max_workers=1) as pool:
                future = pool.submit(search, app, collection, vectors)

                def rows():
                    return list_complete(operator, set(uris), collection_name=collection, user=user)

                row = wait_for_records(rows, {"Running"}, lambda r: r.state, [future], exact=True)["Running"]
                try:
                    host, port = row.client_addr.rsplit(":", 1)
                    assert (host.strip("[]"), int(port)) == expected, (row.client_addr, expected)
                    assert row.proxy_id == a
                    print(f"request={row.request_id} api_peer={row.client_addr} socket_peer={expected}")
                finally:
                    result = operator.cancel_requests([row.request_id], reason="peer-check", timeout=10)
                assert [x.request_id for x in result.canceled] == [row.request_id]
                assert_canceled(future, "peer-check")
        finally:
            app.close()

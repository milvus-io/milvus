"""Configure the two retained clusters through the normal topology RPC."""

import argparse
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

from acceptance import Fixture
from metadata import capture


def cluster(f):
    rows = capture(f.site, f.candidate + "-channels", timeout=30)
    channels = sorted(
        x["data"]["channel"]["name"] for x in rows if x.get("key", "").startswith("streamingcoord-meta/pchannel/")
    )
    ns = f.config["namespace"]
    assert channels == [ns + "-rootcoord-dml_" + str(i) for i in range(2)]
    return dict(cluster_id=ns, pchannels=channels, connection_param=dict(uri="http://" + f.endpoint, token=""))


def configure(primary, secondary):
    a, b = Fixture(primary), Fixture(secondary)
    clusters = [cluster(a), cluster(b)]
    edge = [dict(source_cluster_id=clusters[0]["cluster_id"], target_cluster_id=clusters[1]["cluster_id"])]
    with ThreadPoolExecutor(2) as pool:
        calls = [
            pool.submit(
                f.client.update_replicate_configuration, clusters=clusters, cross_cluster_topology=edge, timeout=100
            )
            for f in (a, b)
        ]
        for future in calls:
            future.result(timeout=105)
    for f in (a, b):
        f.record("configured topology", f.client.get_replicate_configuration(timeout=10))


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("primary", type=Path)
    p.add_argument("secondary", type=Path)
    a = p.parse_args()
    configure(a.primary.resolve(), a.secondary.resolve())

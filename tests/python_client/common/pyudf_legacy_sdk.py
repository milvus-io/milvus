"""An isolated, pinned old SDK probe; connection credentials arrive on stdin."""

import json
import sys

import pymilvus


def main():
    config = json.load(sys.stdin)
    assert pymilvus.__version__ == "2.6.6", pymilvus.__version__
    assert not hasattr(pymilvus, "FunctionChain")
    assert not hasattr(pymilvus, "FunctionChainStage")
    client = pymilvus.MilvusClient(uri=config["uri"], token=config["credential"])
    try:
        version = client.get_server_version(detail=True)
        # SDK2.6.6 ignores detail=True and exposes only the version string.
        # The current SDK independently binds this same endpoint to its commit.
        assert isinstance(version, str) and version == config["server_version"], version
        result = client.search(
            config["collection"],
            data=[[0.0, 0.0]],
            anns_field="vector",
            limit=3,
            output_fields=["ts"],
            search_params={"metric_type": "L2"},
            timeout=30,
        )
        assert [hit["id"] for hit in result[0]] == [1, 2, 3], result
        assert [hit["entity"]["ts"] for hit in result[0]] == [10, 20, 30], result
        expected = [0, 0.0001, 0.0004]
        for hit, distance in zip(result[0], expected, strict=True):
            assert abs(hit["distance"] - distance) <= 1e-7, result
        print(
            json.dumps(
                {
                    "sdk": pymilvus.__version__,
                    "server": version,
                    "ordinary_search": "PASS",
                    "function_chain_api": "ABSENT_LOCAL_SDK",
                    "server_function_chain_request_attempted": False,
                    "detailed_server_version_api": "ABSENT_LOCAL_SDK",
                    "hits": [
                        [{"id": hit["id"], "distance": hit["distance"], "ts": hit["entity"]["ts"]} for hit in hits]
                        for hits in result
                    ],
                }
            )
        )
    finally:
        client.close()


if __name__ == "__main__":
    main()

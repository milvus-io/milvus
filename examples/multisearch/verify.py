"""Populate heterogeneous collections, exercise the real RPC, save evidence."""
import argparse
import concurrent.futures
import json
import time
from pathlib import Path

import grpc
import numpy as np
from pymilvus import DataType, MilvusClient
from pymilvus.client.prepare import Prepare

from client import MultiSearchClient, multisearch_pb2


def run(args):
    admin = MilvusClient(uri=f"http://{args.target}", token="root:Milvus")
    client = MultiSearchClient(args.target)
    rng = np.random.default_rng(20261031)
    specs, tests = [], []
    out = Path(args.output)
    out.parent.mkdir(parents=True, exist_ok=True)
    report = {"target": args.target, "seed": 20261031, "tests": tests, "collections": specs, "reuse": args.reuse, "started_at_utc": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())}

    def save():
        out.write_text(json.dumps(report, indent=2))

    def record(name, detail):
        tests.append({"name": name, "passed": True, "detail": detail})
        save()
        print(f"PASS {name}: {detail}", flush=True)

    def request(i, k=5, **changes):
        s = specs[i]
        params = dict(collection_name=s["name"], data=[s["query"]], anns_field=s["vector_field"],
                      param={"metric_type": s["metric"], "params": {}}, limit=k,
                      output_fields=[s["scalar_field"]], use_default_consistency=False,
                      consistency_level=0, expr=f'{s["scalar_field"]} >= 0')
        params.update(changes)
        return Prepare.search_requests_with_expr(**params)

    def success(result):
        assert result.status.code == 0 and result.status.error_code == 0, str(result.status)

    def compare(a, b):
        success(a)
        success(b)
        assert a.results.ids == b.results.ids
        assert a.results.topks == b.results.topks
        assert a.results.fields_data == b.results.fields_data
        np.testing.assert_allclose(a.results.scores, b.results.scores, rtol=1e-5, atol=1e-5)

    try:
        for i in range(args.collections):
            name = f"ms_prototype_{i:02d}"
            dim = [8, 16, 32][i % 3]
            metric = ["L2", "IP", "COSINE"][i % 3]
            vector_field, scalar_field = f"embedding_{dim}", f"category_{i % 3}"
            string_pk = i % 2 == 1
            schema = admin.create_schema(auto_id=False, enable_dynamic_field=False)
            pk_args = {"max_length": 64} if string_pk else {}
            schema.add_field("id", DataType.VARCHAR if string_pk else DataType.INT64, is_primary=True, **pk_args)
            schema.add_field(vector_field, DataType.FLOAT_VECTOR, dim=dim)
            schema.add_field(scalar_field, DataType.INT64)
            vectors = rng.normal(size=(args.rows, dim)).astype(np.float32)
            if args.reuse:
                assert admin.has_collection(name), name
                assert int(admin.get_collection_stats(name)["row_count"]) == args.rows
            else:
                if admin.has_collection(name):
                    admin.drop_collection(name)
                admin.create_collection(name, schema=schema, consistency_level="Strong")
                rows = [{"id": f"doc-{j}" if string_pk else j, vector_field: vectors[j].tolist(), scalar_field: j % 7} for j in range(args.rows)]
                admin.insert(name, rows)
                admin.flush(name)
                index = admin.prepare_index_params()
                index.add_index(vector_field, index_type="FLAT", metric_type=metric)
                admin.create_index(name, index)
                admin.load_collection(name)
            specs.append(dict(name=name, dim=dim, metric=metric, vector_field=vector_field,
                              scalar_field=scalar_field, primary_key="VARCHAR" if string_pk else "INT64",
                              rows=args.rows, query=vectors[0].tolist()))
            print(f"LOADED {name}: {metric}, dim={dim}, rows={args.rows}", flush=True)
        record("Populated heterogeneous collections", f"{len(specs)} collections, {sum(s['rows'] for s in specs)} rows; mixed fields, dimensions, metrics and primary-key types")

        items = [(f"item-{i}", request(i % len(specs), k=1 + i % 9)) for i in range(50)]
        client.calls.counts.clear()
        batch = client.multi_search(items)
        assert batch.status.code == 0 and len(batch.items) == len(items)
        calls = dict(client.calls.counts)
        assert sum(calls.values()) == 1 and list(calls)[0].endswith("/MultiSearch")
        report["single_envelope_rpc_counts"] = calls
        for (item_id, req), item in zip(items, batch.items):
            assert item.id == item_id and item.grpc_code == 0
            compare(client.search(req), item.result)
        record("50-item equivalence and one RPC", f"50 independent searches across {len(specs)} collections; IDs, topks, scalar fields and scores match ordinary Search")

        mixed = client.multi_search([
            ("valid-a", request(0)),
            ("missing", request(0, collection_name="ms_prototype_missing")),
            ("bad-dim", request(1, data=[[1.0, 2.0]])),
            ("valid-b", request(2, k=7)),
            ("empty", request(0, expr=f'{specs[0]["scalar_field"]} < 0')),
        ])
        assert mixed.status.code == 0
        assert mixed.items[1].result.status.code != 0 and mixed.items[2].result.status.code != 0
        compare(mixed.items[0].result, client.search(request(0)))
        compare(mixed.items[3].result, client.search(request(2, k=7)))
        success(mixed.items[4].result)
        assert len(mixed.items[4].result.results.scores) == 0
        report["mixed_outcomes"] = [{"id": x.id, "code": x.result.status.code, "grpc_code": x.grpc_code, "reason": x.result.status.reason} for x in mixed.items]
        record("Partial failure isolation", report["mixed_outcomes"])

        for label, reqs in [("empty", []), ("duplicate IDs", [("x", request(0)), ("x", request(1))]),
                            ("129 items", [(str(i), request(0)) for i in range(129)])]:
            resp = client.multi_search(reqs)
            assert resp.status.code != 0 and len(resp.items) == 0
            record(f"Envelope rejects {label}", {"code": resp.status.code, "reason": resp.status.reason})

        # End-to-end RBAC: the same authenticated identity can search collection
        # zero but cannot use the envelope to reach collection one.
        username, password, role = "ms_proto_reader", "MultiSearch_test_123", "ms_proto_reader_role"
        if username not in admin.list_users():
            admin.create_user(username, password)
        if role not in admin.list_roles():
            admin.create_role(role)
        admin.grant_role(username, role)
        admin.grant_privilege_v2(role, "Search", specs[0]["name"], db_name="default")
        restricted = MultiSearchClient(args.target, token=f"{username}:{password}")
        deadline = time.monotonic() + 10
        while True:
            rbac = restricted.multi_search([("allowed", request(0)), ("denied", request(1))])
            if rbac.items[0].grpc_code == 0:
                break
            assert time.monotonic() < deadline, str(rbac)
            time.sleep(0.2)
        success(rbac.items[0].result)
        assert rbac.items[1].grpc_code == grpc.StatusCode.PERMISSION_DENIED.value[0]
        assert not rbac.items[1].HasField("result")
        restricted.close()
        record("Per-item collection authorization", {"allowed": 0, "denied_grpc_code": rbac.items[1].grpc_code})

        invalid = MultiSearchClient(args.target, token="root:wrong-password")
        try:
            invalid.multi_search([("x", request(0))])
            raise AssertionError("invalid credentials succeeded")
        except grpc.RpcError as exc:
            assert exc.code() == grpc.StatusCode.UNAUTHENTICATED
            record("Envelope authentication", exc.code().name)
        finally:
            invalid.close()

        try:
            client.multi_search(items, timeout=0.000001)
            raise AssertionError("expired RPC unexpectedly succeeded")
        except grpc.RpcError as exc:
            assert exc.code() == grpc.StatusCode.DEADLINE_EXCEEDED
            record("Client deadline", exc.code().name)
        success(client.multi_search([("after-timeout", request(0))]).items[0].result)
        record("Search succeeds after deadline", "server continues serving requests")

        if args.functional_only:
            report["all_passed"] = True
            save()
            return

        def elapsed(fn):
            start = time.perf_counter()
            fn()
            return (time.perf_counter() - start) * 1000

        timings = {"sequential_50_search": [], "parallel_50_search_16_workers": [], "multisearch_50_items": []}
        with concurrent.futures.ThreadPoolExecutor(max_workers=16) as pool:
            def sequential():
                for _, req in items: success(client.search(req))
            def parallel():
                for result in pool.map(lambda item: client.search(item[1]), items): success(result)
            def multi():
                result = client.multi_search(items)
                assert result.status.code == 0
                for item in result.items:
                    assert item.grpc_code == 0
                    success(item.result)
            variants = [("sequential_50_search", sequential), ("parallel_50_search_16_workers", parallel), ("multisearch_50_items", multi)]
            for _, fn in variants: fn()
            for trial in range(args.trials):
                for offset in range(3):
                    name, fn = variants[(trial + offset) % 3]
                    timings[name].append(elapsed(fn))
        report["benchmark"] = {name: {"samples_ms": values, "p50_ms": float(np.percentile(values, 50)),
                                     "p95_ms": float(np.percentile(values, 95)), "p99_ms": float(np.percentile(values, 99)),
                                     "rpc_per_group": 1 if name.startswith("multisearch") else 50} for name, values in timings.items()}
        record("Local comparison benchmark", f"{args.trials} trials per variant; same 50 logical searches; warmup excluded; rotating variant order")
        report["all_passed"] = True
        save()
    except Exception as exc:
        report["all_passed"] = False
        report["failure"] = repr(exc)
        save()
        raise
    finally:
        client.close()
        admin.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--target", default="localhost:29550")
    parser.add_argument("--collections", type=int, default=24)
    parser.add_argument("--functional-only", action="store_true")
    parser.add_argument("--reuse", action="store_true", help="Reuse the deterministic dataset from a previous run")
    parser.add_argument("--rows", type=int, default=256)
    parser.add_argument("--trials", type=int, default=20)
    parser.add_argument("--output", default="artifacts/multisearch/results.json")
    run(parser.parse_args())

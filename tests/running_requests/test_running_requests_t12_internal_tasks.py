"""T12. Request identity and cancellation across traced internal stages."""

import json
import os
import re
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
from time import monotonic, sleep
from uuid import uuid4

import pytest

from running_requests_evidence import hybrid_branch_evidence, live_server_logs
from running_requests_l1_checkpoints import checkpoint_provider
from running_requests_l1_support import (
    TOPK, assert_all_proxies_answered, assert_canceled, assert_search_baseline, assert_unregistered,
    cancel_exact, deployment, list_complete, search, search_collection, search_hits,
    wait_for_records, workload, shared_dataset, isolated_workload, workload_baseline,
)
from test_running_requests_t02_dql import hybrid_workload
from running_requests_topology import ProxyAuthorizationTopology, run

pytestmark = pytest.mark.tags("L1")


def test_t12_cancel_hybrid_with_trace(deployment, request):
    check_cancel_hybrid_with_trace(deployment, request)


def check_cancel_hybrid_with_trace(deployment, request, *, operator=None):
    if os.getenv("MILVUS_T12_HYBRID_TRACE") != "1":
        pytest.skip("Enable QueryNode DEBUG logs and fully sampled stdout trace for T12 HybridSearch")

    uris, _, user, connect = deployment
    proxy_a = next(iter(uris))
    operator = connect(next(reversed(uris))) if operator is None else operator
    topology = ProxyAuthorizationTopology()
    proxy = topology.proxy_pods()[proxy_a]
    release = os.environ["MILVUS_L1_RELEASE"]
    pods = json.loads(run([*topology.kube, "get", "pods", "-o", "json"]))["items"]
    pods = [pod for pod in pods
            if pod["metadata"]["name"].startswith(f"{release}-milvus-querynode-")
            and not pod["metadata"].get("deletionTimestamp")]
    assert len(pods) == 1, "This trace case requires one QueryNode to observe both branches"
    node = pods[0]["metadata"]["name"]
    collection, hybrid = request.getfixturevalue("hybrid_workload")
    app = connect(proxy_a)
    control = connect(next(reversed(uris)))
    trim = int(os.getenv("MILVUS_L1_HYBRID_NQ", "2048")) - 1
    baseline = search_hits(hybrid(control, trim=trim), 1, TOPK)

    def records():
        return list_complete(operator, set(uris), collection_name=collection, user=user)

    since = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    sources = {"proxy": [proxy], "querynode": [node]}
    with live_server_logs(sources, local=False, kube=topology.kube, since=since) as logs:

        def log_lines(pod, component):
            assert pod in sources[component]
            return logs(component)

        def segment_events(lines, trace_id, message):
            events = []
            for line in lines:
                if f"[traceID={trace_id}]" not in line or f'["{message}"]' not in line:
                    continue
                segment = re.search(r"\[segmentID=(\d+)\]", line)
                span = re.search(r"\[spanID=([0-9a-f]{16})\]", line)
                if segment and span:
                    events.append((segment[1], span[1]))
            return events

        def trace_spans(lines, trace_id):
            spans = []
            for line in lines:
                _, marker, payload = line.partition('{"Name":')
                if marker and trace_id in payload:
                    spans.append(json.loads(marker + payload))
            return [span for span in spans if span["SpanContext"]["TraceID"] == trace_id]

        with ThreadPoolExecutor(max_workers=2) as pool:
            for _ in range(5):
                trace_id = uuid4().hex
                reason = "T12-hybrid-" + trace_id
                target = pool.submit(hybrid, app, client_request_id=trace_id)
                row = None
                starts = []
                deadline = monotonic() + 10
                while not target.done() and monotonic() < deadline:
                    current = records()
                    matching = [item for item in current if item.trace_id == trace_id]
                    assert len(matching) <= 1, matching
                    if not matching:
                        continue
                    if row is not None:
                        assert matching[0].request_id == row.request_id
                    row = matching[0]
                    starts = segment_events(log_lines(node, "querynode"), trace_id, "search segment...")
                    if len(starts) >= 2:
                        break

                if len(starts) < 2 or target.done():
                    target.result(timeout=90)
                    continue
                assert row is not None and row.proxy_id == proxy_a and row.type == "HybridSearch"
                assert len(starts) == 2 and len({segment for segment, _ in starts}) == 1, starts
                assert len({span for _, span in starts}) == 2
                control_call = pool.submit(hybrid, control, trim=trim)
                result = operator.cancel_requests([row.request_id], reason=reason, timeout=10)
                assert_search_baseline(control_call.result(timeout=90), baseline, 1)
                assert_all_proxies_answered(result, set(uris))
                assert not result.undetermined
                if result.not_found == [row.request_id] and not result.canceled:
                    target.result(timeout=90)
                    continue
                assert not result.not_found and [item.request_id for item in result.canceled] == [row.request_id]
                assert len(row.task_ids) == 1 and result.canceled[0].task_ids == row.task_ids
                assert_canceled(target, reason)
                assert_unregistered(records, {row.request_id})
                break
            else:
                pytest.fail("OBSERVATION_WINDOW: no HybridSearch canceled after both branch starts were observed")

        audits = [line for line in log_lines(proxy, "proxy")
                  if '["request canceled by operator"]' in line and f"[requestID={row.request_id}]" in line
                  and f"[traceID={trace_id}]" in line and reason in line and "[servedByProxyID=" not in line]
        assert len(audits) == 1, audits
        deadline = monotonic() + 15
        while monotonic() < deadline:
            lines = log_lines(node, "querynode")
            spans = trace_spans(lines, trace_id)
            query_views = [span for span in spans if span["Name"].endswith(("/SearchOnView", "/SearchOnViewStream"))]
            proxy_spans = trace_spans(log_lines(proxy, "proxy"), trace_id)
            search_tasks = [span for span in proxy_spans if span["Name"] == "SearchTask"]
            failures = segment_events(lines, trace_id, "Search failed")
            successes = segment_events(lines, trace_id, "search segment done")
            if (query_views and search_tasks and set(starts) <= set(failures + successes)
                    and {span for _, span in starts} <= {s["SpanContext"]["SpanID"] for s in spans if s["Name"] == "schedule"}):
                break
            sleep(0.25)
        else:
            pytest.fail(f"EVIDENCE_GAP: trace={trace_id} views={[s['Name'] for s in spans]} "
                        f"proxy_spans={[s['Name'] for s in proxy_spans]} "
                        f"starts={starts} failures={failures} successes={successes}")

        # Hybrid subqueries may be dispatched as two independent streaming RPCs.
        # Verify both actual schedule branches and their RPC ancestry instead.
        if os.getenv("RR_ARTIFACTS"):
            from pathlib import Path
            fields = ("Name", "SpanContext", "Parent", "Status", "StartTime", "EndTime", "ChildSpanCount")
            Path(os.environ["RR_ARTIFACTS"], f"t12-{trace_id}.json").write_text(json.dumps({
                "request_id": row.request_id, "starts": starts, "failures": failures, "successes": successes,
                "spans": [{key: span[key] for key in fields if key in span} for span in spans],
            }, indent=2))
        views, children = hybrid_branch_evidence(spans, starts)
        assert len(search_tasks) == 1, search_tasks
        assert not any(span["Name"] in {"Proxy-Search-PostExecute", "RetrieveTask"} for span in proxy_spans)
        assert sorted(segment_events(lines, trace_id, "search segment...")) == sorted(starts)
        assert sorted(failures) == sorted(starts)
        assert not successes
        cancel_at = audits[0].split("]", 1)[0][1:]
        print(f"T12 HybridSearch request_id={row.request_id} trace_id={trace_id} "
              f"cancel_audit={cancel_at} query_view_ends={[view['EndTime'] for view in views.values()]} branch_spans={sorted(children)}")


@pytest.mark.parametrize("path", ["requery", "search_by_id", "retry", "hybrid"])
def test_t12_cancel_across_internal_stages(path, checkpoint_provider, deployment, workload, workload_baseline, request):
    if path == "hybrid" and os.getenv("MILVUS_T12_HYBRID_TRACE") == "1":
        pytest.skip("T12 HybridSearch uses the trace test in this environment")
    check_cancel_across_internal_stages(path, checkpoint_provider, deployment, workload, workload_baseline, request)


def check_cancel_across_internal_stages(path, checkpoint_provider, deployment, workload, workload_baseline, request, *, operator=None):
    uris, _, user, connect = deployment
    proxy_a, proxy_b = uris
    owner, control, collection, vectors = workload
    operator = connect(proxy_b) if operator is None else operator
    control_vectors = vectors
    baseline = workload_baseline()
    vectors = vectors[:2]
    control_collection = collection
    if path == "hybrid":
        collection, invoke = request.getfixturevalue("hybrid_workload")
    else:
        options = dict(anns_field="vector", limit=TOPK, search_params={"metric_type": "L2"}, timeout=90)

        def run_search(trace_id):
            if path == "search_by_id":
                return owner.search(collection, ids=[0, 1], client_request_id=trace_id, **options)
            fields = ["vector"] if path == "requery" else []
            return owner.search(collection, vectors, output_fields=fields,
                                client_request_id=trace_id, **options)

        control_trace = uuid4().hex
        run_search(control_trace)
        dispatch_evidence = checkpoint_provider.downstream_evidence(path, control_trace)
        target_trace = uuid4().hex

        def invoke():
            return run_search(target_trace)

    def records():
        return list_complete(operator, set(uris), collection_name=collection, user=user)

    reason = f"T12-{path}-{uuid4().hex}"
    with ThreadPoolExecutor(max_workers=2) as pool:
        with checkpoint_provider.hold_internal_stage(collection=collection, path=path) as checkpoint:
            future = pool.submit(invoke)
            previous_id = None
            for phase in ("before", "during"):
                evidence = checkpoint.wait(phase, timeout=30)
                assert evidence.path == path and evidence.phase == phase
                row = wait_for_records(records, {evidence.request_id}, lambda item: item.request_id, [future])[
                    evidence.request_id
                ]
                snapshot = records()
                assert [item.request_id for item in snapshot] == [row.request_id]
                assert row.proxy_id == proxy_a
                assert set(row.task_ids) == set(evidence.proxy_task_ids)
                if previous_id is not None:
                    assert row.request_id == previous_id
                previous_id = row.request_id
                if phase == "before":
                    checkpoint.advance()
            assert evidence.started_task_ids
            if path == "retry":
                checkpoint.assert_retry_observed(row.request_id, timeout=30)
            control_call = pool.submit(search, control, control_collection, control_vectors)

            def control_records():
                return list_complete(
                    operator, set(uris), collection_name=control_collection, user=user,
                )

            control_row = wait_for_records(
                control_records, {len(control_vectors)},
                lambda item: item.nq if item.type == "Search" and item.request_id != row.request_id else None,
                [future, control_call],
            )[len(control_vectors)]
            assert not future.done() and not control_call.done(), "OBSERVATION_WINDOW: calls did not overlap Cancel"
            cancel_exact(operator, set(uris), [row.request_id], reason)
        assert_canceled(future, reason)
        assert_search_baseline(control_call.result(timeout=90), baseline, len(control_vectors))
    completion = checkpoint.wait_for_completion(row.request_id, timeout=30)
    assert set(evidence.started_task_ids) <= set(completion.exited_task_ids)
    if path == "requery":
        checkpoint.assert_requery_canceled(row.request_id, timeout=30)
    if path == "hybrid":
        checkpoint.assert_hybrid_children_terminal(row.request_id, expected_count=2, timeout=30)
    assert not completion.tasks_started_after_cancel
    if path != "hybrid":
        assert row.trace_id == target_trace
        dispatch_evidence(row.trace_id)
    assert_unregistered(records, {row.request_id})
    assert_unregistered(control_records, {control_row.request_id})

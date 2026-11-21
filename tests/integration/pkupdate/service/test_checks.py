"""Offline regressions for the acceptance checker; no Milvus service is contacted."""

import copy
import json
import os
import subprocess
import sys
import tempfile
import threading
import unittest
from concurrent import futures
from pathlib import Path
from types import SimpleNamespace
from unittest import mock

import grpc
from acceptance import create_indexes
from cluster_site import ROLES
from history import check_history, serving_generation
from metadata import capture
from owner_faults import check_row
from pymilvus import MilvusClient
from pymilvus.grpc_gen import common_pb2, milvus_pb2_grpc
from recovery import replace
from replay_gap import same_stream_evidence


def segment(sid, channel, partition, level):
    return {
        "key": f"datacoord-meta/s/9/{partition}/{sid}",
        "data": {
            "ID": sid,
            "insertChannel": channel,
            "collectionID": "9",
            "partitionID": partition,
            "state": "Flushed",
            "level": level,
        },
    }


def row(tick, keys, **values):
    fields = [
        {"field_id": int(field), "literal": {"Value": {"ScalarValue": {"Data": {kind: value}}}}}
        for field, (kind, value) in values.items()
    ]
    return {
        "TimeTick": tick,
        "SourceOrdinal": 0,
        "Operation": 2 if fields else 1,
        "PrimaryKeys": {"IdField": {"IntId": {"data": keys}}},
        "Assignments": fields,
    }


def histories():
    score = {"101": ("LongData", 40)}
    shared = {"101": ("LongData", 99), "102": ("StringData", "shared")}
    src = []
    out = []

    def file(sid, ch, p, rows, source=True, legacy=False):
        ref = f"datacoord-meta/deltalog/9/{p}/{sid}/0" if source else f"datacoord-meta/s/9/{p}/{sid}"
        layout = {
            "CollectionID": 0 if legacy else 9,
            "VChannel": "" if legacy else ch,
            "PartitionID": 0 if legacy else int(p),
            "Fields": None if legacy else [],
        }
        return {"file": sid, "referrer": ref, "layout": layout, "rows": rows}

    for sid, ch, p, rows, leg in [
        ("1", "v0", "-1", [row(10, [16, 116])], True),
        ("2", "v0", "4", [row(20, [1, 2, 101, 102], **score)], False),
        ("3", "v0", "-1", [row(30, [2, 102], **shared)], False),
        ("4", "v1", "-1", [row(31, [999], **shared)], False),
    ]:
        src.extend([segment(sid, ch, p, "L0"), file(sid, ch, p, rows, legacy=leg)])
    for sid, ch, p, rows in [
        ("a", "v0", "4", [row(10, [16]), row(20, [1, 2], **score), row(30, [2], **shared)]),
        ("b", "v0", "5", [row(10, [116]), row(30, [102], **shared)]),
        ("c", "v1", "4", []),
        ("d", "v1", "5", []),
    ]:
        out.extend([segment(sid, ch, p, "L1"), file(sid, ch, p, rows, False)])
    return src, out


class Checks(unittest.TestCase):
    def test_worker_witness_rejects_stale_value_or_different_owner(self):
        result = {
            "base": {"sourceID": "3"},
            "status": {},
            "ids": {"intId": {"data": ["1"]}},
            "fieldsData": [
                {"fieldId": "100", "scalars": {"longData": {"data": ["1"]}}},
                {"fieldId": "101", "scalars": {"longData": {"data": ["73"]}}},
            ],
        }
        response = {"returncode": 0, "stdout": json.dumps({"legacyResults": result})}
        self.assertEqual(check_row(response, 73, 3), 3)
        with self.assertRaises(AssertionError):
            check_row(response, 72, 3)
        with self.assertRaises(AssertionError):
            check_row(response, 73, 4)

    def test_shared_stream_witness_rejects_reconnect_and_subscription_replacement(self):
        entry = "querynode transform log buffer received entry [pchannel=channel] [vchannel=channel_sibling] [subscriptionID=8] [timeTick={}]"
        terminal = "QNSegmentUnrecoverable channel_failed"
        first, last = entry.format(10), entry.format(20)
        with tempfile.TemporaryDirectory() as temp:
            log = Path(temp) / "worker.log"

            def verify(lines):
                log.write_text("\n".join(lines) + "\n")
                return same_stream_evidence(log, "channel", "channel_sibling", 10, 20, terminal)

            self.assertEqual(verify([first, terminal, last])["subscription_id"], "8")
            for lines in (
                [first, terminal, "resumable transform log stream acquired underlying stream [pchannel=channel]", last],
                [first, terminal, last.replace("subscriptionID=8", "subscriptionID=9")],
                [first, last, terminal],
            ):
                with self.subTest(lines=lines), self.assertRaises(AssertionError):
                    verify(lines)

    def test_event_sequences(self):
        src, out = histories()

        def check(s, d):
            return check_history(s, d, "9", "4", "5", 10, 20, 31)

        check(src, out)
        for where in ("source", "target"):
            for fault in ("drop", "duplicate", "tick", "ordinal", "assignment"):
                with self.subTest(where=where, fault=fault):
                    s, d = copy.deepcopy((src, out))
                    rows = s[3]["rows"] if where == "source" else d[1]["rows"]
                    event = rows[0] if where == "source" else rows[1]
                    if fault == "drop":
                        event["PrimaryKeys"]["IdField"]["IntId"]["data"].remove(2)
                    if fault == "duplicate":
                        rows.append(copy.deepcopy(event))
                    if fault == "tick":
                        event["TimeTick"] += 1
                    if fault == "ordinal":
                        event["SourceOrdinal"] = 1
                    if fault == "assignment":
                        event["Assignments"][0]["literal"]["Value"]["ScalarValue"]["Data"]["LongData"] = 99
                    with self.assertRaises(AssertionError):
                        check(s, d)

    def test_generation(self):
        data = []
        expected = {}
        for ch in ("v0", "v1"):
            p = {"segmentIds": [ch], "segmentManifestVersions": ["2"]}
            gen = {"streamingVersion": ch, "compactVersion": "4"}
            expected[ch] = {"insertChannel": ch, "manifestPath": '{"ver":2}', "transformStartAfterTimetick": "40"}
            data.append(
                {
                    "key": "coord/dv/9/" + ch,
                    "data": {
                        "collectionId": "9",
                        "dataVersion": gen,
                        "shards": [
                            {"vchannel": ch, "partitions": [{**p, "segmentTransformStartAfterTimeticks": ["40"]}]}
                        ],
                    },
                }
            )
            data.append(
                {
                    "key": "streamingnode-meta/wal/p/qv/9/" + ch,
                    "data": {
                        "meta": {
                            "collectionId": "9",
                            "state": "QueryViewStateUp",
                            "vchannel": ch,
                            "version": {"dataVersion": gen},
                            "readAdmissionFloor": {"transformingTimetick": "40"},
                        },
                        "queryNode": [{"nodeId": "8", "partitions": [p]}],
                    },
                }
            )
        self.assertEqual(serving_generation(data, "9", 8, expected), {"v0": 40, "v1": 40})
        stale = copy.deepcopy(data[1])
        stale["data"]["queryNode"][0]["partitions"][0]["segmentManifestVersions"] = ["1"]
        self.assertIsNone(serving_generation(data + [stale], "9", 8, expected))
        self.assertIsNone(serving_generation(data, "9", 7, expected))
        expected["v0"]["transformStartAfterTimetick"] = "41"
        self.assertIsNone(serving_generation(data, "9", 8, expected))

    def test_wrong_pid(self):
        with tempfile.TemporaryDirectory() as d:
            site = Path(d)
            (site / "processes.jsonl").write_text(json.dumps({"role": "querynode", "pid": os.getpid()}) + "\n")
            f = SimpleNamespace(site=site, config={"binary": "/does/not/exist", "namespace": "pk-update-dummy"})
            with mock.patch("recovery.os.kill") as signal:
                with self.assertRaisesRegex(RuntimeError, "ownership mismatch"):
                    replace(f, "querynode")
                signal.assert_not_called()

    def test_evidence_failure_still_restarts(self):
        with tempfile.TemporaryDirectory() as d:
            site = Path(d)
            (site / "processes.jsonl").write_text(json.dumps({"role": "querynode", "pid": 123}) + "\n")
            f = SimpleNamespace(
                site=site,
                config={"binary": "/test/milvus", "namespace": "pk-update-dummy", "metrics": {"querynode": 222}},
                record=mock.Mock(side_effect=OSError("write failed")),
            )
            with (
                mock.patch(
                    "recovery.subprocess.check_output",
                    return_value="/test/milvus run querynode -alias pk-update-dummy-querynode",
                ),
                mock.patch("recovery.owns_listener", return_value=True),
                mock.patch("recovery.os.kill") as kill,
                mock.patch("recovery.subprocess.run", return_value=SimpleNamespace(returncode=1)),
                mock.patch("recovery.launch") as launch,
            ):
                with self.assertRaisesRegex(OSError, "write failed"):
                    replace(f, "querynode")
                kill.assert_called_once()
                launch.assert_called_once_with(site, "querynode", binary=None, points=None)

    def test_optimized_cannot_start(self):
        with tempfile.TemporaryDirectory() as d:
            site = Path(d) / "new-site"
            r = subprocess.run(
                [sys.executable, "-O", Path(__file__).with_name("cluster_site.py"), site, "fake", "fake"],
                capture_output=True,
                text=True,
                timeout=10,
            )
            self.assertNotEqual(r.returncode, 0)
            self.assertIn("requires Python assertion checks", r.stderr)
            self.assertFalse(site.exists())

    def test_index_timestamp_rpc_deadline(self):
        budgets = []

        class NoTimestampReply(milvus_pb2_grpc.MilvusServiceServicer):
            def CreateIndex(self, request, context):
                budgets.append(context.time_remaining())
                return common_pb2.Status()

            def AllocTimestamp(self, request, context):
                budgets.append(context.time_remaining())
                cancelled = threading.Event()
                context.add_callback(cancelled.set)
                # Bound the test even if the caller regresses to no deadline.
                if not cancelled.wait(3):
                    context.abort(grpc.StatusCode.INTERNAL, "client never cancelled")
                context.abort(grpc.StatusCode.CANCELLED, "observed client cancellation")

        with futures.ThreadPoolExecutor(max_workers=2) as executor:
            server = grpc.server(executor)
            milvus_pb2_grpc.add_MilvusServiceServicer_to_server(NoTimestampReply(), server)
            port = server.add_insecure_port("127.0.0.1:0")
            server.start()
            try:
                with grpc.insecure_channel(f"127.0.0.1:{port}") as channel:
                    grpc.channel_ready_future(channel).result(timeout=3)
                    indexes = MilvusClient.prepare_index_params()
                    indexes.add_index("vec", index_type="FLAT", metric_type="L2")
                    with self.assertRaises(grpc.RpcError) as failure:
                        create_indexes(milvus_pb2_grpc.MilvusServiceStub(channel), "probe", indexes, timeout=0.5)
                    self.assertEqual(failure.exception.code(), grpc.StatusCode.DEADLINE_EXCEEDED)
                    self.assertEqual(len(budgets), 2)
                    self.assertTrue(all(0 < value <= 0.51 for value in budgets), budgets)
            finally:
                server.stop(0).wait(timeout=3)

    def test_inspector_deadline_preserves_log(self):
        with tempfile.TemporaryDirectory() as d:
            site = Path(d)
            (site / "site.json").write_text(
                json.dumps(
                    {"binary": str(site / "milvus"), "conf": {}, "configs": d, "metrics": dict.fromkeys(ROLES, 1)}
                )
            )
            helper = site / "inspect"
            helper.write_text("#!/bin/sh\necho inspector started\nexec sleep 20\n")
            helper.chmod(0o700)
            with self.assertRaises(subprocess.TimeoutExpired):
                capture(site, "hung", timeout=2)
            self.assertIn("inspector started", (site / "hung.log").read_text())


if __name__ == "__main__":
    unittest.main()

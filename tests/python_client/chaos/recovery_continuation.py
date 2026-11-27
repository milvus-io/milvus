"""Retain write inputs across recovery and verify continued persistence.

Fault injection belongs to the caller. An API-only run proves data correctness,
not that an already persisted growing segment was recovered and appended to.
"""

import fcntl
import hashlib
import json
import os
import time
from pathlib import Path

import pymilvus
from chaos.checker import Checker, _run_checker_with_interval, trace
from common import common_func as cf
from pymilvus import CollectionSchema, DataType, FieldSchema, connections
from utils.util_log import test_log as log

COLLECTION_PREFIX = "RecoveryContinuation_"
DEFAULT_ARTIFACT_DIR = "/tmp/ci_logs/recovery_continuation"
SMALL_PAYLOAD_BYTES = 64
LARGE_PAYLOAD_BYTES = 8192
BATCH_SIZE = 16
PHASE_BATCHES = 12


def is_recovery_collection(name):
    return isinstance(name, str) and name.startswith(COLLECTION_PREFIX)


class JournalWriteError(RuntimeError):
    pass


class RecoveryJournal:
    """Write intent before sending an RPC; an absent acknowledgment is uncertain."""

    def __init__(self, path):
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.file = self.path.open("a+", encoding="utf-8")
        try:
            fcntl.flock(self.file, fcntl.LOCK_EX | fcntl.LOCK_NB)
            self.file.seek(0)
            self.records = [json.loads(line) for line in self.file]
        except BaseException:
            self.file.close()
            raise

    def append(self, kind, **values):
        record = {"kind": kind, "time": time.time(), **values}
        serialized = json.dumps(record, separators=(",", ":"))
        try:
            self.file.write(serialized + "\n")
            self.file.flush()
            os.fsync(self.file.fileno())
        except OSError as error:
            raise JournalWriteError("Cannot persist the recovery input journal") from error
        # Keep the oracle independent even if the SDK mutates its input objects.
        record = json.loads(serialized)
        self.records.append(record)
        return record

    def events(self, kind):
        return [record for record in self.records if record["kind"] == kind]

    def close(self):
        self.file.close()


class RecoveryContinuationChecker(Checker):
    """Use an exclusive collection and never manually flush during the workload."""

    def __init__(self, collection_name=None, resume=False):
        name = collection_name or cf.gen_unique_str(COLLECTION_PREFIX)
        if not is_recovery_collection(name) or not name.replace("_", "").isalnum():
            raise ValueError(f"Use an exclusive collection starting with {COLLECTION_PREFIX}")
        self.journal = RecoveryJournal(Path(DEFAULT_ARTIFACT_DIR) / f"{name}.jsonl")
        if resume and not self.journal.events("header"):
            self.journal.close()
            raise AssertionError("Cannot resume recovery workload without its original input journal")
        self.fatal_error = None
        schema = CollectionSchema(
            [
                FieldSchema("id", DataType.INT64, is_primary=True, auto_id=False),
                FieldSchema("batch", DataType.INT64),
                FieldSchema("payload", DataType.VARCHAR, max_length=LARGE_PAYLOAD_BYTES),
                FieldSchema("vector", DataType.FLOAT_VECTOR, dim=8),
            ],
            enable_dynamic_field=False,
        )
        try:
            super().__init__(collection_name=name, shards_num=1, schema=schema, insert_data=False)
            description = self.milvus_client.describe_collection(name)
            self.collection_id = int(description["collection_id"])
            headers = self.journal.events("header")
            if self.journal.records and len(headers) != 1:
                raise AssertionError("Recovery journal must have exactly one header")
            if headers:
                header = headers[0]
                if header["version"] != 1 or header["collection_id"] != self.collection_id:
                    raise AssertionError("Recovery journal belongs to a different collection or workload version")
                self.batch_size = header["batch_size"]
                self.phase_batches = header["phase_batches"]
            else:
                # Never adopt data without its independently retained input ledger.
                rows = self.milvus_client.query(name, filter="", output_fields=["count(*)"], timeout=30)
                if rows[0]["count(*)"] != 0:
                    raise AssertionError("Cannot recover a nonempty collection without its input journal")
                self.batch_size = BATCH_SIZE
                self.phase_batches = PHASE_BATCHES
                self.journal.append(
                    "header",
                    version=1,
                    collection=name,
                    collection_id=self.collection_id,
                    batch_size=self.batch_size,
                    phase_batches=self.phase_batches,
                )
            self.attempts = self.journal.events("attempt")
            self.acknowledged = {record["batch"] for record in self.journal.events("ack")}
            self.observed = {pk for record in self.journal.events("observed") for pk in record["ids"]}
            self.fatal_error = next((record["reason"] for record in self.journal.events("integrity_failure")), None)
            if [entry["batch"] for entry in self.attempts] != list(range(len(self.attempts))):
                raise AssertionError("Non-contiguous attempt journal")
            if not self.acknowledged.issubset(range(len(self.attempts))):
                raise AssertionError("Acknowledgment without a retained input batch")
            self.journal.append(
                "session",
                server=self.milvus_client.get_server_version(detail=True, timeout=30),
                sdk_version=pymilvus.__version__,
                checker_sha256=hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
            )
        except BaseException:
            self.close()
            raise

    def close(self):
        if self.journal.file.closed:
            return
        try:
            if hasattr(self, "milvus_client"):
                self.milvus_client.close()
            if hasattr(self, "alias"):
                connections.disconnect(self.alias)
        finally:
            self.journal.close()

    def write_batch(self, payload_size=None, timeout=30):
        batch = len(self.attempts)
        if payload_size is None:
            payload_size = SMALL_PAYLOAD_BYTES if (batch // self.phase_batches) % 2 == 0 else LARGE_PAYLOAD_BYTES
        rows = []
        for offset in range(self.batch_size):
            pk = batch * self.batch_size + offset
            prefix = f"{self.c_name}:{batch}:{pk}:"
            rows.append(
                {
                    "id": pk,
                    "batch": batch,
                    "payload": (prefix + "x" * payload_size)[:payload_size],
                    # Binary fractions survive FLOAT_VECTOR's float32 conversion exactly.
                    "vector": [((pk + index) % 16) / 16 for index in range(8)],
                }
            )
        attempt = self.journal.append("attempt", batch=batch, rows=rows)
        self.attempts.append(attempt)
        # Never retry these PKs: a failed/timeout RPC might already have committed.
        result = self.milvus_client.insert(self.c_name, data=rows, timeout=timeout)
        if result.get("insert_count") != len(rows):
            raise AssertionError(f"Unexpected insert_count for batch {batch}: {result.get('insert_count')}")
        self.journal.append("ack", batch=batch)
        self.acknowledged.add(batch)
        return attempt

    def verify_attempt(self, attempt, timeout=30):
        expected = {row["id"]: row for row in attempt["rows"]}
        actual = self.milvus_client.query(
            self.c_name,
            filter=f"id in {list(expected)}",
            output_fields=["id", "batch", "payload", "vector"],
            consistency_level="Strong",
            limit=len(expected) + 1,
            timeout=timeout,
        )
        ids = [row["id"] for row in actual]
        if len(ids) != len(set(ids)):
            raise AssertionError(f"Duplicate primary keys in batch {attempt['batch']}")
        required = set(expected) if attempt["batch"] in self.acknowledged else self.observed.intersection(expected)
        if not required.issubset(ids):
            raise AssertionError(
                f"Missing confirmed rows in batch {attempt['batch']}: {sorted(required.difference(ids))}"
            )
        for row in actual:
            if row != expected.get(row["id"]):
                raise AssertionError(f"Content mismatch for primary key {row['id']}")
        newly_observed = set(ids).difference(self.observed)
        if newly_observed:
            self.journal.append("observed", ids=sorted(newly_observed))
            self.observed.update(newly_observed)
        return len(actual)

    @trace()
    def recovery_continuation(self):
        try:
            if self.fatal_error:
                return self.fatal_error, False
            attempt = self.write_batch()
            self.verify_attempt(attempt)
            if len(self.attempts) > 1:
                self.verify_attempt(self.attempts[0])
            return None, True
        except JournalWriteError as error:
            self.fatal_error = str(error)
            return self.fatal_error, False
        except AssertionError as error:
            self.fatal_error = str(error)
            try:
                self.journal.append("integrity_failure", reason=self.fatal_error)
            except JournalWriteError:
                log.error("Failed to persist the recovery integrity failure")
            return self.fatal_error, False
        except Exception as error:
            return str(error), False

    def run_task(self):
        return self.recovery_continuation()

    def keep_running(self):
        _run_checker_with_interval(self, 10)

    def verify(self):
        """Run after stopping the writer; every content/persistence failure is fatal."""
        # Allow the retained history to grow with the framework's request duration,
        # while keeping each individual RPC bounded.
        deadline = time.monotonic() + 180 + len(self.attempts)

        def remaining():
            value = deadline - time.monotonic()
            if value <= 0:
                raise AssertionError("Recovery verification deadline exceeded")
            return min(value, 180)

        try:
            if self.fatal_error:
                raise AssertionError(self.fatal_error)
            if not self.acknowledged:
                raise AssertionError("No acknowledged baseline writes; recovery coverage was not established")
            # An opposite-size batch must succeed before sealing anything.
            last_size = len(self.attempts[-1]["rows"][0]["payload"])
            next_size = SMALL_PAYLOAD_BYTES if last_size == LARGE_PAYLOAD_BYTES else LARGE_PAYLOAD_BYTES
            self.write_batch(payload_size=next_size, timeout=remaining())
            self.milvus_client.flush(self.c_name, timeout=remaining())
            self.milvus_client.release_collection(self.c_name, timeout=remaining())
            self.milvus_client.load_collection(self.c_name, timeout=remaining())
            visible = sum(self.verify_attempt(attempt, timeout=remaining()) for attempt in self.attempts)
            count = self.milvus_client.query(
                self.c_name,
                filter="",
                output_fields=["count(*)"],
                consistency_level="Strong",
                timeout=remaining(),
            )[0]["count(*)"]
            if count != visible:
                raise AssertionError(f"Unexpected rows after recovery: count={count}, verified={visible}")
            return self.journal.append(
                "verification",
                outcome="PASS",
                verified_rows=visible,
                acknowledged_batches=len(self.acknowledged),
                uncertain_batches=len(self.attempts) - len(self.acknowledged),
            )
        except BaseException as error:
            try:
                if isinstance(error, AssertionError):
                    self.journal.append("integrity_failure", reason=str(error))
                self.journal.append("verification", outcome="FAIL", reason=str(error))
            except JournalWriteError:
                log.error("Failed to persist the recovery verification failure")
            raise

    def check_result(self):
        result = super().check_result()
        log.info(f"Recovery journal: {self.journal.path}; attempts={len(self.attempts)}, ack={len(self.acknowledged)}")
        return result

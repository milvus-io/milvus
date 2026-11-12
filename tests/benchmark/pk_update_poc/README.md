# PK-update query-side POC benchmark

Compare three isolated standalone instances, running sequentially with identical
resource limits:

| Mode | Persisted data | Query path |
|---|---|---|
| R1-ON | Initial dataset, then local JSONL updates at segment load | All-data versioned temp index + UpdatesRecord |
| R1-OFF | Same initial dataset and logical updates | Original index + sparse UpdatesRecord correction; no temp index construction/maintenance |
| R2 | Precomputed final mirror dataset | Ordinary base/scalar indexes |

The runner requires an image containing sub-phases 1–4 of this POC. A normal
release image cannot validate it: missing injection/mode/reader logs fail the run.
The Docker daemon must be local so it can mount the generated files. The runner
binds only localhost ports and never modifies an existing instance or collection.

## Local small end-to-end run

From the repository root:

```bash
uv venv /tmp/pk-update-poc-venv --python 3.12
uv pip install --python /tmp/pk-update-poc-venv/bin/python \
  -r tests/benchmark/pk_update_poc/requirements.txt

docker pull <poc-image>
/tmp/pk-update-poc-venv/bin/python tests/benchmark/pk_update_poc/run.py \
  --image <poc-image> --work-dir /tmp/pk-update-poc-small --rows 2000
```

Use a **new** work directory for each run. Defaults: four CPUs, 8 GiB container
memory, ports 19540/9095, three warmups, ten measured requests per case, one
client. Adjust `--timeout`, `--cpus`, `--memory`, `--port` and `--health-port` as
needed. Small runs validate functionality, not production performance.

The runner:

1. Generates initial rows and logical updates; independently replays the written
   updates in SQLite to create the mirror and expected PK/value files.
2. Creates one shard/partition, inserts without intermediate flushes, flushes
   once, and builds identical INVERTED scalar indexes plus a FLAT vector index.
3. Resolves real collection/field IDs. Binds update timestamps after the largest
   insert timestamp, atomically publishes JSONL, and restarts before loading so
   the non-refreshable POC configuration takes effect. R2 keeps injection disabled.
4. Requires exactly one loaded sealed segment with the expected row count and
   checks injection ready, mode and applied-through timestamp logs. Queries use
   Strong consistency after injection; no production clock is rewritten.
   R1-ON deliberately withholds the file until two missing-file retry warnings
   appear, then publishes it by atomic rename and waits for readiness.
5. Validates complete result PK sets against precomputed ground truth, and
   equality output values, before timing any query.
6. Requires sparse-store counters and per-query path evidence before timing:
   equality uses no base-value reads, general `LIKE 'hot%tail'` uses original-index
   correction, and an indexed conjunction reads only its candidate offsets.
7. Warms up and measures the fixed cases, checks reader-usage evidence, saves
   results/logs, then stops the instance before running the next mode.

Each instance uses `dataCoord.segment.maxSize=2048`, `sealProportion=1.0`, zero
jitter, compaction disabled, expression cache explicitly disabled, and segment pruning
disabled. POC load rejects an enabled expression cache. The layout check is
mandatory; these settings alone do not prove a single segment was produced.

## Workload and cases

The initial review baseline is limited to `age == 42` with `count(*)` across
ON, OFF and R2. Establish its result count, actual reader path, latency and
memory first. The broader cases below remain coverage items; they do not need
to block publication of this explicitly limited baseline. Candidate-mask
checks must use a plan where an earlier leaf actually produces candidates;
the current optimizer may place arithmetic before equality.

- Schema: INT64 PK, nullable INT64 `age`, nullable VARCHAR `name` (maximum 32
  bytes), FLOAT_VECTOR dimension 8. Non-NULL names contain 32 lowercase ASCII
  bytes. PK lists of up to 256 entries share assignments in the updates file.
- Fixed workload: 20% of offsets receive three successive updates to both scalar
  fields. Includes NULL transitions, restoration of earlier values, and rows
  entering/leaving matches. No writes occur during correctness or timing runs.
- Eight filters: equality, IN, range, prefix, NULL, arithmetic, suffix LIKE, and
  an AND/OR/NOT combination. Arithmetic and pure suffix LIKE exercise value
  evaluation: the current INVERTED reader declines the pure suffix operator even
  without updates. A separate, untimed general-LIKE gate checks original-index
  correction in both ON and OFF. The same timed filters run in all three modes.
- Filter timings use `count(*)` to avoid result-transfer volume dominating the
  comparison. Equality alone adds two cases with limit 128: PK-only output and
  `pk, age, name` output. No predicate × output-field Cartesian product.
- The correctness iterator checks all matching PKs, independent of the timing
  limit. Equality also compares scalar values. The three groups must agree with
  ground truth individually; agreement with another buggy group is insufficient.

Generate reusable fixtures separately, including the intended 10M-row workload:

```bash
python tests/benchmark/pk_update_poc/workload.py /tmp/pk-update-fixture --rows 10000000
/tmp/pk-update-poc-venv/bin/python tests/benchmark/pk_update_poc/run.py \
  --image <poc-image> --work-dir /tmp/pk-update-large \
  --fixture /tmp/pk-update-fixture --memory 32g --repeats 30
```

Generation streams JSONL and uses disk-backed SQLite. It creates both base and
mirror datasets, so allow several GiB of disk for a 10M-row workload, plus
database/index storage. Verification holds one case's expected PK set in memory.
Sparse update histories and the optional all-data temp index need memory beyond the
base column size; choose memory limits from measured usage before scaling up.

## Artifacts and interpretation

- `fixture/`: `base.jsonl`, logical `updates.jsonl`, `mirror.jsonl`, SQL oracle,
  expected result files and SHA-256 manifest. Existing fixtures are verified
  before use.
- `R1-ON/`, `R1-OFF/`, `R2/`: effective config, persisted data, bound injection
  file where applicable, and complete `server.log`. Containers are stopped but
  retained for diagnosis, including on failure. Remove these task-owned
  containers/data explicitly when finished.
- `run.json`: image ID/digest, workload identity and arguments.
- `results.json`: per-mode segment identity, applied-through timestamp, reader
  counters, container usage, raw latency samples, median and p95.
- `report.md`: compact median comparison and ON/R2, OFF/R2 ratios.
- `runner.log`: generation/preparation, correctness and timing progress.

Latencies include SDK/RPC and Strong-consistency waiting. Separate processes and
sequential execution avoid these three instances competing with each other, but
unrelated host activity and cross-architecture emulation still affect results.
R1-ON/R1-OFF compare the index choice; R1/R2 measure regression relative to
pre-materialized final values. This static harness does not validate GC,
concurrent mutation publication, historical snapshots, WAL or CDC.

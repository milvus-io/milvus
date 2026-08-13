# Bulk Import in CDC Replication Mode

This guide shows how to run a bulk import against a cluster that is part of a CDC
replication topology. In replication mode, imports must use **two-phase commit
(2PC)** so that the import is committed as a single, ordered point across the
primary and standby clusters.

Before you begin, make sure replication is already configured between your
clusters. See [CDC Replication Quick Start](./02-cdc-replication-quick-start.md)
for how to deploy two clusters and apply a replication configuration.

## Why 2PC Is Required

A normal bulk import auto-commits: the job runs to completion and the data
becomes visible on its own. This is not allowed in a replicating cluster.

Instead, you run the import in two-phase-commit mode by setting the import
option `auto_commit=false`:

1. **Import phase** — the data is loaded on the primary and replicated to the
   standby, but stays invisible. The job stops at the `Uncommitted` state and
   waits.
2. **Commit phase** — you explicitly commit the job. The commit is replicated to
   the standby as a single ordered fence, so both clusters make the data visible
   at the same logical point.

## Step 1: Enable Import in a Replicating Cluster

Import in a replicating cluster is disabled by default. Enable it by setting the
`dataCoord.import.enableInReplicatingCluster` config to `true`. Enable it on
**both** the primary and the standby clusters.

If you deploy with Milvus Operator, add the setting to `spec.config` of each
`Milvus` resource:

```yaml
spec:
  config:
    dataCoord:
      import:
        enableInReplicatingCluster: true
```

If you configure Milvus directly through `milvus.yaml`:

```yaml
dataCoord:
  import:
    enableInReplicatingCluster: true
```

This setting is refreshable, so it can take effect without a full restart.

When it is enabled, only `auto_commit=false` imports are accepted in a
replicating cluster. If you submit an import that violates these rules, it is
rejected:

| Situation | Error message |
|---|---|
| Config not enabled | `import in replicating cluster is not supported yet` |
| `auto_commit=true` submitted | `auto_commit=true import in replicating cluster is not supported` |

## Step 2: Run a 2PC Import

Run all import calls against the **primary** cluster. The import data and the
commit decision are replicated to the standby automatically, so you do not
submit or commit the import on the standby yourself.

Each cluster reads the import files from its own object storage. Make sure the
files you import exist in **both** the primary's and the standby's object
storage: upload them to both, or use object storage that both clusters can read.
If the files are missing on the standby, the replicated import fails there with
an object-not-found error.

The example uses the REST-based import helpers from `pymilvus.bulk_writer`.
The `url` values are the same Milvus addresses you use for other API calls.

```python
import time

from pymilvus.bulk_writer import (
    bulk_import,
    get_import_progress,
    commit_import,
)

# Primary and standby addresses. Replace with your own.
source_url = "http://127.0.0.1:19530"
target_url = "http://127.0.0.1:19531"

collection_name = "demo_collection"

# Object-storage paths of the files to import, prepared the same way as a
# normal bulk import (for example, with BulkWriter). Each inner list is one
# batch of files.
files = [
    ["import-data/part-1.parquet"],
]


def wait_for_state(url, job_id, target_state, timeout=600):
    """Poll an import job until it reaches target_state (or fails)."""
    deadline = time.time() + timeout
    while time.time() < deadline:
        resp = get_import_progress(url=url, job_id=job_id)
        data = resp.json().get("data", {})
        state = data.get("state")
        print(f"[{url}] job {job_id} state={state} progress={data.get('progress')}")
        if state == target_state:
            return
        if state == "Failed":
            raise RuntimeError(
                f"import job {job_id} failed on {url}: {data.get('reason')}"
            )
        time.sleep(3)
    raise TimeoutError(f"job {job_id} did not reach {target_state} on {url}")


# 1. Start a 2PC import on the PRIMARY. auto_commit=false is required in a
#    replicating cluster; the job stops at the Uncommitted state.
resp = bulk_import(
    url=source_url,
    collection_name=collection_name,
    files=files,
    options={"auto_commit": "false"},
)
job_id = resp.json()["data"]["jobId"]
print(f"started 2PC import job: {job_id}")

# 2. Best practice: wait until BOTH clusters report Uncommitted before you
#    commit. The same job_id is used on the primary and the standby, because
#    the import is replicated through the WAL.
wait_for_state(source_url, job_id, "Uncommitted")
wait_for_state(target_url, job_id, "Uncommitted")

# 3. Commit ONCE on the primary. The commit is replicated to the standby as a
#    single ordered fence, so you do not commit on the standby yourself.
commit_import(url=source_url, job_id=job_id)
print(f"committed import job: {job_id}")

# 4. Wait for the job to complete on both clusters.
wait_for_state(source_url, job_id, "Completed")
wait_for_state(target_url, job_id, "Completed")
print("import committed and visible on both clusters")
```

### Why Wait for `Uncommitted` on Both Clusters

Committing before the standby has finished importing does not corrupt data, but
it does mean the standby is still catching up at the moment you commit. Waiting
until both the primary and the standby report `Uncommitted` confirms that the
imported data has fully replicated and both clusters are ready to make it
visible together. This keeps the primary and standby as close as possible when
the commit is applied.

## Step 3: Verify the Data

After the job reaches `Completed`, the imported rows are visible on both
clusters. Load and query the collection on the primary, then run the same query
on the standby without manually loading the collection there, and confirm the
imported rows are present on both.

The standby is read-only while it remains a standby. Do not submit imports,
commit, or run other DDL or DCL operations directly on the standby. Perform them
on the primary and let replication apply them.

## FAQ

### Which cluster do I run the import and commit on?

The primary. The standby receives both the imported data and the commit through
replication. You never submit or commit an import on the standby.

### Do I need to commit on the standby?

No. Committing on the primary replicates the commit to the standby as a single
ordered fence, and the standby makes the data visible at the same logical point.

### Why does my import fail with "import in replicating cluster is not supported yet"?

`dataCoord.import.enableInReplicatingCluster` is not enabled on that cluster.
Set it to `true` on both the primary and the standby. See
[Step 1](#step-1-enable-import-in-a-replicating-cluster).

### Why does my import fail with "auto_commit=true import in replicating cluster is not supported"?

In a replicating cluster, only `auto_commit=false` (2PC) imports are accepted.
Set `options={"auto_commit": "false"}` on the import request.

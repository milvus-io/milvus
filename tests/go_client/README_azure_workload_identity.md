# Azure external-table workload identity E2E

`TestExternalTableAzureWorkloadIdentityE2E` targets Milvus master and its shared
C++ filesystem for Parquet and Iceberg. It does not target Milvus 3.0's separate
Rust Iceberg cloud backend.

## Server prerequisites

Run the patched Milvus server with Azure workload identity configured on every
pod that accesses external storage (including discovery, indexing and query
workers). The server pods need `AZURE_CLIENT_ID`, `AZURE_TENANT_ID` and a readable
`AZURE_FEDERATED_TOKEN_FILE`, with a matching federated identity configuration
and Blob read access to the fixtures. For sovereign clouds, also configure the
appropriate `AZURE_AUTHORITY_HOST`.

The identity belongs to the **server**. Setting these variables only on the Go
test runner does not configure Milvus. Use a dedicated test deployment with no
shared account key, SAS Broker or client secret configured for these reads.
The shared Go client test harness removes non-default databases during teardown;
run it only against a disposable test instance. Keep the projected token file
configured so the server selects workload identity
instead of managed identity. Azurite does not validate this Azure OAuth flow.

## Read-only fixtures

Prepare two fixtures in the same storage account, accessible to the server's
identity: a directory containing Parquet files, and an Iceberg table with a
current snapshot. The Iceberg metadata, manifests and data files must all remain
available in Azure; use a table without delete files for this basic test.

Both fixtures must contain the same positive number of rows and these columns:

| Column | Type | Requirement |
| --- | --- | --- |
| `pk` | int64 | Unique; exactly one row with `pk = 0` |
| `label` | string | At most 256 bytes; row 0 is `azure-workload-identity` |
| `vector` | list of float32 | Length 4; row 0 is `[1, 0, 0, 0]`; no zero vectors |

Use the actual fixture row count for `AZURE_FIXTURE_ROWS`. The test checks that
count, the scalar/vector values for row 0 and a filtered vector search result.
It creates and drops uniquely named Milvus collections, but never writes to or
deletes the source fixtures.

## Run

From `tests/go_client`, with a Go version matching `go.mod`:

```bash
export AZURE_WORKLOAD_IDENTITY_TEST_ENABLED=true
export AZURE_STORAGE_ACCOUNT=mystorageacct
export AZURE_PARQUET_SOURCE=azure://core.windows.net/test-container/parquet-fixture/
export AZURE_ICEBERG_SOURCE=azure://core.windows.net/test-container/iceberg-fixture/metadata/00000.metadata.json
export AZURE_FIXTURE_ROWS=16

go test -tags dynamic,test -gcflags='all=-N -l' -count=1 -v \
  ./testcases -run '^TestExternalTableAzureWorkloadIdentityE2E$' \
  -timeout 35m -args -uri http://localhost:19530
```

Replace the account, container, prefixes, metadata filename, row count and
Milvus URI with the test deployment's values. For sovereign storage, replace
`core.windows.net` in both sources with the appropriate Azure Storage authority.
The account name is passed as `extfs.access_key_id`; no account key is supplied.

The test first rejects a missing account name and conflicting shared-key/IAM
configuration. Each format then exercises create, refresh, index, load, query
and search, followed by another refresh without resending the spec to verify
persisted configuration reuse. Failed refresh jobs fail the test immediately.

Without the enable flag the test skips. Once enabled, missing fixture settings
fail instead of silently skipping either format. Compilation or a skipped run
is **not** evidence of successful Azure authentication. Record the server
revision and results of a live run separately. OAuth expiry/token rotation,
permission revocation and transient Azure failures require additional live
fault-injection validation; this test does not claim to cover them.

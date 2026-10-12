# FunctionChain minimal call context

- Related issues: #46565, #52319
- Related designs: [stages](20261010-function-chain-transform-stages.md), [runtime schemas](20261012-function-chain-runtime-schema.md), [query expansion](20261012-function-chain-query-expansion.md)
- Status: implemented and locally verified; draft integration under review

## Contract

Each UDF invocation receives immutable, request-owned `database`, `collection`,
`user` and an operator `resource_name` for logging, alongside the existing stage,
Arrow schema, query snapshots and result grouping metadata. A per-call logger
attaches these four fields to user logs. User parameters remain separate and
cannot override these fields.

Database names are resolved through the existing Proxy metadata path, including
the default database. Collection names identify the canonical collection from
the resolved schema, including calls through an alias. This minimal interface
does not introduce database/collection IDs or a separate collection schema.

`user` comes from a private request-context marker installed only after real
password or API-key authentication succeeds. API keys use their verified user
mapping. Parsing an authorization header alone does not authenticate its name.
Calls with authorization disabled, direct internal calls, and custom auth
overrides without the marker have `user=""`; the runtime does not infer root.
This metadata is descriptive and does not create connector authorization.

The existing REST v2 L2 path calls Proxy locally, so HTTP authentication also
sets this marker after real password/API-key verification. New REST transform
stages and ingestion-bound REST INSERT retain their existing rejection guards.
Gin username fields and reconstructed authorization metadata do not promote an
unverified identity into this marker.

## Operator logging

The native executor derives one diagnostic `resource_name` for each operator
from the chain name (or stage), step position and operator name. It binds the
label and request scope to the operator's Go context using `mlog.WithFields`.
Success diagnostics use debug logging; failures retain existing typed errors.
No new public SDK operator parameter or chain/operator field is required.

Nested map/explode execution inside flat_map retains its outer operator label.
Selecting, aggregating or expanding rows does not change request identity.
Cloning the function context preserves query metadata, reduction/table flags,
row limits and the parent context's deadline/cancellation.

The call-context label identifies an operator for logs. The top-level PyUDF
`resource_name` and `udf_path` still identify the registered wheel for loading;
factory initialization and loader caching retain their existing resource
contract. Request scope and log adapters are never cached on the shared factory
instance. UDF authors still own thread safety for their own instance state.

## Deadline and cancellation

The existing Go `context.Context` remains the authority. PyUDF RPC execution
uses the earlier of the parent request deadline and configured RPC timeout.
gRPC already delivers that deadline/cancellation to the Python worker. Worker
checks before/after execution and Arrow processing remain in place.

This change adds no second timeout configuration, serialized deadline,
request/invocation ID, or Python deadline/control API. A running Python function
is not forcibly interrupted, and independently created model/SDK clients still
need their own timeouts. No connector registry, hosted Milvus client, recursion
policy, transaction or callback replay mechanism is introduced.

## Transport and compatibility

The private worker protocol adds a request-context message outside user
parameters. Execution version 6 requires it and an exact worker acknowledgement,
so older workers cannot silently discard identity. Version 6 preserves complete
Arrow schemas, expanded-query origins, typed empty groups and TABLE reduction
semantics. Runtime 0.6.0 supports the new contract; legacy 0/2/3/4/5 calls retain
their existing paths when no request context is attached.

Public Milvus/SDK protobufs and client DSLs are unchanged by this extension.
Matching server and installed worker versions are required. Capability checks
for PRE expansion and POST TABLE retain their existing meaning.

## Usage

```python
def transform_query(self, params, columns, *, context):
    context.logger.info("embedding request")
    # context.database / context.collection / context.user are supplied by host.
    # context.resource_name identifies this operator in diagnostic logs.
    return self.embed(columns)
```

INGESTION, PRE_PROCESS, Proxy L2_RERANK, POST HITS and POST TABLE use the same
request-scope transport. POST Query has the same identity metadata and no query
snapshots. QueryNode L0/L1 do not gain Python execution from this extension.

## Verification

Verification must trace authenticated identity from its construction sites to
all stage adapters, native executor, private RPC and Python callable. Required
cases include alias/default-database resolution, anonymous and failed auth,
parameter spoofing, concurrent calls through the same cached wheel instance,
old-worker rejection, schema/query-origin retention, and cancellation.

Local validation on 2026-10-12:

| Scope | Result |
|---|---|
| Complete chain/expr/transform/types suites with race detector | Passed |
| Complete PyUDF, DQL, DML, requestcontext, ingestion and transformio suites with race detector | Passed |
| gRPC/HTTP authentication and REST wrapper regression tests | Passed; both password and API-key sources traced |
| Installed runtime 0.6.0 | 117 tests and 65 subtests passed |
| Real worker gRPC scope/expansion tests | Passed; includes 16 concurrent calls through one cached instance, repeated input references/root schema metadata, old ACK rejection, cancellation and logger-allocation OOM |
| Final standalone: authenticated scopes | Two users in two databases passed INGESTION, PRE, L2, POST HITS/TABLE, Query and Hybrid 3/4 prepared-NQ calls; 16 concurrent requests retained their own scopes |
| Final standalone: metadata source checks | Aliases reported canonical names, missing database resolved to default, forged UDF params did not replace scope, invalid password was rejected before the handler |
| Final standalone: anonymous calls | Authentication-disabled requests with an unverified username header returned user="" |
| REST L2 failure/recovery | Before HTTP marker injection, a scope-checking UDF failed with code 2400; the final binary passed the same request with authenticated user=root |
| Formatting/generated products | gci, gofumpt, ruff, diffcheck and official proto generation/check passed |

Go tests used `-tags dynamic,test`, `-gcflags="all=-N -l"`, `-race` and `-count=1`.
Evidence is in `/tmp/milvus-function-chain-call-context-e2e-20261012/` and the
`/tmp/milvus-*request-scope*` / `/tmp/milvus-function-chain-call-context-*` logs.
These are local package and service checks, not remote CI or a repository-wide
test/deployment result. Stopping the isolated standalone still reaches the
unchanged `CloseEtcdClient` path and panics with `context canceled`; graceful
shutdown is outside these request-context checks.

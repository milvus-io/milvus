# External Table Credential Broker Authentication

- **Feature DRI:** @weiliu1031
- **Primary Approver:** Pending selection
- **Independent Approver:** Pending selection
- **Design Review:** Pending

## Scope

External Tables may obtain temporary credentials from an explicitly configured
Credential Broker. AWS, Aliyun and Tencent support Broker IAM and Broker
AssumeRole. Azure retains its SAS Broker flow. Internal Milvus storage does not
inherit this configuration. No schema or task protobuf changes are needed.

## Configuration

```yaml
common:
  security:
    credentialBrokerAllowedEndpoints: '["https://broker.example.com/v1/credentials/assume-role?scope=a,b","https://other-broker.example.com/token"]'
```

The value is a JSON array encoded as a YAML string, not a native YAML list or
comma-separated URLs. Each array element is one complete initial URL, including
its raw query. Commas and nested URLs inside a query do not create additional
allowlist entries. Empty values and `[]` deny every Broker, including existing
Azure Brokers; malformed JSON, non-array values and invalid URLs fail startup.
Configure all components consistently before enabling a Broker or upgrading an
existing Azure Broker deployment. The policy is captured at startup; change
file/environment configuration and restart every affected component to change
it. The management configuration endpoint cannot write or delete this key.
The policy is not an etcd-persisted Immutable parameter, so a first startup does
not pin an empty value above subsequent file/environment configuration.

```json
{
  "format": "parquet",
  "extfs": {
    "cloud_provider": "aliyun",
    "region": "cn-hangzhou",
    "use_iam": "true",
    "credential_endpoint": "https://broker.example.com/v1/credentials/assume-role?scope=a,b",
    "load_frequency": "900"
  }
}
```

`endpoint_url` addresses object storage; `credential_endpoint` addresses the
Broker. Values in extfs remain strings. IAM selects the Broker workload's own
identity, not the Milvus process identity. It rejects role_arn, external_id,
session_name, static keys and other credential modes. With use_iam=false (or
omitted), the three S3 clouds require role_arn for Broker AssumeRole;
external_id is optional. Requested S3 credential lifetime must be 900..3600
seconds. Request timeout remains the storage default.

Milvus Table snapshot metadata and its data files use the same Broker extfs
configuration. `milvus-table` is accepted alongside file formats.

Without a Broker endpoint, native authentication selection is unchanged.
Other cloud providers cannot use the new endpoint in this release.

## Azure compatibility

`azure_credential_endpoint` remains an Azure-only alias. Either spelling is
accepted. If both are present, their normalized URLs must match. Persisted JSON
is not rewritten. The native boundary emits only
`extfs.<collectionID>.credential_endpoint`, as required by the storage
property registry. Azure still requires its client, tenant, account and region
fields and cannot combine SAS Broker with native IAM.

## Endpoint authorization

Normalize scheme and hostname case and remove default HTTP/HTTPS ports. Preserve
path case and trailing slash; normalize an empty path to `/`. Broker paths may
contain only ASCII letters, digits, `-._~` and `/`. Reject percent escapes,
repeated slashes and `.`/`..` segments because the HTTP transport would rewrite
them. Require HTTP(S), a
host and a valid port. Preserve the raw query, including its order, escaping and
an explicitly empty `?`, as part of exact URL identity. Reject userinfo, fragment, whitespace, non-ASCII
characters and backslashes. No wildcard, prefix, suffix or DNS-to-IP matching
is performed. The actual native request uses the checked normalized URL.
The allowlist authorizes a hostname URL, not its future DNS resolution.

Go validation receives an explicit server-owned policy, independent of the
JSON parser. Native property injection rechecks a process-owned policy for
persisted schemas, QueryNode reads, IndexBuilder and Go FFI helpers. Missing
native initialization denies Brokers. Reinitialization with a different policy
fails; publishing the same policy is idempotent.

The allowlist authorizes the configured initial Broker URL. HTTP redirects follow
the storage SDK's existing behavior; Milvus does not re-authorize redirect targets
or impose a Broker-specific redirect policy. Native GCP credential injection
remains endpoint/bucket scoped, and Milvus's GCP request factory skips allowed
Broker endpoints.

External Tables with a Broker endpoint always use the origin filesystem, including
Azure legacy aliases. Native injection forces Talon mode to Disabled and removes
Talon-specific settings for these tables. Other External Tables retain their
configured Talon behavior. Broker tables do not receive Talon acceleration.

The native endpoint allowlist is checked before filesystem construction or reuse.
Broker acquisition and refresh remain local; no Broker descriptor is sent to a
Talon worker and no companion worker patch is required for this feature.

## Application-managed source authorization

Credential renewal and authorization policy have separate owners. Milvus keeps
the configured Broker endpoint in the existing collection schema; the storage
provider obtains and refreshes temporary credentials. An external application
and Broker can decide whose source permissions to check without adding user
ownership metadata or a Catalog-specific authorization hook to Milvus.

This integration pattern requires **trusted application-managed provisioning**.
It does not establish per-user isolation for arbitrary clients that can create
or change external collections under a shared Milvus workload identity.

### Registration and background reads

1. The application authenticates the registration request and checks the chosen
   source table using the source system's authorization API. It stores a durable
   association between its registration, the source table/root and the principal
   or policy to reauthorize in its own protected database. This policy may be
   user-bound or service-bound; Milvus does not select an ownership model.
2. The application provisions the collection with an approved Broker endpoint
   selecting that association. The Broker authenticates its caller through the
   deployment's trusted transport or authentication proxy, resolves the stored
   association, and checks current source permissions before issuing credentials.
   Request account/container and target-identity fields must agree with the
   association; they must not let the caller select another principal or root.
3. Background readers reuse the persisted endpoint after the registration request
   ends or Milvus restarts. On subsequent credential issuance, the Broker repeats
   the source permission check. Where supported, source-system service delegation
   lets the Broker act for the stored principal using renewable workload
   credentials; otherwise the application needs a supported renewable grant.
   Persisting a username alone cannot replace that authorization mechanism.

For example, a deployment can provision the following Azure Iceberg spec for
`external_source = azure://core.windows.net/example-container/warehouse/orders/metadata/v1.metadata.json`.
Resolve the actual metadata location from the authorized source catalog; the
Broker's allowed table root must also cover the referenced manifests and data:

```json
{
  "format": "iceberg-table",
  "extfs": {
    "cloud_provider": "azure",
    "access_key_id": "examplestorage",
    "region": "westus3",
    "azure_client_id": "configured-client-id",
    "azure_tenant_id": "configured-tenant-id",
    "credential_endpoint": "https://broker.example.com/registrations/orders-reader/sas",
    "load_frequency": "900"
  }
}
```

All values are illustrative configuration, not credentials. The account field
`access_key_id` is the Azure storage account name, not an account key. Add the
complete endpoint to `common.security.credentialBrokerAllowedEndpoints` on every
component, using the startup configuration above. Each distinct registration URL
needs its own exact allowlist entry and a restart to change the policy; this
example does not add dynamic registration or wildcard endpoint support. A Lance
table uses `format: "lance-table"` and its own table root and approved endpoint.
Keep the Azure client/tenant fields required by the engine's existing Broker
mode; do not combine this example with native `use_iam` or static storage keys.

The Azure Broker receives an HTTP POST with storage account, container, region,
requested duration and target client/tenant fields. It does **not** receive an
authenticated Milvus user or a collection ID. Client/tenant IDs, the URL and an
opaque registration identifier are configuration, not proof of identity. The
stock request does not acquire or attach an end-user JWT. Workload authentication
to the Broker must be supplied by the deployment, for example by an authenticated
proxy; HTTPS alone authenticates the server, not the caller. Never put user JWTs,
SAS tokens or bearer capabilities in the URL or persisted external specification.

### Protect the association outside Milvus

Only the trusted provisioner may choose or replace the source/Broker tuple.
Enable `common.security.authorizationEnabled` and enforce this with Milvus RBAC
and the application's admission checks, covering create, explicit refresh,
import/restore and replication paths
that can introduce collection metadata. A refresh with a replacement source or
endpoint must pass the same admission checks. Do not give untrusted clients a
broader role that bypasses these checks. The cluster endpoint allowlist controls
which Brokers may be contacted; it is not a per-user or per-collection grant.

In particular, copying another registration's endpoint into a new collection
must be denied at admission. A shared workload reaching the same endpoint looks
identical to the Broker, so the existing request cannot detect that substitution
on its own. Deployments allowing direct, untrusted collection provisioning need
an independently authenticated isolation boundary; this pattern is insufficient
for them. A random or hidden URL does not fix that limitation.

The Broker should issue read-only credentials scoped to the authorized source
and fail issuance if the principal, table or grant is no longer valid. Reuse
table authorization APIs when table permissions are required: a directory's
file-read grant is not automatically equivalent to a table-read grant. Raw file
credentials also cannot enforce row filters or column masking; a deployment must
reject such policies unless another trusted layer enforces them.

### Renewal and validation boundaries

Rejecting new issuance does not revoke a previously issued SAS or undo data
already loaded or indexed by Milvus. Search-caller authorization is a separate
application/Milvus RBAC decision. Cache, retry and refresh behavior belongs to the
pinned storage provider; verify it for the deployed version rather than assuming
each query contacts the Broker. The [3.0 retained-Iceberg renewal fix](https://github.com/milvus-io/milvus-storage/pull/817)
does not establish the behavior of main's C++ provider.

Before enabling this pattern, test original-user allowed/denied registration,
copied-endpoint rejection, source replacement, restart with the external binding
store intact, and retained Lance/Iceberg readers across actual credential expiry.
Exercise Broker denial, outages and source permission revocation through refresh,
index building and query/load. Check that internal generated files still use
internal storage credentials. Configuration validation and a raw storage-reader
test alone do not qualify this complete application integration.

## Dependency compatibility

Upgrade milvus-storage directly to 5c2ea329889e30d94b4e65209c0bc782256e7a95,
including Broker IAM and the Lance V2.0 metadata-version compatibility fix
([storage PR 764](https://github.com/milvus-io/milvus-storage/pull/764)). Retain
the standard SDK I/O backend for internal and external origin filesystems so
the upgrade preserves configured TLS minimums. Reuse
master's milvus-common 1.0.0-a88c21f at recipe revision
0eb9a7f2f5d5fe0e7d37f45f3f9a38ea. Milvus consumes the pinned upstream sources
directly and does not maintain or apply a storage source patch.

Adapt read-only storage APIs to shared immutable Properties. Build or modify
properties before publishing them to readers, writers and filesystem caches.
Retain master's batch loading estimates, including its separate resident,
private overhead and shared overhead accounting. No per-cell compatibility shim
is needed. Keep master's Knowhere b045acd9, the merged version of upstream
[PR 1943](https://github.com/zilliztech/knowhere/pull/1943), which selects the
Cardinal revision compatible with common's batch estimates. Its source tree is
identical to the previously pinned a75a411 revision, so no Knowhere PR-ref fetch
override is needed. The AWS SDK version is retained.

Retain Milvus's existing GCP endpoint and provider selection. Register
operator-configured storage as trusted and check SDK registration failures.
The HTTP factory honors the SDK Bearer-delivery gate and excludes allowed
Broker URLs from GCP credential injection.

Publishing the native Broker policy does not initialize the AWS SDK or change its
process-wide HTTP factory selection. Existing storage SDK initialization, client
configuration and TLS behavior remain unchanged.

## Verification

Go tests cover modes, aliases, exact endpoint matching, malformed configuration,
default denial, immutable startup policy and management mutation rejection.
Native tests cover matching, alias conversion, configuration resolution and
policy rejection before filesystem construction. Broker cases for all three S3
clouds verify origin routing with external Talon modes Disabled, Full and
SmallReads. Azure legacy aliases also force origin and clear Talon settings.

End-to-end validation must exercise discovery/refresh, QueryNode query and
index building, and verify IAM csp=aws/ali/tc with credentialType=iam and no
roleArn/externalId. Broker failure cases include non-success HTTP responses, timeouts, denial,
malformed responses, expiration and failed refresh. Provider retry/cache
behavior is inherited from storage; protocol mocks do not prove live cloud
workload identity permissions. Report each validation boundary separately.

## Replication and rollout

The existing collection-schema JSON carries the Broker endpoint through the
existing create/refresh metadata paths; no new WAL message or payload field is
introduced. The endpoint allowlist is cluster-local deployment policy and is
never carried in a replicated schema. Configure the necessary allowed endpoints
on every receiving cluster before loading Broker-backed data. A destination
without the endpoint authorization fails closed rather than inheriting the
source cluster's permission. Broker identity permissions also remain local to
the destination deployment.

Existing Azure collections need no schema rewrite, but their endpoint must be
added to the allowlist before upgrading. Configure all roles consistently and
restart them to change the policy. A downgrade cannot consume new generic
endpoint configurations; retain the original legacy Azure metadata where
backward compatibility is required.

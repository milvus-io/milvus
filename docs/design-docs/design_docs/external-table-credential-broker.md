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

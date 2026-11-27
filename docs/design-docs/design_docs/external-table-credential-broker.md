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

Broker credential configuration is independent of the external table format.
Go admission and native property injection apply no Broker-specific format
filter. Format reading remains the storage SDK's responsibility.

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

Without a Broker endpoint, native authentication selection is unchanged.
Other cloud providers cannot use the new endpoint in this release.

## Azure compatibility

`azure_credential_endpoint` remains an Azure-only alias. Either spelling is
accepted. If both are present, their normalized URLs must match. Persisted JSON
is not rewritten. The native boundary emits
`extfs.<collectionID>.azure_credential_endpoint` for Azure and
`extfs.<collectionID>.credential_endpoint` for S3 clouds, matching the storage
3.0 property registry. Azure still requires its client, tenant, account and region
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

External Tables with a Broker endpoint use the origin filesystem, including
Azure legacy aliases. The storage 3.0 branch has no Talon filesystem or routing
settings, so no Talon adaptation is required.

The native endpoint allowlist is checked before filesystem construction or reuse.
Broker acquisition and refresh remain local.

## Dependency compatibility

Upgrade milvus-storage directly to the 3.0 branch commit
d140273685a8fd91c4e03d5b1bb943834ed54954
(storage [PR 758](https://github.com/milvus-io/milvus-storage/pull/758)) and milvus-common to 1.0.0-a88c21f at recipe revision
0eb9a7f2f5d5fe0e7d37f45f3f9a38ea. Milvus consumes the pinned upstream sources
directly and does not maintain or apply a storage source patch.

Retain storage 3.0's value-based Properties, transaction and filesystem metrics
APIs. Its non-Broker AWS IAM mode retains the default AWS credential chain.
Retain the existing Rust 1.92 build setup.
Reuse upstream 3.0's batch loading estimates and matching milvus-common
interfaces. No feature-local resource-estimate compatibility helper is needed;
resident, private overhead and shared overhead follow the upstream loaders.
Reuse upstream Knowhere b045acd9, whose source tree
matches the previous a75a411 revision and selects the common-compatible Cardinal
revision. No Knowhere PR-ref fetch override is needed. The AWS SDK is retained.

Keep Milvus's existing GCP endpoint and provider selection. Its HTTP request
factory excludes allowed Broker URLs from GCP credential injection.

Publishing the native Broker policy does not initialize the AWS SDK or change its
process-wide HTTP factory selection. Existing storage SDK initialization, client
configuration and TLS behavior remain unchanged.

## Verification

Go tests cover modes, aliases, exact endpoint matching, malformed configuration,
default denial, immutable startup policy and management mutation rejection.
Native tests cover matching, alias conversion, configuration resolution and
policy rejection before filesystem construction. Broker cases for all three S3
clouds verify SDK configuration resolution. Azure cases verify that both endpoint
spellings, including queries, reach the storage 3.0 Azure property.

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

# GCP target service accounts for external collections

Issue: [#54803](https://github.com/milvus-io/milvus/issues/54803). milvus-storage: [milvus-storage#753](https://github.com/milvus-io/milvus-storage/pull/753).

## Scope

An external collection on GCS can read with three identities today: HMAC
keys, the cluster's own identity (`use_iam=true`), or, in name only, a
target service account (`gcp_target_service_account`). The target mode does
not work. Go validation treats `use_iam=true` and a target as two exclusive
credential modes, while milvus-storage impersonates only when both are set,
so every combination is rejected on one side.

This change makes a target service account usable in two ways:

- **Direct impersonation.** The cluster identity calls the IAM Credentials
  API to get short-lived tokens for the target. The target grants
  `roles/iam.serviceAccountTokenCreator` to the cluster identity. This suits
  self-managed deployments.
- **Credential Broker.** A Broker service gets the tokens for the target and
  returns them to Milvus. Only the Broker identity holds the impersonation
  grant, so a multi-tenant deployment grants one identity per target instead
  of every cluster identity. Milvus uses the Broker protocol it already uses
  for Azure SAS tokens.

Writes, Lance native mutations, and non-GCP providers are unchanged.

## Spec

| Mode | extfs keys |
| --- | --- |
| Cluster identity | `use_iam=true` |
| Direct impersonation | `gcp_target_service_account` (`use_iam=true` optional) |
| Broker | `gcp_target_service_account` + `credential_endpoint` |
| HMAC | `access_key_id` + `access_key_value` |

`credential_endpoint` is the provider-neutral Broker endpoint. milvus-storage
renamed `fs.azure_credential_endpoint` to `fs.credential_endpoint`
(milvus-storage #746), so the C++ spec injection maps the existing
`azure_credential_endpoint` key to `credential_endpoint`. Both keys are
accepted for Azure; they must not differ.

Rules for a target service account, checked by `ValidateExtfsComplete` at
create and refresh:

- `cloud_provider` must be `gcp`.
- The token must go to a Google API host over HTTPS. The endpoint is resolved
  as `InjectExternalSpecProperties` resolves it: `endpoint_url` when set, else
  a cloud endpoint URI host (`gs://storage.googleapis.com/bucket/key`), else
  `storage.googleapis.com`. The host must end with `.googleapis.com`, and
  `use_ssl` must be `true` (or `endpoint_url` must be HTTPS).
- `load_frequency`, the requested token lifetime, must be in [900, 3600]
  when set. The default is 900.
- With `credential_endpoint`, `use_iam=true` is rejected: it selects the
  cluster identity, as it does for Azure.

## milvus-storage

`BuildGcpProviderFromConfig` selects the provider in this order:

1. `credential_endpoint` without `use_iam`: `GcpBrokerCredentialProvider`.
2. `gcp_target_service_account`: impersonation from the cluster identity.
3. `use_iam`: the cluster identity.
4. HMAC.

The Broker request is a JSON POST with `csp=gcp`, `region`, `bucket`,
`durationSeconds` and `gcpServiceAccountEmail`. The response carries the
access token in `credentials.sessionToken` and its expiry in
`credentials.expiredAt` (RFC 3339). The provider caches the token and
refreshes it 60 seconds before expiry, one refresh at a time. The first fetch
retries three times. A failed refresh keeps a token that has not expired yet.
Without a usable token the request is sent without `Authorization`, and GCS
answers 401/403.

The filesystem cache key includes the target and the Broker endpoint, so two
external collections with different targets do not share a filesystem.

### Token delivery

The AWS SDK HTTP factory is process-wide. It finds the provider for a request
by (endpoint, bucket) in `GcpCredentialRegistry`. External filesystems
register providers for endpoints from user specs. The factory therefore
attaches a Bearer token only when the request host is `*.googleapis.com` or an
endpoint the primary filesystem registered (operator configuration). It never
lets an exception out of the SDK callback, because the AWS SDK is built
without exceptions. Milvus's own GCP factory applies the same rule
(#54760).

## Credential policy

An external spec can make Milvus read with credentials that are not the
user's own: the cluster's cloud identity, or tokens a Broker issues for an
account the spec names. Two settings, both read at process start, decide
whether a deployment allows that.

- `common.security.externalCollection.allowServerIdentity` (default `false`).
  Whether a spec may read with the cluster's own identity. A spec uses the
  server identity when `use_iam=true` (any letter case; milvus-storage
  lowercases booleans), when `role_arn` is set (the cluster identity assumes
  the role), or when `gcp_target_service_account` is set without a
  `credential_endpoint` (the cluster identity impersonates the account). A
  target reached through a Broker is not server identity: the Broker's
  identity holds the impersonation grant.
- `common.security.externalCollection.credentialBrokerEndpoints` (default
  empty). Comma-separated Broker URLs a spec may name in
  `credential_endpoint` or the older `azure_credential_endpoint`, which the
  C++ injection folds into the same property before the check. An entry
  matches when it equals the spec value after trimming surrounding whitespace
  and a trailing `/`. The empty default disables Brokers. `allowServerIdentity`
  does not stand in for this list.

Enforcement happens twice. The Go admission path checks the two settings at
create and refresh, so a user gets the error from the request that names the
credential. The C++ spec injection (`InjectExternalSpecProperties`) checks
them again every time an external filesystem is opened: segcore loads on the
QueryNode and StreamingNode, index builds on the DataNode, and the Go FFI
explore path on the MixCoord. The second check covers specs persisted before
the settings changed, and it fails closed: a process that has not published
its policy (`SetExternalCollectionCredentialPolicy`, called from
`initcore.InitExternalCollectionCredentialPolicy` in every role) rejects
every spec that needs a policy decision. Specs that need none, such as access
keys or anonymous access, are not affected by either setting.

The consequence of the defaults: after the upgrade, an existing collection
that reads with the server identity or through a Broker fails to load and to
refresh until the operator sets the keys on every Milvus process. The error
names the setting and the collection.

Azure note: an external Azure filesystem with an attached identity (Broker
or `use_iam`) must use the standard Azure storage authorities
(`core.windows.net` and the sovereign-cloud equivalents) over HTTPS. The
token is sent to the authority the spec resolves to, so a custom authority
would receive it.

## Compatibility

- Specs that already store a target service account were accepted but could
  not read. They now read after an upgrade, through the same token delivery
  rule and subject to the credential policy. A stored target spec without
  `use_ssl` is accepted: a missing `use_ssl` takes the scheme default, which
  is `true` for `gs://`. A stored spec whose `load_frequency` is outside
  [900, 3600] is rejected by milvus-storage when the provider is built and
  must be resubmitted with a value in range.
- Azure Broker specs keep working after the milvus-storage version that
  renamed the endpoint property, provided their Broker is listed in
  `credentialBrokerEndpoints`.

## Limitations

- One GCP identity per (endpoint, bucket) per process. Two external
  collections on the same bucket with different targets share whichever
  provider registered last. A deployment must map a bucket to one target.
- The admission rules apply to create and refresh. Persisted specs rely on
  the token delivery rule and on the credential policy check at file open.
- On QueryNode and StreamingNode the Bearer-injecting HTTP factory is
  installed only when Milvus's own storage is GCS with `useIAM`. In any other
  configuration no factory attaches the token, so a GCP Bearer external
  collection (`use_iam`, target or Broker) there gets 401 from GCS. HMAC
  external collections are unaffected because the SDK signs them itself.
  Milvus's own factory needs #54760 to route tokens per bucket.
- The 3.0 branch of milvus-storage still reads Lance and Iceberg through Rust
  object stores with their own GCP credentials. A 3.0 backport needs the
  Broker there too.

## Release note

Upgrade prerequisite for managed deployments. Both settings default to the
restrictive value and are read at process start, so set them in `milvus.yaml`
on every Milvus process before the upgrade where the deployment already has:

- AWS or Aliyun external collections with `role_arn`, or any external
  collection with `use_iam=true` or a GCP target without a Broker: set
  `common.security.externalCollection.allowServerIdentity: true`. Otherwise
  those collections fail to load and to refresh after the upgrade.
- Azure or GCP external collections that use a credential Broker: list the
  Broker URL in `common.security.externalCollection.credentialBrokerEndpoints`
  (comma-separated when several Brokers are deployed). Otherwise those
  collections fail to load and to refresh after the upgrade.

Deployments without such collections need no change. The load and refresh
errors name the setting and the collection, so a missed collection can be
found from the logs.

## Tests

- Go: `TestValidateExtfsComplete_GCPTargetServiceAccount`,
  `TestValidateExtfsComplete_AzureCredentialEndpointKeys`.
- C++: `InjectExtfsAllowlist.GcpCredentialBrokerProperties` and the updated
  Azure case; `ExternalCredentialPolicy.*` for the server-identity gate
  (`use_iam` in any letter case, `role_arn`, target without Broker), the
  Broker allowlist (unlisted, listed, trailing slash, `allowServerIdentity`
  not substituting), `azure_credential_endpoint` mapping and gating, the
  uninitialized policy, the C ABI path, and
  `GcpProvidersResolveFromInjectedProperties`, which resolves the injected
  properties through `FilesystemCache::resolve_config`, builds the
  impersonation and Broker providers, and looks them up in
  `GcpCredentialRegistry`.
- milvus-storage: Broker contract, caching, refresh failure, retries,
  invalid responses, provider selection, registry host rule, config
  validation and cache key.

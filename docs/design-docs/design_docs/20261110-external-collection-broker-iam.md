# External collection Broker IAM across cloud providers

Issue: [#54803](https://github.com/milvus-io/milvus/issues/54803).
This extends the Broker framework from [#54986](https://github.com/milvus-io/milvus/pull/54986)
and the native credential APIs from [milvus-storage #779](https://github.com/milvus-io/milvus-storage/pull/779).
[Storage #785](https://github.com/milvus-io/milvus-storage/pull/785) supplies
Aliyun/Tencent target-role request routing for the native S3 filesystem. Until
it merges, this change pins its immutable fork revision; restore the upstream
merge commit before merge.

## Common mode selection

A non-empty `credential_endpoint` selects a Broker. Within that path,
`use_iam=true` requests the Broker's own cloud identity; `use_iam=false` or an
omitted flag requests a supplied target identity. The same rule applies to
AWS, Aliyun, Tencent, GCP and Azure.

| Cloud | Broker IAM (`true`) | Target mode (`false` or omitted) |
| --- | --- | --- |
| AWS | Broker identity | `role_arn`, optional `external_id` |
| Aliyun | Broker identity | `role_arn`, optional `external_id` |
| Tencent | Broker identity | `role_arn`, optional `external_id` |
| GCP | Broker identity | `gcp_target_service_account` |
| Azure | Broker identity obtains SAS | `azure_client_id` + `azure_tenant_id` obtain SAS |

Resource fields still depend on the cloud. S3-family requests require a region.
Azure needs a storage account name in `access_key_id` and a container from the
source URI; its target mode also requires region, while IAM mode may omit it
when the source already names the storage authority
(`azure://core.windows.net/<container>/[<path>]`). A container root keeps its
trailing slash: the storage URI parser needs the separator after the
container. Azure IAM admits two source shapes, both over HTTPS:
`azure://<authority>/<container>/[<path>]`, where the authority is one of the
four Azure Storage authorities in lowercase without a port and the container
is followed by `/`, and `azure://<container>/[<path>]` with a region. Container
names follow the Broker's naming rule and are read from the raw source, as
native injection splits it: a percent-encoded separator or letter is not the
container. Admission errors name the host and container, never the source's
query or fragment, which may carry a signed token. When the source names an
authority, that authority is the address;
[#55484](https://github.com/milvus-io/milvus/pull/55484) makes native
injection prefer it over a region-derived one. Target mode
predates this change and keeps its existing source admission. Without a
region, every Azure mode also admits a dotted host at a container root
(`azure://<host>/<container>/`), which names the endpoint as a longer path
already did. The account name is not a secret key. Both Azure modes return
SAS, not an account key.

IAM mode rejects target fields rather than silently ignoring them. Both Azure
modes reject `access_key_value`, `role_arn`, a GCP target, and anonymous mode.
Azure IAM also rejects `external_id` and `session_name`. Azure target mode
predates this change and keeps accepting those two fields without reading
them: refresh revalidates persisted specs, so a stricter rule would fail
collections that load today. GCP Brokers reject access keys, S3 role/session
fields and Azure identity fields. Go admission is the complete check. Native
injection re-checks persisted specs for the Broker allowlist and IAM/target
conflicts, and retains the existing Azure endpoint alias. It fails on an extfs
value that is not a JSON string (null counts as unset) instead of skipping the
field, because `credential_endpoint` and `use_iam` select the credential
source.

Without a Broker endpoint, existing direct node IAM and AssumeRole behavior
is preserved. A GCP target also accepts `use_iam=true` without counting the
two fields as separate credential modes. On Azure,
[#55318](https://github.com/milvus-io/milvus/pull/55318) admits the node's
workload identity as `use_iam=true` with the account name and no endpoint, so
removing the endpoint from an Azure Broker IAM spec selects the node identity
instead of failing admission.

Native injection writes empty `access_key_id` and `access_key_value` for every
external filesystem before applying the spec. milvus-storage builds an
external filesystem only from its `extfs.<collection>.*` keys, and a missing
key takes the library default (`minioadmin`). That default would pose as
explicit HMAC keys; GCP rejects HMAC keys next to a target service account or
Broker while Talon is disabled.

## Producer contract

`use_iam` means different things with and without a Broker. Without an
endpoint, a target field (`role_arn`, `gcp_target_service_account`) absorbs
`use_iam=true`, and node identity applies only when no target is set. With an
endpoint, `use_iam=true` selects the Broker's own identity, which may carry
broader permissions than any node identity. Components that generate external
specs on behalf of users must therefore:

- set `use_iam=true` next to a Broker endpoint only when they intend the
  Broker's own identity;
- drop `use_iam` before adding a Broker endpoint to a target-identity spec;
  admission rejects `use_iam=true` together with target fields instead of
  choosing one;
- write target identities under their kernel keys (`role_arn`,
  `gcp_target_service_account`, `azure_client_id` + `azure_tenant_id`).
  Admission rejects unknown extfs keys, so a misnamed target fails the
  request; a target that is left out next to `use_iam=true` and an endpoint
  selects the Broker identity without an error;
- build `extfs` from an explicit key allowlist and never copy user-controlled
  parameters into it verbatim, because `credential_endpoint` and `use_iam`
  select the credential source.

Admission does not add a separate mode selector, which keeps existing
`use_iam=true` Broker specs compatible. An explicit selector, or allowlist
entries that state which modes an endpoint may serve, would make the Broker
identity opt-in instead of relying on producers; that is left for follow-up.

## Transport and lifetime

All Brokers use `common.security.credentialBrokerAllowedEndpoints` and its
complete normalized initial URL identity, including raw query. Existing URL
normalization and redirect handling remain in effect.

GCP target and Broker credentials require HTTPS and a storage host under
`*.googleapis.com`. Their requested token lifetime is in [900, 3600] seconds.
Ordinary GCP node IAM retains its existing endpoint behavior, including
operator-trusted custom GCS endpoints. Azure's storage producer continues to
restrict credential-bearing external filesystems to Azure Storage authorities
over HTTPS; new Azure Broker IAM accepts lifetimes in [900, 3600]. Existing
Azure target-mode lifetime behavior is preserved.

A checked lifetime (`load_frequency`) is a plain decimal number of seconds.
milvus-storage parses it without accepting a sign, so admission and native
injection reject `+900` instead of letting every read fail.

Cached credentials use the response's actual expiry, not an assumed full
requested lifetime. A Broker error must not fall back to node credentials.

## Protocol and rollout

IAM requests include `credentialType=iam` and omit target identity fields.
Target requests retain their role/service-account/client identity fields and
omit `credentialType`. Both S3-family modes use the cloud-specific CSP alias:
`aws`, `ali`, or `tc`; Aliyun/Tencent target requests must not be sent as AWS.
The native S3 filesystem uses the alias in both modes. An S3-family target
request carries `role_arn` and an optional `external_id`. `session_name` stays
admitted in target mode for specs written for direct AssumeRole, but it is not
sent: the Broker names the session. Talon accepts
Aliyun/Tencent only in IAM mode: its Broker backend accepts S3 target roles
only with `csp=aws`, so the Talon bridge rejects Aliyun/Tencent target roles
before any request. Milvus disables Talon for every external filesystem with a
Broker, so external collections always acquire Broker credentials natively.

The Broker server must implement both modes for the requested cloud. Deploy
support for Aliyun/Tencent target-role requests before consumers start sending
those CSP aliases. The new handlers use the Broker workload's own temporary
credentials to call the vendor STS AssumeRole API, validate the target and
request duration, and return the actual temporary-credential expiry. They
must not interpret a target request as an IAM request or return the Broker's
own credentials when target-role acquisition fails.

The allowlist authorizes Broker URLs only. Milvus does not authenticate to
the Broker, and the Broker contract does not bind a caller to the target
identities or buckets it may request. Anyone who can create or alter an
external spec with an allowlisted endpoint can therefore request the Broker
identity or any target that trusts it; target roles are additionally guarded
only by their `external_id`. Deployments must restrict who can write such
specs (for example, a front end that generates `extfs`) and which workloads
can reach the Broker. This change does not add a global server-identity
permission switch.

## Validation and limits

Cover true, false and omitted IAM flags for all five cloud providers at
admission, plus missing/unused fields, unlisted endpoints and alias conflicts.
Exercise native property/config selection, including GCP provider
construction for target impersonation with and without a Broker, mocked
Broker payloads for both modes, caching/expiry, and failure without identity
fallback. Server tests
verify vendor AssumeRole requests with temporary source credentials and
sanitized failures.

The one-GCP-identity-per-(endpoint,bucket) registry restriction and mixed
GCP/S3 HTTP-factory initialization limitation remain. Mocked tests and source
inspection do not establish production deployment or live five-cloud E2E.

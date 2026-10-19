# Explicit AWS IAM credential sources

The default AWS credential chain can cache the EC2 instance-profile provider after a transient Web Identity failure. As long as the node provider keeps returning credentials, S3 AccessDenied does not cause the chain to retry the Pod identity.

For IRSA deployments, select the credential source explicitly:

```yaml
minio:
  cloudProvider: aws
  useIAM: true
  iamCredentialSource: web_identity
  region: us-east-1
```

`web_identity` requires `AWS_ROLE_ARN` and `AWS_WEB_IDENTITY_TOKEN_FILE`. The region comes from `minio.region`, `AWS_REGION`, or `AWS_DEFAULT_REGION`. It selects only the STS Web Identity provider; missing tokens or failed STS calls never select environment, profile, container, or EC2 credentials. Token contents are reloaded by the SDK for refresh.

`instance_profile` selects only the EC2 instance-profile provider. It ignores ambient Web Identity and static environment credentials. `default` (also an omitted field in older RPC messages) preserves the previous behavior. Unknown sources and explicit sources combined with disabled IAM or a non-AWS provider fail configuration validation.

The source is propagated through index-build and clustering RPCs, Go object-storage and access-log clients, Cgo, C++ chunk managers, and the milvus-storage filesystem properties. Filesystem caches distinguish sources. Changing the setting requires a restart; it does not repair already-created default-chain clients.

Transient retrieval failure remains an error from the selected provider. Retrying after STS/IMDS recovers uses the same source. Initialization paths that already require an accessible bucket can still fail startup when credentials cannot be acquired. This change does not guarantee uninterrupted I/O during an STS outage.

Scope: this setting controls Milvus's object-storage clients. Woodpecker WAL uses its own client and does not yet consume this field. External-table `role_arn` and other explicit external credential modes are independently configured; this setting does not impose a cluster-wide identity policy or pin an exact role ARN. EKS Pod Identity (`container` credentials) is not included in this first version.

Before rollout, validate STS failure and recovery on the actual deployment, with a reachable IMDS endpoint and an EC2 role that lacks bucket access. The Pod mode must never send S3 requests signed with that node role. Upgrade all participating Milvus nodes before enabling the field; older binaries ignore the additive RPC field.

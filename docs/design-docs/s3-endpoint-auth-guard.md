# S3 endpoint authentication guard

## Scope

This is a Milvus-side mitigation for the legacy `MinioChunkManager` /
`AwsChunkManager` startup crash, not a patch to AWS SDK or a change to region
selection. The implementation is based on master `3cd8755ee2`, whose Conan recipe
pins AWS SDK C++ 1.11.842.

The observed failure sequence is endpoint authentication properties failing JSON
parsing, an empty signer name, and a native crash in `AWSClient::AttemptOneRequest`.
The SDK's attributes parser can return an object with an empty authentication
scheme on parse failure. Its endpoint provider still returns success. The SDK
then uses the empty name instead of its default signer. The registry asserts in
Debug or returns null in Release; the request code does not check that pointer.

Explicitly configuring the correct region resolved the reported deployment.
However, the original source of the malformed authentication properties is not
proven. In particular, the hypothesis that an unexpected metadata response became
the effective region must not be presented as a confirmed incident root cause.

## Change

`GuardedS3EndpointProvider` decorates the client's **already initialized** endpoint
provider. It forwards both initialization overloads, endpoint overrides, and
client context access. On resolution it:

1. Passes through existing resolution errors.
2. Rejects a successful result whose attributes exist but whose auth scheme name
   is empty, using non-retryable `CoreErrors::ENDPOINT_RESOLUTION_FAILURE`.
3. Preserves other results, including absent attributes (SDK default signer) and
   explicitly named signing schemes. It does not introduce a signer allowlist.

`MinioChunkManager::PreCheck` installs this decorator before its first ListObjects
call. This common entry point covers the legacy S3-backed constructors in
`storage/ChunkManager.cpp` and `storage/minio/MinioChunkManager.cpp`. Installation
is idempotent and must occur before exposing the client to concurrent users. The
same provider remains installed for subsequent storage operations.

No endpoint, region, credentials, addressing mode, checksum policy, or anonymous
fallback is changed. The added diagnostic contains configuration key names, not
raw metadata, credentials, or the malformed properties value.

## Error propagation audit

The following chain was inspected; no shared error mapping or wire contract is
changed by this patch:

| Stage | Behavior |
| --- | --- |
| SDK properties parser | Malformed JSON leaves the auth scheme name empty. |
| SDK default endpoint provider | Can publish those attributes in a successful outcome. |
| Milvus decorator | Converts that success to non-retryable endpoint-resolution failure. |
| S3 ListObjects / InvokeServiceOperation | Checks endpoint outcome before signing; its failure macro retains the message and returns endpoint-resolution failure with retry disabled. |
| MinioChunkManager::ListObjects / ThrowS3Error | Existing `S3ErrorToErrorCode` maps this permanent failure to `UnexpectedError`; no new code is introduced. |
| MinioChunkManager::PreCheck | Preserves the typed SegcoreError code while adding context. |
| CreateLegacyChunkManager / RemoteChunkManagerSingleton::Init | Failed construction does not publish a chunk manager. |
| InitRemoteChunkManagerSingleton | Uses `CGO_CATCH_AND_RETURN_CSTATUS`; the typed exception goes to `FailureCStatus`. |
| initcore.HandleCStatus / InitQueryNode | Uses the existing `merr.SegcoreError` conversion and returns initialization failure. |

This remains a system initialization failure, not a user-request InputError.
A failed initialization may still stop the process and cause Kubernetes to
restart the pod. Preventing the native crash does not make an invalid storage
configuration usable.

## Verification (2026-09-22)

The repository test source `internal/core/unittest/test_s3_endpoint_auth.cpp` was
compiled and run in an isolated macOS arm64 build against the official upstream
AWS SDK tag 1.11.842 (`303a20b6305e7bde7bdb03e1e3ee8297e0fa09da`). Both the SDK and
test executable were built separately in Debug and Release with AppleClang 16.
This is not the production Conan binary. An in-memory HTTP implementation was
used; no production S3 or IMDS endpoint was contacted.

| Check | Debug | Release |
| --- | --- | --- |
| Eight SDK-facing regression tests | 8 passed | 8 passed |
| Same malformed-properties request with guard condition disabled in a temporary header | SDK assertion / signal 6 | SIGSEGV / signal 11 |
| Same empty-auth request through the credentials-provider constructor, guard disabled | SDK assertion / signal 6 | SIGSEGV / signal 11 |

The eight tests cover:

- Actual SDK JSON parsing failure followed by ListObjects, with zero HTTP sends.
- Empty auth through the credentials-provider constructor, with zero HTTP sends.
- Malformed effective region rejected without an HTTP send.
- Preservation of named SigV4, SigV4a, S3 Express, NullSigner and absent attributes
  at the provider boundary (not full end-to-end authentication for all schemes).
- Preservation of a delegate's error, message and retry flag at the provider
  boundary; the SDK's existing outer error conversion is not changed.
- Normal signed ListObjects and non-retryable HTTP 403.
- Retryable S3 SlowDown / HTTP 503 and network connection errors.
- Default region, path-style / virtual-host addressing, endpoint override and
  both initialization overloads.

The malformed-region test also passes with the guard disabled in this SDK: newer
endpoint rules reject this particular input earlier. It is a compatibility test,
not evidence that the incident's original region source has been reproduced.
The malformed-properties and empty-auth negative controls are the proof that the
new guard changes the dangerous behavior.

`git diff --check` and formatting checks were also run. A separate native
integration test, `test_minio_endpoint_auth.cpp`, deliberately does **not** install
the guard in its setup. It invokes real PreCheck, ListObjects and the CStatus catch
tail, so it will detect removal of the production installation call. It is
registered in `all_tests`, but **has not been compiled or run locally** because the
full Milvus native dependency/build environment is unavailable.

Before merging, build Milvus with its pinned Conan dependencies and run:

```sh
all_tests --gtest_filter='S3EndpointAuthGuardDeathTest.*:MinioEndpointAuthGuardDeathTest.*'
```

The complete cgo/Go startup path and a Linux deployment smoke test remain to be
executed. No claim is made that the full Milvus build or production upgrade has
been verified. No shared merr code, wire projection, oldCode or metric label was
changed.

## Limitations and follow-up

- Only empty auth-scheme names on this legacy S3 client path are intercepted.
  Unknown **nonempty** signer names and failures inside the SDK before endpoint
  resolution returns are not generically repaired.
- This does not patch independent clients owned by Arrow / milvus-storage or
  other applications using the SDK.
- The generic null-signer guard and debug assertion behavior should still be
  fixed upstream in AWS SDK and delivered through its dependency recipe.
- Keep the explicit valid region workaround in the affected deployment.

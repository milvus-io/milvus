# Failure-path audit for the native prototype

This change preserves existing Search outcomes; it does not introduce error-code
reclassification or a new retry policy. The following paths were traced/read and
exercised where noted:

- Request size/count/IDs: executor rejects before dispatch using existing input
  error factories. Empty/malformed cases are unit-tested; oversized payload,
  duplicate IDs and 129-item envelopes are tested over gRPC.
- Admission saturation: `ErrServiceTooManyRequests` is returned as an envelope
  status. Existing sentinel code/retriability is preserved; concurrent unit test
  checks rejection and subsequent permit release.
- Missing database: ordinary Search metadata resolution returns an existing status;
  executor retains that `SearchResults`. Native SDK e2e observes the failing item
  alongside successful items in another database.
- Bad vector dimension: QueryNode/native validation -> Search result status ->
  envelope item -> SDK error and original status. E2e verifies failure isolation.
  No claim is made about uniform classification across every native error origin.
- Envelope authentication: missing credentials and a wrong password call the native
  MultiSearch RPC directly and both return gRPC Unauthenticated before dispatch.
- RBAC: shared Search entry -> database/collection privilege check -> gRPC
  PermissionDenied -> item grpc_code=7. Same collection name in two databases,
  with permission granted in only one database, is tested end-to-end.
- Quota: shared rate-limit interceptor uses each SearchRequest's DB/collection
  and request count, checks the limiter, and builds the ordinary failed response.
  Full status preservation (including ExtraInfo) is unit-tested. Dynamic quota
  rejection injection on the v3.0.5 instance was not repeated in this run.
- Deadline/cancel: parent context -> permit acquisition and child Search -> parent
  transport deadline. Waiting-for-permit cancellation is unit-tested; tiny client
  deadline and a subsequent successful native request are exercised end-to-end.
  Return of Search is not proof of immediate downstream cancellation cleanup.
- Response overflow: executor cancels its context, waits for workers, returns an
  envelope size error. Unit test verifies the error and permit cleanup.
- Nil result/status: executor constructs an internal error instead of treating an
  absent result as success; unit-tested. No existing upstream code is remapped.
- Search's normal result status fields pass through unchanged. Item gRPC errors
  retain code/message only; arbitrary gRPC details/trailers are not preserved.

No generic retry-benefit, PSC recovery, cloud billing/gateway compatibility,
cluster-wide fairness, or immediate backend cancellation guarantee is claimed.
The earlier paused-container load experiment was discarded after embedded etcd
leases expired; it is a harness failure, not a MultiSearch performance result.

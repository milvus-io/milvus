# CPU ONNX execution infrastructure

This document describes the manager and tensor runner for issue #53844 in
`internal/util/function/onnx`.

## Problem

Embedding and reranking functions need one CPU model execution path. Loading a
model for every request is expensive. An unbounded cache or executor can use
too much native memory or too many worker threads. The runner also has to
reject malformed tensors before they reach the runtime.

## Scope

The package contains:

- a `Backend` adapter and `Session` contract for a pinned CPU ONNX Runtime
  binding;
- content-addressed session identity using model SHA-256, backend name and
  runtime version;
- serialized, cancellation-aware loading with resident-session, model-size,
  native-memory and inference-concurrency limits;
- leases that keep sessions alive during inference and close idle sessions
  after the configured TTL; and
- tensor signature and dense-payload validation for inputs and runtime outputs.

The package does not include an ONNX Runtime binding, tokenizer, pooling, GPU
execution, model download, or Function integration. A deployment supplies a
backend whose name and version identify its runtime build and execution
provider configuration.

## Usage

The runtime adapter implements `Backend.Load` and returns a `Session` with the
model's input and output signatures. The caller creates one manager and runner
for the component, then runs each request through `Runner.Run`. `Lease` keeps a
loaded session alive for the duration of one inference. `FuncBackend` is a
small bridge for adapters that already expose a loader function; the package's
example shows the lifecycle without requiring a native runtime in unit tests.

## Lifecycle and invariants

1. `Acquire` checks the path and size before hashing the model. It rechecks the
   digest after the backend returns. A replacement during load is rejected and
   the new session is closed.
2. Only a lease holds a positive session reference. Session removal and lease
   acquisition are synchronized by the manager mutex, so closing the manager
   cannot strand a reference or leak a native session.
3. Loads use a cancellable gate. A caller waiting behind another model load can
   return when its context is canceled.
4. Resident memory is charged only after a session passes signature validation.
   Evicted sessions are removed from accounting under the manager mutex and
   closed after unlocking it.
5. Inference admission uses a weighted semaphore. Cancellation while waiting
   for admission returns without invoking the backend.
6. A caller tensor must match name, type, rank, every static dimension and
   dense payload byte length. A malformed runtime output is a function failure
   while preserving the underlying validation cause for in-process inspection.

## Error boundaries

Path and tensor-shape errors use `ErrParameterInvalid`. A missing model path is
a parameter error. Permission, read, and verification failures use the I/O
family, so callers can distinguish transient storage failures. Runtime
signature and output violations use `ErrFunctionFailed`. `merr.Wrap` keeps
typed backend errors intact.

## Validation plan

Focused tests cover session reuse, content changes, single-load behavior,
idle release, memory and concurrency limits, admission cancellation, inference
failures, malformed outputs, unknown runtime types, oversized models and
replacement during load. Race-enabled tests and `go vet` are required for the
package.

The package tests use a fake backend. They do not claim ONNX Runtime numerical
parity or native executor integration; those require a pinned runtime adapter
and reference-model fixtures supplied by the deployment integration.

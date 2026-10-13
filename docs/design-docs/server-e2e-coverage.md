# Go and C++ coverage for server E2E tests

Related issue: https://github.com/milvus-io/milvus/issues/54200

## Problem and scope

SDK and REST tests exercise a long-running server across Go and C++ boundaries.
Unit-test coverage does not measure those server processes. This design provides
an opt-in Linux/GCC CPU build and a local snapshot protocol. It does not change
normal server builds or add an HTTP/RPC endpoint. Rust, third-party coverage,
crash recovery, GPU/Clang builds, and CI report hosting are outside this change.

## Build contract

In a configured Milvus Linux build environment:

```bash
# Use a dedicated build tree; do not reuse libraries from an ordinary build.
jobs=8 CMAKE_BUILD_PARALLEL_LEVEL=8 CARGO_BUILD_JOBS=4 \
  make USE_SERVER_COVERAGE=ON
bash scripts/package_server_coverage.sh /tmp/milvus-coverage-build
```

`USE_SERVER_COVERAGE=ON` enables `go build -cover -covermode=atomic` for the
server, including linked packages in the Milvus module and its submodules. The
`servercoverage` build tag includes the collector and the native bridge. The
normal build uses a no-op startup hook.

CMake's `BUILD_SERVER_COVERAGE` instruments the core with GCC coverage and atomic
counter updates, independently of `BUILD_UNIT_TEST`. It disables compiler
launchers that might restore objects without their `.gcno` sidecars. Existing
unit-test coverage behavior is preserved. Unsupported server coverage toolchains
fail configuration. Build farms must isolate coverage and ordinary build caches.

The packaging script preserves `.gcno` paths and the source revision, and copies
the matching `gcov` and Go `covdata` decoders. Keep this bundle paired with the
exact server binary and shared libraries. A source update requires rebuilding
all of these artifacts; coverage data must not be mixed across builds.

## Runtime contract

Use a fresh, writable absolute directory for each process:

```bash
export MILVUS_COVERAGE_DIR=/tmp/milvus-e2e-coverage
mkdir -p "$MILVUS_COVERAGE_DIR/go"
export GOCOVERDIR="$MILVUS_COVERAGE_DIR/go"
./bin/milvus run standalone
```

When `MILVUS_COVERAGE_DIR` is unset, the local snapshot collector is inactive.
When set, the collector configures `GCOV_PREFIX`, `GCOV_PREFIX_STRIP=0`, and
`GCOV_ERROR_FILE`. Conflicting values are rejected. The supervisor used by
`--run-with-subprocess` does not start a collector; the actual server owns it.

The collector checks Go instrumentation and publishes `ready` with its PID.
An existing `ready` marker or counters/requests from a previous process cause
startup to fail, requiring a fresh directory. This prevents silent mixing after
a restart. A container image should supply metadata separately from writable
runtime output; a pod restart needs a new process directory even on a shared
volume.

After E2E clients finish, stop issuing test traffic and request a snapshot:

```bash
snapshot=$(tr -d '-' < /proc/sys/kernel/random/uuid)
/tmp/milvus-coverage-build/bin/coverage-export "$snapshot" \
  /tmp/milvus-coverage-build/coverage > coverage.tar.gz
mkdir coverage-result
tar -xzf coverage.tar.gz -C coverage-result
```

The exporter must run in the server's PID and filesystem namespace, for example
through `kubectl exec` in the server container. Copy the metadata/tools bundle
into that container or mount it read-only. `coverage-gcov` also needs the matching
runtime libraries from the builder; include them when using a minimal image.
The server itself does not need the source checkout or reporting tools.

For remote collection, save the archive inside the container before downloading
it. A failed stream must not be mistaken for a valid archive. Verify its size and
checksum after transfer, and retain raw data when report conversion fails.

## Snapshot protocol and artifacts

Create an empty file `requests/<id>`, where `<id>` is 32 lowercase hexadecimal
characters. The collector processes requests serially and creates an exclusive
`runs/<id>` directory. The same ID cannot overwrite a previous snapshot.

The collector writes Go metadata/counters, explicitly dumps the C++ runtime,
and copies `.gcda` files preserving their paths. `done.json` is atomically
published only after these steps succeed; otherwise `error` describes the
failure. The caller must treat missing completion as failure/timeout. A native
dump error permanently rejects subsequent snapshots from that process because
the failed interval may have been lost.

The exporter converts Go data into `go.out`, pairs C++ counters with `.gcno`
files, and produces gcov JSON, including units with no hits. It archives these
alongside raw data, `done.json`, and `build-info.txt`. It preserves the snapshot
on disk for inspection; the caller owns cleanup. Keep the process's `cpp/`
accumulator until the server has finished, even after deleting individual runs.

Coverage is cumulative since process start, including startup and background
work. Go counters are not cleared. GCC dumps merge an interval into existing
`.gcda` files, then reset/re-arm counters for the next dump. Go and C++ snapshots
are not a globally atomic transaction. Quiesce test traffic before collection;
counts during concurrent background work are best effort. Use fresh processes
when measuring isolated E2E suites. Do not sum successive snapshots of the same
process, which would count earlier work more than once.

A crash, OOM kill, forced termination, or removed pod can lose unexported data.
This mechanism neither stops the server nor guarantees crash persistence.

## Distributed reporting

Collect one snapshot from every participating Milvus process before teardown,
including proxies, coordinators and worker nodes. Record pod UID, container ID,
restart count, image digest, role, and source revision in the orchestration layer.
Reject a complete-run claim if participating processes restart, disappear, or
change image during the test/collection window.

Merge raw Go data from distinct processes of the same build with `go tool
covdata merge`, then use `textfmt`/`percent`. Aggregate gcov JSON by source path
and line, or convert it with a compatible reporting tool. Preserve zero-hit
units in the denominator. Restrict C++ reports to Milvus-owned source paths and
exclude generated code/third-party dependencies as appropriate. Exclude the
collector itself when reporting application Go coverage.

Report Go statement coverage and C++ line coverage separately. They have
different denominators and must not be averaged into a single percentage.

## Validation

Collector tests exercise raw snapshot copying, duplicate IDs, malformed IDs,
conflicting/stale directories, Go writer failures, absent native counters,
copy errors, and persistent native dump failures. Full validation also requires
an instrumented Linux/GCC build, a live SDK workload against that build, and
successful report decoding with observed hits in both Go and C++ business code.
Normal builds must continue to compile without the collector/native hook.

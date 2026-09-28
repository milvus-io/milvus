# Native ThreadSanitizer builds

## Scope

`USE_TSAN` is an opt-in CMake option for 64-bit Linux CPU builds with LLVM 20 (Clang, compiler-rt,
libomp and Archer). It instruments Milvus C/C++ targets and CMake source dependencies with
`-fsanitize=thread`, debug information, and frame pointers. It is independent of
`BUILD_UNIT_TEST` and defaults to `OFF`. ASan and TSan cannot be combined.

This is a native-code diagnostic build, not a claim of whole-process race
coverage. Go `-race` is a separate build. Stable Rust/Cargo builds and ordinary
prebuilt Conan packages do not become instrumented when this option is enabled.
GPU builds are rejected. Runtime compatibility and dependency coverage must be
validated for the particular compiler, container, and dependency versions used.

## Build interface

The existing build entry points propagate the option:

```bash
make USE_TSAN=ON mode=RelWithDebInfo build-cpp-with-unittest
make USE_TSAN=ON mode=RelWithDebInfo install
```

These are commands for the Linux build environment with LLVM 20 installed.
`MILVUS_LLVM_ROOT` defaults to `/usr/lib/llvm-20`; the build scripts select its
Clang drivers and fail if the compiler-rt, libomp or Archer library is absent.
The native/cgo links use `-shared-libsan` so DSOs and the Go executable share
one LLVM TSan runtime. The C++ ABI remains `libstdc++11`. With a TSan-capable
Milvus Dev CLI client and server, the remote development workflow is:

```bash
milvus-dev-cli build local . -b master --tsan --debug
milvus-dev-cli cpp-ut local . -b master --tsan -j 1 -t bitset_test
```

The worker runs the CMake smoke fixture before the build, isolates native and
Conan caches from ordinary/ASan builds, and verifies installed dependency
instrumentation. TSan worker compiler parallelism defaults to eight jobs;
test sharding is a separate setting. A local `USE_TSAN` environment variable
does not automatically forward the option to a remote job.

The direct Core script accepts `-T ON`. Direct CMake users pass
`-DUSE_TSAN=ON` along with their existing Conan toolchain, install prefix and
other configuration arguments. No CLI-specific configuration is needed inside
CMake. Use a separate worktree/build/install tree and Cargo cache from ordinary
or ASan builds, including when using remote build caches. CMake rejects changing
to or from TSan in a build/install tree already marked with another mode.
For direct CMake invocation, first prepare the parser prerequisite with
`USE_TSAN=ON bash scripts/build_plan_parser.sh`; CMake cannot change an already
built parser wrapper.

`cmake/Sanitizers.cmake` is loaded before dependency and source subdirectories.
Its directory compile options reach the OBJECT libraries aggregated into
`milvus_core`; flags attached only to the final shared library would not
instrument those objects. Link options reach shared libraries and executables.
Both C and C++ compiler/runtime links are checked during configuration.

ASan flags are centralized in the same module. The Tantivy C++ helpers no longer
implicitly enable ASan just because the build type is Debug; select `USE_ASAN`
explicitly. TSan builds skip jemalloc and split DWARF. The handwritten parser
wrapper build and Go/cgo compilation receive their own TSan flags.

## Conan dependency variants

The standard build scripts rebuild Folly, milvus-common, libevent, oneTBB,
GEOS and gtest with TSan by default. Synchronization in prebuilt Folly is not
visible to TSan and can produce reports during service startup. The generated
host profile selects Clang 20 and C++20 for Core and these six packages,
retains the default compiler identity for other packages and build tools and makes sanitizer flags part of
package IDs. A scoped Conan hook preserves the flags that the Folly recipe
otherwise overwrites. The generated dependency manifest verifies native symbols
and records package revisions and library hashes; the worker validates it again
after cache restore.

OpenBLAS uses its pthread backend in the TSan profile. Its default GCC/Fortran
OpenMP backend would introduce `libgomp` alongside LLVM `libomp`; pthread BLAS
preserves parallelism without mixing OpenMP runtimes. Other Conan dependencies
remain prebuilt. For broader native dependency
instrumentation, opt into the provided full host profile (this broader profile
still requires validation of each recipe):

```bash
CONAN_HOST_PROFILE="$PWD/internal/core/conan/profiles/tsan" \
    make USE_TSAN=ON mode=RelWithDebInfo build-cpp-with-unittest
```

The full profile extends `default` and configures C/C++ compile flags and executable/
shared-library link flags. It includes those settings and the sanitizer marker
in Conan package IDs. `--build=missing` can therefore reuse TSan variants without
silently substituting ordinary packages. Build-context tools retain the normal
profile. A custom compiler profile must match the Core compiler and runtime.

The Conan-generated CMake toolchain records `MILVUS_CONAN_SANITIZER`. CMake
rejects TSan dependencies with `USE_TSAN=OFF`. This marker proves profile
selection, not that every recipe honors the flags: inspect actual dependency
compile commands, especially custom build systems and assembly. Validate
OpenMP synchronization and runtime selection separately; do not mask entire
libraries with suppressions to obtain a passing build.

Rust/Tantivy and the milvus-storage Rust bridge remain uninstrumented. Adding
Rust coverage requires a separately validated nightly toolchain and compatible
sanitizer runtime. CMake flags cannot instrument a prebuilt archive or a Cargo
build automatically.

## Runtime and packaging

CMake installs `lib/milvus-sanitizer` (or `lib64` on applicable layouts), recording
`none`, `address`, or `thread`. The shell environment and local launchers use it
to recognize TSan builds even when the original build environment is gone.
They remove jemalloc preloads, reject
ASan preloads and Go `-race` in `GOFLAGS`, and enforce
`TSAN_OPTIONS=...:halt_on_error=1:exitcode=66`.

The C++ runner defaults to one shard for TSan; callers can set `CPP_UT_SHARDS`
and `TEST_TIMEOUT` for the available resources. The installation script copies
the dynamically linked TSan runtime with the other runtime libraries.
`build/build_image.sh` disables jemalloc preloading when packaging a TSan build.
CMake also installs `libomp.so.5`, `libarcher.so`, the shared compiler-rt and a
`milvus-archer` marker. Launchers and the Dev CLI image activate OMPT through
`OMP_TOOL=enabled` and an absolute `OMP_TOOL_LIBRARIES` path. Native caches use
an LLVM/Archer-specific namespace and require these artifacts.

Archer's upstream configuration uses `ignore_noninstrumented_modules=1` to
exclude runtime internals. This reduces coverage of accesses originating in
uninstrumented dependencies; it is not a declaration that those dependencies
are race-free. No project-wide race suppression is installed. Both synchronized
and deliberately racy OpenMP probes run with the same setting.

External image builders must set `MILVUS_JEMALLOC_LIB` to an empty build
argument, `MILVUS_ARCHER_LIB=/milvus/lib/libarcher.so`, and `MILVUS_TSAN_OPTIONS`
to `halt_on_error=1:exitcode=66:ignore_noninstrumented_modules=1:external_symbolizer_path=/milvus/lib/llvm-symbolizer`.
CMake installs the matching LLVM symbolizer and its dependencies in
`lib/tsan-symbolizer`, with a `lib/llvm-symbolizer` launcher that isolates its
library search path from Core's Conan libraries. Preserve this directory and
launcher when copying native caches or packaging images. The report checker
requires source filenames and line numbers as well as function names. After packaging, run
`python3 internal/core/milvus_tsan.py verify-runtime lib` to reject missing
runtime files, unresolved dependencies, GNU libgomp and GNU libtsan.

`all_tests` has a C++ main but also loads the Go plan-parser shared library. Go
has special cgo/Tsan boundary annotations; these do not provide complete
cross-language race detection. Never combine native TSan with Go `-race` and
interpret successful startup or a passing SDK query as race coverage.

## Verification

The dependency-free smoke fixture lives in `internal/core/unittest/tsan`. It can
be configured independently on a Linux runner, or built as part of a TSan Core
UT build. `ctest --test-dir <build-dir> -L tsan --output-on-failure` runs:

- A mutex-protected C/C++ shared-library access case that must pass.
- An intentional C data race in a shared library that must report and exit 66.
- An intentional C++ data race in a shared library with the same requirement.
- OpenMP lock, barrier and task-depend cases that must be race-free.
- An intentionally racy OpenMP case that must report and exit 66.

The report checker rejects initialization crashes, silent success, wrong exit
codes, missing symbolized access functions, and a missing Archer startup banner. The intentionally racy probe is
not installed among ordinary UT executables. No suppressions are enabled by
default. `bash scripts/sanitizer_env_test.sh` checks allocator handling, parser
and image command propagation, and configuration conflicts without compiling
Milvus. `python scripts/test_tsan_conan_profile.py` (with Conan 2.25.1 installed
in the development virtual environment) checks distinct host package IDs and
unchanged build-tool IDs offline, without compiling dependencies or modifying
the user's Conan cache.

Before expanding the coverage claim, audit compile/link commands and loaded
libraries, run the positive and negative smoke cases, then exercise real
concurrent Core paths and language boundaries. A clean report only describes
executed, instrumented paths. Full process and image validation is separate
from verifying that CMake emits the required options.

### Historical GCC validation

The GCC 12.3.0 native + Go build and image completed with the six instrumented
Conan packages. The image reached Healthy, and SDK insertion and growing-query
cases passed. A sealed-query case failed during HNSW construction with a
neighbor-array read/write race in Knowhere `faff72c4931ca6e4d3642aca26fbe3085f2fb23b`
(`HNSW.cpp:328` and `:423`). Isolated native controls reproduced that access
pair after exposing OpenMP synchronization to TSan; the reader and writer hold
different per-node locks. This finding is not fixed or suppressed by the build
option. A successful sanitizer build does not imply that all existing workloads
run without sanitizer findings.

The same controls show that the worker's uninstrumented `libgomp` can also
report races for correctly locked OpenMP accesses. The LLVM/Archer profile replaces those diagnostic synchronization wrappers
with upstream OMPT annotations. The earlier HNSW finding remains a separate
algorithm issue; this integration does not suppress it.

## References

- [LLVM Archer](https://github.com/llvm/llvm-project/blob/llvmorg-20.1.8/openmp/tools/archer/README.md)
- [Clang ThreadSanitizer](https://clang.llvm.org/docs/ThreadSanitizer.html)
- [Conan configuration and package-ID confs](https://docs.conan.io/2/reference/config_files/global_conf.html)
- [Go cgo boundary implementation](https://go.dev/src/cmd/cgo/out.go)
- [Rust sanitizer support](https://doc.rust-lang.org/unstable-book/compiler-flags/sanitizer.html)

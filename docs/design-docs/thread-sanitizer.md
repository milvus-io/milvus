# Native ThreadSanitizer builds

## Scope

`USE_TSAN` is an opt-in CMake option for 64-bit Linux CPU builds with GCC or
Clang. It instruments Milvus C/C++ targets and CMake source dependencies with
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

These are commands for the Linux build environment. In the development workflow,
use the remote runner once it supports passing this option; setting a local
environment variable does not automatically forward it to a remote job.

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
host profile keeps build tools unchanged and makes sanitizer flags part of
package IDs. A scoped Conan hook preserves the flags that the Folly recipe
otherwise overwrites. The generated dependency manifest verifies native symbols
and records package revisions and library hashes; the worker validates it again
after cache restore.

Other Conan dependencies remain prebuilt. For broader native dependency
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
to recognize TSan builds even when the original build environment is gone or
Clang has linked the runtime statically. They remove jemalloc preloads, reject
ASan preloads and Go `-race` in `GOFLAGS`, and enforce
`TSAN_OPTIONS=...:halt_on_error=1:exitcode=66`.

The C++ runner defaults to one shard for TSan; callers can set `CPP_UT_SHARDS`
and `TEST_TIMEOUT` for the available resources. The installation script copies
the dynamically linked TSan runtime with the other runtime libraries.
`build/build_image.sh` disables jemalloc preloading when packaging a TSan build.
External image builders must likewise set `MILVUS_JEMALLOC_LIB` to an empty
build argument for the CPU Dockerfiles. Preserve symbols and supply a compatible
symbolizer in the test environment.

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

The report checker rejects initialization crashes, silent success, wrong exit
codes, and missing symbolized access functions. The intentionally racy probe is
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

## References

- [Clang ThreadSanitizer](https://clang.llvm.org/docs/ThreadSanitizer.html)
- [Conan configuration and package-ID confs](https://docs.conan.io/2/reference/config_files/global_conf.html)
- [Go cgo boundary implementation](https://go.dev/src/cmd/cgo/out.go)
- [Rust sanitizer support](https://doc.rust-lang.org/unstable-book/compiler-flags/sanitizer.html)

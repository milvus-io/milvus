# Shared AWS runtime

This is the 3.0 backport of #54118 for #54101. Milvus, milvus-storage,
Arrow and native tests must use one shared AWS SDK/CRT/C-library dependency
graph. Static AWS archives can embed private CRT JSON allocator state in both
an executable and a shared library. `Aws::InitAPI` can initialize one copy
while another copy is used by endpoint code, causing an allocator assertion.
Compiler inlining can affect whether an endpoint-provider test exposes it.

## Build and delivery contract

- Build AWS SDK C++, CRT C++, all `aws-c-*` libraries and `aws-checksums` with
  `shared=True`. The root Conan recipe rejects static overrides from profiles
  and command-line options.
- Resolve SDK/CRT CMakeDeps targets from that graph. The direct CRT requirement
  exposes link libraries needed by tests, tracks the SDK recipe, and omits
  `force=True` so incompatible SDK upgrades fail dependency resolution.
  `find_package(AWSSDK)` already loads the CRT target.
- With `MILVUS_AUDIT_AWS_RUNTIME=ON`, Linux builds audit linker maps and full,
  unstripped symbols for milvus-storage, milvus_core and all_tests. The dynamic
  symbol table alone cannot reveal hidden runtime copies. Successful storage
  audits are cached until the library or checker changes.
- Regenerate Conan inputs and rebuild Arrow and all native consumers when
  changing this policy. Conan imports and installation must deliver the complete
  AWS shared-library dependency closure, including SONAME symlinks.
- Verify the installed output without access to a Conan cache. This build-policy
  change retains the branch's milvus-common and milvus-storage source pins.

The audit defaults to the unit-test build setting. `scripts/core_build.sh`
passes it explicitly on every configure, including switching an existing build
tree from normal to CI unit/coverage builds. `make install` and `make gpu-install`
also default the audit to ON for packaged builds. Developers can override it
with the `MILVUS_AUDIT_AWS_RUNTIME=ON/OFF` environment or make variable; direct
CMake invocations use `-DMILVUS_AUDIT_AWS_RUNTIME=ON/OFF`. Conan's shared-library
validation remains mandatory regardless of the audit setting.

## Regression coverage

`AwsRuntimeDeathTest.CrtRuleEngineUsesSdkInitialization` directly constructs
the real S3 CRT RuleEngine after SDK initialization and destroys it before
SDK shutdown. It requires no network access or separate CRT initialization.
The test has its own source file because 3.0 does not contain master's
endpoint-authentication guard test suite.

The existing checksum test balances its SDK initialization after its client
configuration is destroyed. This lets CRT workers finish before SDK logger
teardown. Linux CTest `aws_runtime_link_check` covers a valid shared graph,
AWS archives, a real hidden-symbol runtime copy and a stripped binary.
Both the full C++ unit-test runner and the coverage runner execute this CTest on
Linux and fail if it is missing or unsuccessful. `MILVUS_CMAKE_BUILD_DIR` can
select a nondefault build tree.

```sh
all_tests --gtest_filter='AwsRuntimeDeathTest.*:MinioChecksumConfig.*'
(cd cmake_build/unittest && ctest -R '^aws_runtime_link_check$' --output-on-failure --no-tests=error)
```

Backport validation must use 3.0's native consumers and installed libraries.
The master PR's successful CI and runtime tests establish the original fix's
coverage; they do not replace building and testing this branch.

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
- Resolve SDK/CRT CMakeDeps targets from that graph. Linux builds audit linker
  maps and full, unstripped symbols for milvus-storage, milvus_core and all_tests.
  The dynamic symbol table alone cannot reveal hidden runtime copies.
- Regenerate Conan inputs and rebuild Arrow and all native consumers when
  changing this policy. Conan imports and installation must deliver the complete
  AWS shared-library dependency closure, including SONAME symlinks.
- Verify the installed output without access to a Conan cache. This build-policy
  change retains the branch's milvus-common and milvus-storage source pins.

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

```sh
all_tests --gtest_filter='AwsRuntimeDeathTest.*:MinioChecksumConfig.*'
ctest --test-dir cmake_build/unittest -R '^aws_runtime_link_check$' --output-on-failure
```

Backport validation must use 3.0's native consumers and installed libraries.
The master PR's successful CI and runtime tests establish the original fix's
coverage; they do not replace building and testing this branch.

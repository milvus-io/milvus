# Artifact publication failure regression

`V1FileSourceTest.RollbackFailurePreservesCodeAndPath` is registered in
`all_tests` by the existing recursive `src/**/*Test.cpp` discovery. The test
requires the syscall fault library in this directory and reports a skip when
that library is absent. The dedicated runner compiles the library, preloads it,
and invokes the registered test explicitly.

Run from the repository root after the final C++ build and after loading the
usual Conan runtime environment:

```sh
rtk proxy bash internal/core/unittest/storage_artifact/run_publication_fault_tests.sh \
  "$PWD/cmake_build/unittest/all_tests" /tmp/segidx-storage-publication-faults
```

If that build requires an explicitly preloaded sanitizer runtime, set
`MILVUS_TEST_PRELOAD` to that runtime's absolute path before running the command.
An existing `LD_PRELOAD` is also preserved as a prefix.

The fault library is disabled until the fixture arms one unique temporary
publication directory. It fails the second staged-file publication with EIO,
then fails either rollback unlink with EROFS or backup restoration with EACCES.
The regression covers paired and unmatched braces in paths, checks that both
failures were injected, and checks `FileWriteFailed`, diagnostic paths and
errno messages, and the production `FailureCStatus` projection to code 2015.
Downloads and calls outside that directory are unaffected.

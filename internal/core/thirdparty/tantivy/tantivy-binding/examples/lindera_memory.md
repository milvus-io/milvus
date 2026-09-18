# Lindera dictionary sharing regression

Run from `internal/core/thirdparty/tantivy/tantivy-binding`.

```sh
cargo test --locked --lib test_dictionary_cache
cargo test --locked --lib --features lindera-ipadic test_independent_lindera
```

The default-feature tests build a small local dictionary without downloading
assets. They cover concurrent first loads, directory aliases, identity isolation,
missing files, corrupt character definitions, and retry after repair. The lifecycle
regression also covers last-consumer release, failed reload, concurrent reload,
and the requested-path fast cache after reload. The embedded
IPADIC test constructs independent tokenizers and compares token text, offsets,
and positions against an uncached dictionary.

## Allocation experiment

```sh
cargo build --locked --release --example lindera_memory
export LINDERA_MEMORY_DICT_ROOT=/tmp/milvus-lindera-memory
for mode in direct nested; do
    for count in 1 100 1000; do
        target/release/examples/lindera_memory "$count" "$mode"
    done
done
```

Each invocation is a fresh process and retains every independently constructed
analyzer until measurement. `nested` uses `language_identifier` with an IPADIC
sub-analyzer. The first invocation downloads/builds IPADIC if necessary; prepare
the on-disk dictionary before recording comparable results.

`rust_live_bytes_delta` measures outstanding requested bytes through Rust's global
allocator, relative to immediately before analyzer construction. It excludes
allocator overhead, direct C allocations, and process RSS. Expect dictionary
buffers to be shared, not total allocation to be independent of analyzer count.
For a before/after comparison, run this same example on the parent revision with
the same feature flags and dictionary files. Set an external memory limit for
unfixed runs: 1000 runtime dictionaries can require tens of GiB.

Dictionary lifetime follows its consumers, not the process lifetime. Both cache
layers retain only weak references: releasing the last strong reference drops
the dictionary immediately, without waiting for process exit. Cache keys, weak
references, and per-key initialization locks remain cached to coordinate reloads;
they do not keep the dictionary's owned buffers alive. Releasing those allocations
does not guarantee an immediate decrease in process RSS.

While a dictionary is live, previously loaded paths reuse it without accessing
the dictionary directory, including after file replacement or symlink retargeting.
After it is released, the next request resolves the path and loads it again.
New path aliases are canonicalized to share existing dictionaries;
different canonical directories remain isolated. Relative paths are anchored to
the current working directory, which must still be available to resolve them.
Download URLs configure mirrors, not dictionary identity: changing them does not
refresh existing disk or memory dictionaries.

These checks do not replace the growing-segment experiment from issue #53227.
Record actual retained segment counts, sealing/release activity, and jemalloc
allocated bytes when verifying the full Milvus workload.

## Local validation, 2026-09-07

On macOS, debug profile, default features, with real IPADIC 2.7.0-20070801 files:

| Retained analyzers | Direct, Rust bytes | Nested, Rust bytes |
| --- | ---: | ---: |
| 1 | 47,060,550 | 47,060,953 |
| 100 | 47,081,142 | 47,121,442 |
| 1000 | 47,268,342 | 47,671,342 |

A temporary negative control replaced the shared initialization cell with a fresh
cell per call. Direct construction then retained 47,060,550 bytes for one analyzer
and 752,962,920 bytes for 16. The concurrent sharing regression failed at
`Arc::ptr_eq` with that control. This is a cache-disabled control, not a complete
parent-revision benchmark; large unfixed runs were not performed.

The production library and example compiled without unrelated source changes.
Two dictionary-cache tests and ten IPADIC tokenizer tests passed in a temporary
copy. The original checkout's unit-test build is blocked by existing issues:
conflicting normal/test `rand` dependencies (0.9/0.7), and a missing
`enable_background_merge` argument in `test_fuzzy_match_query`. Only in the
temporary copy, the test-only `rand` dependency was removed and the missing
argument supplied. These adjustments are not part of this fix.

Full Milvus growing-segment recovery, jemalloc profiling, and an unmodified
checkout unit-test pass remain unverified.

After adding the loaded-path fast cache, five dictionary-cache tests and ten
IPADIC tokenizer tests passed in the same temporary validation setup. New tests
cover directory removal without recreation, URL changes, relative paths, pinned
symlinks, and symlink-aware `..` resolution. Before the fast-cache change, the
directory-removal and symlink-retargeting regressions both failed. The production
library and allocation example also rebuilt in the original checkout. The
allocation figures above were not remeasured after this follow-up.

## Lifecycle validation, 2026-09-15

The production library built with `cargo build --locked --lib`. All six
`test_dictionary_cache` tests passed in a temporary copy with only the two
test-build workarounds described above: removing the conflicting test-only
`rand` dependency and supplying the missing `enable_background_merge` argument.
The new regression verifies concurrent sharing, last-consumer release, a missing
file on reload, successful concurrent reload after repair, and fast-cache reuse
of the new dictionary after directory removal. The unmodified checkout's unit
test build remains blocked by those pre-existing test issues. Full Milvus
segment-release behavior and process RSS reclamation were not measured.

Additional validation used real IPADIC files and a temporary variant of the
allocation example that drops all 100 analyzers after each of three rounds:

| Mode | First retained delta (bytes) | First released delta (bytes) | Released delta in rounds 2 and 3 |
| --- | ---: | ---: | ---: |
| Direct | 47,081,750 | 1,296 | 0 |
| Nested language identifier | 47,122,050 | 1,296 | 0 |

These are live Rust allocation deltas, not process RSS. A negative control using
the pre-change cache implementation and the new lifecycle test failed at
`old.upgrade().is_none()` after releasing the last consumer, as expected.

Full-suite validation used the same temporary copy, additionally making logger
initialization idempotent, redirecting hard-coded `/var/lib` and `/logs` paths
to `/tmp`, and supplying `CARGO_PKG_VERSION=0.1.0` for the asset-fetch test.
No tests were skipped. The initial full run exposed logger initialization and
system-directory permission failures; these adjustments are not in this PR.
The default-feature suite passed all 110 tests with real dictionaries. Only
the third-party `yada` dependency used `opt-level=3` to accelerate the initial
Neologd dictionary build; the binding and cache code remained unoptimized.
The `lindera-ipadic` feature suite then passed all 106 tests in the normal debug
profile, reusing the prepared disk dictionaries. Both runs also completed the
doc-test target (zero doc tests). Commands from the adjusted copy, with
`MY_LOG_LEVEL=error` and `CARGO_PKG_VERSION=0.1.0`:

```sh
cargo test --offline --config 'profile.dev.package.yada.opt-level=3' --no-fail-fast
cargo test --offline --features lindera-ipadic --no-fail-fast
```

This validates the Rust binding suite, not the full Milvus Go/C++ or distributed
end-to-end suites.

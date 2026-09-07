# Lindera dictionary sharing regression

Run from `internal/core/thirdparty/tantivy/tantivy-binding`.

```sh
cargo test --locked --lib test_dictionary_cache
cargo test --locked --lib --features lindera-ipadic test_independent_lindera
```

The default-feature tests build a small local dictionary without downloading
assets. They cover concurrent first loads, directory aliases, identity isolation,
missing files, corrupt character definitions, and retry after repair. The embedded
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

Successful cached dictionaries remain resident until process exit. Previously
loaded paths reuse their dictionary without accessing the dictionary directory.
Replacing files or retargeting a previously loaded symlink requires restarting
the process. New path aliases are canonicalized to share existing dictionaries;
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

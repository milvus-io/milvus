# ScalarPredicateReaderTest 独立静态审查

Baseline: pinned master `a876f471053edb2f68a06a9894afee9810ea7906`.
Review mode: source only. No configuration, compilation, test listing, test
binary, or Milvus binary was run.

## 已检查范围

- Master scalar predicate coverage in `ScalarIndexTest.cpp`,
  `BoolIndexTest.cpp`, `BitmapIndexTest.cpp`, `ScalarIndexSortTest.cpp`,
  `StringIndexTest.cpp`, `StringIndexSortTest.cpp`, `InvertedIndexTest.cpp`,
  `HybridScalarIndexTest.cpp`, and `FMIndexTest.cpp`.
- Current `ScalarPredicateReader<T>` operations: `In`, `NotIn`, six unary
  comparisons, and four-bound interval comparisons.
- bool, int8, int16, int32, int64, float, double, and string-view data and query
  argument ownership.
- nullable versus non-nullable memory/mmap backend registration and dataset
  eligibility.

## 发现与关闭情况

- The initial suite omitted `NotIn(true)` over the multi-row all-true bool
  distribution while its report claimed both keys for both membership
  operations. The author added `NotInTrueAllTrue` on `PredicateAllEqual`; the
  master all-true/all-false/mixed bool matrix is now represented.
- `In` builds the row-wise membership result directly. `NotIn` flips that
  result and then intersects validity, preserving null rejection for the
  negative predicate. Unary and interval oracles compare each valid row with
  the contract's native operators.
- bool query arrays use owned `bool[]` storage rather than `vector<bool>` proxy
  storage. String query values own their bytes for the duration of each reader
  call; embedded-NUL `string_view` length is preserved by the oracle and query
  adapter.
- High-cardinality fixtures stay within each type's representable domain:
  int8 uses 200 distinct values, while wider integer, float, double, and string
  fixtures use 2,000 distinct values. Float and double infinity cases are
  separate from the finite edge corpus; NaN remains explicitly deferred.
- Dataset type, nullability, and arguments align with the shared catalog. FM is
  correctly absent because it exposes no scalar predicate contract. Nullable
  descriptors select nullable profiles only; all-valid descriptors select both
  metadata profiles.

No remaining semantic or baseline-coverage finding was identified. The final
source-derived inventory is 506 descriptors and 5,570 expanded parameters.
The Marisa embedded-NUL contract defect remains intentionally exposed and is
recorded in `21-scalar-predicate-issues.md`.

Runtime behavior remains unverified because builds and test execution were
prohibited for this task.

Final source checks: `git diff --check` and
`clang-format --dry-run --Werror` completed with no output for both scalar and
pattern contract test files.

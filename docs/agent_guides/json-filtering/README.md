# JSON filtering

Read the following contract before changing JSON parsing, filtering, stats, or
index execution:

- [Cross-path query semantics](cross-path-semantics.md): the common result
  contract, safe fallback rules, known expected divergences, and the regression
  test matrix for raw scans, JSON stats, path indexes, and flat indexes.

Update the contract and its linked tests together whenever an execution path
changes observable result or validity semantics.

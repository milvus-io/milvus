# Search / MultiSearch competition under CPU contention

This addendum tests an already saturated resident workload of 128 independent
Search callers while another client is added, then removed. The server binary
is unchanged from the original native MultiSearch experiment.

Read `mixed-report.pdf` for GitHub preview, or download `mixed-report.html`.
`summary.json` contains per-run and aggregate measurements, `comparisons.json`
compares the added-Search and added-MultiSearch cases, and `manifest.json` records
the workload, resource limits, timestamps/windows and binary identity.

The first 160/s pair was excluded and rerun because an operator binary-hash check
ran inside the tested container during its MultiSearch phase, sharing its CPU
quota. Both original pair members are retained in the evidence bundle under
`excluded-hash-interference/`. `excluded-runs.json` records the exact reason and
replacement names. They are never silently pooled into the formal results.

The evidence bundle contains valid and excluded raw samples, resource telemetry,
logs and the source changes. Raw files are deliberately outside git. Reproduce
with the scripts documented in `examples/multisearch/README.md` on a clean raw
results directory. The reported 160/s pair uses the later r2 rerun and the r1
reverse-order repetition; the other pairs use r0/r1.

# Evidence for the native v3.0.5 MultiSearch prototype

The English HTML and PDF reports include the design, core implementation, build provenance,
functional results, and performance measurements. `performance-summary.json`
contains one row per measured run; `comparison.json` aggregates the two-CPU runs.
Group latency includes all eight logical searches for both ordinary Search and
MultiSearch. Group p99 in the HTML pools raw samples across repeated runs; it is
not an average of the per-run p99 values in the summary.

The review bundle contains raw group/probe latency samples, per-second container
resource samples, functional outputs, logs, the rendered PDF/HTML and code patches.
These larger raw/generated files remain outside git. Scripts under
`examples/multisearch` regenerate the evidence. The discarded paused-container
experiment is excluded from reported results and the review bundle.

Read `failure-audit.md` and `verification-summary.json` for the limits of validation.
This is a bounded local feasibility/performance experiment, not a production
readiness or PSC reliability claim.

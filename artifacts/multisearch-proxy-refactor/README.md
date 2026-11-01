# Proxy-owned MultiSearch

This is the current implementation report. The older reports under
`multisearch-v2` and `multisearch-mixed` describe the previous Server-owned
coordinator and keep their original performance evidence and binary identities.

The transport delegates `Server.MultiSearch -> Proxy.MultiSearch`. The Proxy
owns admission and batch coordination; its children run the shared Search entry
chain, directly call `Proxy.Search`, and enter the existing DQL scheduler.
There is no MultiSearchExecutor or additional Server field.

Read `report.pdf` for GitHub preview, or download `report.html`. `summary.json`
contains the four new mixed-traffic runs. Functional results, hashes, verification
scope and failure-path audit are included. Raw distributions, telemetry and logs
are in the evidence bundle delivered through the Feishu bridge.

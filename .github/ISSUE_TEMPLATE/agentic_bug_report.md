---
name: "\U0001F916 Agentic Bug Report"
about: Structured bug report optimized for AI Agent to analyze, reproduce, and fix autonomously
title: "[Bug]: "
labels: kind/bug, needs-triage
assignees: yanliang567
---

## Environment

- **Milvus Version**: <!-- image tag (e.g. master-20260204-e12a5b0b-amd64) or commit hash -->
- **Deployment Mode**: <!-- standalone | cluster -->
- **MQ**: <!-- rocksmq | pulsar | kafka -->
- **SDK**: <!-- e.g. pymilvus v2.6.0 / go-sdk v2.6.0 -->
- **OS**: <!-- e.g. Ubuntu 22.04 -->

## Reproduction

<!-- Provide ONE of the following: -->

### Option A: Script (preferred for functional bugs)

<!--
Complete, runnable script that reproduces the bug.
Requirements:
- Must be self-contained (include collection creation, data insertion, index building, loading, etc.)
- Must use a unique collection name to avoid conflicts
- Connection defaults: host="localhost", port="19530"
- End with an assertion or clear print that shows the bug
-->

```python

```

### Option B: Steps (for systemic / stability / timing issues)

<!--
Structured steps with specific parameters.
Be precise: not "insert some data", but "10k rows/sec for 30 minutes".
-->

1. Deploy: <!-- deployment topology, e.g. 3 QueryNodes, 2 DataNodes -->
2. Setup: <!-- collection schema, index params, load params -->
3. Workload: <!-- exact operations, rate, duration -->
4. Trigger: <!-- what operation or condition triggers the bug -->
5. Observed via: <!-- metrics / logs / query result / crash -->

### Trigger Conditions

- **Frequency**: <!-- always | intermittent (~30%) | rare -->
- **First observed after**: <!-- e.g. ~20 minutes of continuous load -->
- **Does NOT happen when**: <!-- e.g. only 1 QueryNode / no compaction / default config -->

## Expected Behavior

<!-- What should happen. Be specific with numbers when possible. -->

## Actual Behavior

<!-- What actually happens. Be specific with numbers when possible. -->

## Error Logs

<!--
Key error logs or stack traces. Only the relevant part, not full logs.
If no error is raised (silent wrong result), state:
"No error in logs, results are incorrect."
-->

```

```

## Non-default Configuration

<!--
Milvus server config that differs from default. Skip if all defaults.
Only include relevant sections.
-->

```yaml

```

## Analysis Hints (Optional)

<!--
Any clues that help narrow down the root cause:
- Suspect code location: e.g. internal/proxy/task_search.go
- Related PR/commit: e.g. might be introduced by #12345
- Trigger boundary: e.g. only happens with sealed segments, growing is fine
- Already ruled out: e.g. not a SDK issue, confirmed by raw gRPC call
- Related issues: e.g. #46820, #46972
-->

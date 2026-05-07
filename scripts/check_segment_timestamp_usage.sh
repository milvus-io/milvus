#!/bin/bash
# check_segment_timestamp_usage.sh
#
# Static guard: detect direct segment timestamp access in non-test Go files.
# All temporal decisions on segments must use segmentEffectiveTs/segmentEffectiveDmlTs
# to correctly handle import/CDC segments with commit_timestamp.
#
# Allowlisted paths are verified to only operate on Growing/L0 segments or are
# the helper definitions themselves. Each allowlist entry must include a comment
# explaining why the raw access is safe.
#
# Usage: scripts/check_segment_timestamp_usage.sh
# Exit code: 0 = pass, 1 = violations found

set -euo pipefail

PATTERN='(GetStartPosition\(\)\.GetTimestamp\(\)|GetDmlPosition\(\)\.GetTimestamp\(\))'

# Allowlist: paths verified to be safe for raw timestamp access.
# Format: grep -E pattern (file basename or file:context).
ALLOWLIST_PATTERNS=(
    # Helper function definitions — segmentEffectiveTs/segmentEffectiveDmlTs
    # fallback to raw timestamp when commitTs=0 (by design)
    "segmentEffective"
    # segment_info.go: helper function bodies (return seg.GetXxxPosition().GetTimestamp())
    "segment_info.go"
    # Growing segment sealing — import segments are never Growing
    "segment_allocation_policy.go"
    # Empty Growing segment cleanup — import segments have rows
    "segment_manager.go"
    # Growing segment release — import segments are never Growing
    "segment_checker.go"
    # DML pipeline for Growing segments — import segments don't receive DML messages
    "flow_graph_dd_node.go"
    # Meta update validation guard — compares within same segment, not temporal decision
    "meta.go:.*GetDmlPosition"
    # GetEarliestStartPositionOfGrowingSegments — filters Growing only
    "meta.go:.*GetStartPosition"
    # Import task sets positions from actual binlog data (upstream of commit_ts)
    "import_task_import.go"
    # Growing segment DML position for excluded segments — unflushed only
    "services.go"
    # L0 deleteCheckPoint — only operates on L0 segments, not import segments
    "handler.go:.*deleteCheckPoint"
    # Handlers for growing segment info reporting (not temporal decision)
    "handlers.go"
    # Log statements (not temporal decisions)
    "zap\\.Time\\|zap\\.Uint64.*Ts"
    # delegator_data.go: segmentEffectiveTs helper fallback + log statements
    "delegator_data.go"
)

# Build grep -v pattern from allowlist
ALLOWLIST_REGEX=$(IFS='|'; echo "${ALLOWLIST_PATTERNS[*]}")

VIOLATIONS=$(grep -rn --include='*.go' --exclude='*_test.go' -E "$PATTERN" internal/ \
    | grep -v -E "$ALLOWLIST_REGEX" \
    || true)

if [ -n "$VIOLATIONS" ]; then
    echo "============================================================"
    echo "ERROR: Direct segment timestamp access detected!"
    echo ""
    echo "For import/CDC segments, raw GetStartPosition().GetTimestamp()"
    echo "and GetDmlPosition().GetTimestamp() return stale values."
    echo ""
    echo "Use segmentEffectiveTs() or segmentEffectiveDmlTs() instead."
    echo ""
    echo "If this is a false positive (e.g., only operates on Growing"
    echo "segments), add to the allowlist in this script with a comment"
    echo "explaining why raw access is safe."
    echo ""
    echo "See: docs/plans/2026-04-02-commit-timestamp-test-design.md"
    echo "============================================================"
    echo ""
    echo "$VIOLATIONS"
    exit 1
fi

echo "segment timestamp usage check: PASS"

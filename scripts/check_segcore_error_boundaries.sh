#!/bin/bash
# check_segcore_error_boundaries.sh
#
# Static guard for the segcore error-classification contract. Two rules, each
# enforcing one half of the same invariant: an error that reaches the cgo
# boundary must carry a milvus ErrorCode, because FailureCStatus() reports any
# exception that is not a SegcoreError as UnexpectedError(2001) — the bucket
# that means "unclassified internal bug", indistinguishable from a real one.
#
#   RULE 1 (typed throw)      Every throw in internal/core/src must throw a
#                             milvus exception (or a folly control-flow type,
#                             or be a bare rethrow).
#
#   RULE 2 (library confinement)
#                             Vendored libraries under internal/core/thirdparty
#                             are free to throw plain std:: exceptions — they
#                             have no dependency on milvus-common and are synced
#                             from a pinned upstream revision (see their NOTICE).
#                             The obligation to classify therefore sits at the
#                             consumption point, so each such library's symbols
#                             are confined to declared boundary files that own
#                             the try/catch.
#
# Rule 2 exists because rule-1-style counting cannot catch the real defect: a
# consumption point can hold a classifying catch on one code path and none on
# another (e.g. FM index had one on build, none on load, so a truncated blob
# surfaced as 2001 instead of DataFormatBroken). Confining the library to a
# small set of files turns a per-branch obligation into a per-file one, which
# is reviewable.
#
# Usage: scripts/check_segcore_error_boundaries.sh
# Exit code: 0 = pass, 1 = violations found

set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
SRC="${ROOT}/internal/core/src"

# Test sources are exempt: they throw std:: exceptions on purpose to drive the
# handlers under test.
is_test_file() {
    [[ "$1" =~ Test\.cpp$ || "$1" =~ _test\.(cpp|h)$ || "$1" =~ /test_[^/]*\.(cpp|h)$ || "$1" =~ test_case ]]
}

fail=0

############################################################################
# RULE 1 — every throw in internal/core/src carries a milvus ErrorCode.
############################################################################
# Allowed throw operands:
#   SegcoreError / milvus::SegcoreError   the typed base
#   ExecOperatorException / ExecDriverException
#                                         typed, derive from SegcoreError
#   folly::Future*                        folly's own control-flow signals,
#                                         consumed by the futures layer and
#                                         never projected to a CStatus
#   ;                                     bare rethrow (throw;) preserving the
#                                         original dynamic type. `throw e;` is
#                                         deliberately NOT allowed: it rethrows
#                                         by the CAUGHT type, slicing a derived
#                                         SegcoreError back to its base
ALLOWED_THROW='^(milvus::)?(SegcoreError|ExecOperatorException|ExecDriverException)\(|^folly::Future[A-Za-z]*\(|^;'

# `throw` is matched anywhere on the line, not just at its start: `if (bad)
# throw ...;` and `} else throw ...;` are throws too. Line comments are stripped
# and block-comment bodies skipped first, so prose about throwing does not
# register. The preceding-character class keeps `rethrow` from matching.
rule1_violations=""
while IFS= read -r file; do
    rel="${file#${ROOT}/}"
    is_test_file "$rel" && continue
    while IFS= read -r hit; do
        lineno="${hit%%:*}"
        operand="${hit#*:}"
        if ! grep -qE "$ALLOWED_THROW" <<<"$operand"; then
            rule1_violations+="  ${rel}:${lineno}: throw ${operand}"$'\n'
        fi
    done < <(awk '
        /^[[:space:]]*[*]/                { next }
        { line = " " $0; sub(/\/\/.*/, "", line) }
        line ~ /[^A-Za-z_]throw[[:space:]]*[^A-Za-z_]/ {
            sub(/^.*[^A-Za-z_]throw[[:space:]]*/, "", line)
            sub(/[[:space:]]+$/, "", line)
            printf "%d:%s\n", FNR, line
        }
    ' "$file")
done < <(find "$SRC" \( -name '*.cpp' -o -name '*.h' -o -name '*.hpp' \) -print)

if [[ -n "$rule1_violations" ]]; then
    fail=1
    echo "ERROR: untyped throw in internal/core/src."
    echo "  An exception without a milvus ErrorCode reaches the cgo boundary as"
    echo "  UnexpectedError(2001). Throw a SegcoreError, or use ThrowInfo(code, ...)."
    echo "$rule1_violations"
fi

############################################################################
# RULE 2 — vendored libraries stay behind their declared boundary files.
############################################################################
# One entry per confined library: "<symbol pattern>|<boundary file regex>".
# The boundary file is the one that owns the classifying try/catch; adding a
# file here means taking on the obligation to classify every exception the
# library can throw on every path that file reaches.
#
# Libraries already spread across the tree (knowhere, arrow, milvus_storage,
# tantivy) cannot be confined to one file today — they are ratcheted instead
# (RULE 3 below): the current file set is frozen as a baseline and only
# shrinking it is free.
CONFINED=(
    # fm-index-lite throws std::runtime_error / std::length_error / std::bad_alloc
    # on a truncated or oversized corpus; FMIndex.cpp classifies them in
    # BuildFMIndexLibrary (build) and LoadFMIndexLibrary (load).
    'fmindex::|^internal/core/src/index/FMIndex\.(cpp|h)$'
)

for entry in "${CONFINED[@]}"; do
    symbol="${entry%%|*}"
    boundary="${entry#*|}"
    offenders=""
    while IFS= read -r f; do
        rel="${f#${ROOT}/}"
        is_test_file "$rel" && continue
        grep -qE "$boundary" <<<"$rel" && continue
        offenders+="  ${rel}"$'\n'
    done < <(grep -rlE "$symbol" "$SRC" --include='*.cpp' --include='*.h' 2>/dev/null || true)

    if [[ -n "$offenders" ]]; then
        fail=1
        echo "ERROR: '${symbol}' used outside its declared boundary (${boundary})."
        echo "  This library throws untyped std:: exceptions. Route the call through"
        echo "  the boundary file, or extend CONFINED here and add the classifying"
        echo "  try/catch in the new file."
        echo "$offenders"
    fi
done

############################################################################
# RULE 3 — spread-out libraries are ratcheted: no NEW files may touch them.
############################################################################
# knowhere/arrow/milvus_storage/tantivy throw or return untyped errors and are
# referenced from far too many files to confine behind one boundary today. The
# ratchet freezes the current file set per library in a baseline; a file not in
# the baseline referencing the library fails the check. Shrinking is free and
# encouraged: regenerate the baseline after removing usages.
#
#   regenerate:  UPDATE_SEGCORE_ERROR_BASELINE=1 scripts/check_segcore_error_boundaries.sh
#
# Growing the baseline is a reviewed decision: regenerating with a NEW file in
# it means that file now owes a classifying catch (or a mapper such as
# KnowhereStatusToErrorCode / ArrowStatusToErrorCode / ToSegcoreErrorCode) for
# every error the library can surface on every path the file reaches.
BASELINE="${ROOT}/scripts/segcore_error_boundary_baseline.txt"

RATCHETED=(
    'knowhere|knowhere::'
    'arrow|arrow::'
    'milvus_storage|milvus_storage::'
    'tantivy|tantivy_|RustResult|TantivyIndexWrapper'
)

list_users() { # $1 = symbol pattern -> sorted repo-relative file list
    local symbol="$1" f rel
    while IFS= read -r f; do
        rel="${f#${ROOT}/}"
        is_test_file "$rel" && continue
        echo "$rel"
    done < <(grep -rlE "$symbol" "$SRC" --include='*.cpp' --include='*.h' 2>/dev/null || true) \
        | sort
}

if [[ "${UPDATE_SEGCORE_ERROR_BASELINE:-0}" == "1" ]]; then
    : > "$BASELINE"
    for entry in "${RATCHETED[@]}"; do
        lib="${entry%%|*}"
        symbol="${entry#*|}"
        while IFS= read -r rel; do
            [[ -n "$rel" ]] && printf '%s\t%s\n' "$lib" "$rel" >> "$BASELINE"
        done < <(list_users "$symbol")
    done
    echo "baseline regenerated: ${BASELINE#${ROOT}/}"
    exit 0
fi

if [[ ! -f "$BASELINE" ]]; then
    echo "ERROR: missing ${BASELINE#${ROOT}/}; run UPDATE_SEGCORE_ERROR_BASELINE=1 $0"
    exit 1
fi

for entry in "${RATCHETED[@]}"; do
    lib="${entry%%|*}"
    symbol="${entry#*|}"
    new_files="$(comm -13 \
        <(awk -F'\t' -v l="$lib" '$1==l {print $2}' "$BASELINE" | sort) \
        <(list_users "$symbol"))"
    if [[ -n "$new_files" ]]; then
        fail=1
        echo "ERROR: new file(s) reference '${lib}' (ratcheted library)."
        echo "  This library surfaces untyped errors; a new consumer owes a"
        echo "  classifying catch or status mapper on every path. Add it, then"
        echo "  regenerate: UPDATE_SEGCORE_ERROR_BASELINE=1 $0"
        echo "$new_files" | sed 's/^/  /'
    fi
done

if [[ $fail -ne 0 ]]; then
    exit 1
fi

echo "segcore error boundaries OK"

# Milvus design documents

Design documents are durable records of significant Milvus feature and
architecture decisions. They explain the problem, alternatives, selected
design, operational impact, and verification plan before or alongside the code
that implements the decision.

## What counts as a design document

A formal design document is any Markdown file under
`docs/design-docs/design_docs/`, including files in legacy or topic
subdirectories. The directory is the durable classification boundary: an
existing document does not stop being a Design Doc because its historical
filename or subdirectory differs from the current recommendation.

For new documents, use `YYYYMMDD-short-descriptive-name.md`, where the date is
the initial review date and the remaining words are lowercase and separated by
hyphens. New topic subdirectories should use lowercase letters, numbers,
hyphens, or underscores. These naming conventions improve navigation but are
not merge-blocking classification rules.

The following are deliberately outside the formal design-document interface:

- this README and [`TEMPLATE.md`](TEMPLATE.md);
- public or operator-facing user guides;
- current subsystem reference documentation;
- benchmarks and experimental source code;
- superseded architecture guides under `docs/archive/`.

Keep supporting files outside `design_docs/`: put images under `assets/`,
and keep executable experiments with the implementation, tests, or benchmark
suite they validate. Put other document classes in their owning directories.
This organizational rule does not invalidate or exempt a legacy Markdown
Design Doc from review policy.

## Recommended review metadata

For easier ownership and review tracking, place these four exact, unbolded
fields within the first 50 lines of a new or substantively revised Design Doc,
after the level-one title and before the first section heading:

```markdown
# MEP: <Title>

- Feature DRI: @github-login
- Primary Approver: @github-login
- Independent Approver: @github-login
- Design Review: YYYY-MM-DD
```

Replace every placeholder when using this block. `Feature DRI`, `Primary
Approver`, and `Independent Approver` should identify three distinct GitHub
users. The exact format lets automation recognize the fields and clear its
advisory reminder. Missing or invalid review metadata does not fail the policy
check and does not block merging. The template also contains recommended
lifecycle and discovery fields such as `Created`, `Status`, `Component`,
related issues, and release information.

When an older design document is substantively revised, add the four review
fields when practical. Pure path, link, or formatting maintenance may preserve
historical metadata. The policy check makes a bounded, best-effort inspection of
changed formal Design Docs and synchronizes a reviewer-visible advisory reminder
when fields are absent or invalid. Blob retrieval, decoding, header parsing,
inspection-budget exhaustion, and reminder-comment create, update, or cleanup
failures are reported as warnings without changing the hard policy result. If
inspection is incomplete, existing reminders are left unchanged.

Use [`TEMPLATE.md`](TEMPLATE.md) as the starting point for a new document.

## Pull-request policy

- Every Milvus feature must have a related design document in this repository
  and link it from the feature pull request.
- A pull request that adds, modifies, renames, or removes a formal design
  document requires two distinct non-author Approver approvals.
- Pull-request authors cannot satisfy either required approval themselves.
- Approvers can use the existing Prow flow by commenting `/approve`; a GitHub
  Review approval is not required. Automation adds `approved/design-doc` while
  two valid non-author approvals are active and removes it when they are not.
- The trusted Design Doc Policy workflow makes a best-effort attempt to post a
  non-blocking advisory comment for missing or invalid review metadata. Blob,
  metadata-parser, or reminder-comment failures produce warnings but do not
  change the approval or Design Doc existence result. A nonexistent feature
  Design Doc reference fails the pull request check. Files removed by a pull
  request still trigger the two-Approver policy, but naturally have no metadata
  to inspect.
- A release-branch pull request may reference a Design Doc that exists only on
  the base repository's default branch; it does not need a duplicate copy on
  the target release branch.

## Content expectations

At minimum, cover:

- **Summary:** the problem and intended outcome;
- **Motivation:** why the change is needed now;
- **Design:** interfaces, invariants, data flow, failure handling, and rollout;
- **Alternatives:** meaningful options considered and why they were rejected;
- **Compatibility:** upgrade, downgrade, wire, storage, and operational impact;
- **Verification:** unit, integration, end-to-end, fault-injection, and
  observability evidence appropriate to the change.

Place shared images under `docs/design-docs/assets/`. Put executable experiments
with the code, tests, or benchmark suite they exercise rather than under the
documentation tree.

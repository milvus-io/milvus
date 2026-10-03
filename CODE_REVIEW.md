# Milvus Code Review Guide

Code review checks whether a change solves its stated problem, preserves compatibility, and can be maintained safely. This guide applies to human and automated reviewers.

## Requirements and recommendations

Use [CONTRIBUTING.md](CONTRIBUTING.md) for contribution requirements, including commit history, DCO, PR format, issue links, and design documents. Do not introduce additional submission rules in review.

Check the target branch's configuration when evaluating automated requirements: [Mergify](.github/mergify.yml) defines labeling and blocking rules, [CodeCov](codecov.yml) defines coverage targets, and applicable formatter and lint configurations define mechanical style. Required GitHub checks and branch protection also apply. Recommendations and personal preferences are not additional merge gates.

If a written rule conflicts with configuration or implementation, identify the conflicting sources and ask maintainers to resolve the discrepancy before treating it as a blocking policy violation. A missing automated check does not by itself repeal a documented requirement.

## Review and approval

Reviewers focus on correctness, error handling, test coverage, readability, and compatibility. Approvers also assess the overall design, maintenance cost, and readiness to merge. Use applicable `OWNERS` files and [OWNERS_ALIASES](OWNERS_ALIASES) to identify maintainers.

The review commands `/lgtm` and `/approve` correspond to the `lgtm` and `approved` labels. Merge readiness also requires DCO, applicable CI checks, and resolution of blocking labels and outstanding review requests. Approval labels alone do not guarantee that a PR can merge.

CI requirements vary by target branch and changed files. In the current Mergify rules, `[skip e2e]` is matched in the **PR title**, not the commit message. It changes the applicable CI gate; it does not by itself guarantee that an E2E job will not run. Reviewers should check the reason for using it and whether the remaining validation covers the change.

## Before reviewing

- Read the PR description, related issue, and applicable design document. Establish the intended behavior and scope before evaluating the implementation.
- Read relevant repository and subsystem guidance, then cross-check it against the affected code and callers.
- For bug fixes, understand the failure and regression coverage. For performance changes, inspect comparable benchmarks and their conditions.
- Consider simpler alternatives and maintenance cost without requiring a rewrite solely to match your preferred approach.

## What to check

- **Correctness:** Does the implementation satisfy the stated behavior, including boundary conditions, invalid inputs, and failure paths?
- **Compatibility:** Are public APIs, configuration, persisted data, upgrades, and rollback affected? Are changes documented and tested where relevant?
- **Error handling and concurrency:** Are errors preserved and handled correctly across layers? Check cancellation, timeouts, retries, resource cleanup, and races where the change affects them.
- **Validation:** Do tests exercise the behavior being changed and catch the regression? For routing, error classification, retry, or fallback changes, trace representative failures from their origin to the consumer or use fault injection. Passing happy-path tests alone does not establish failure-path behavior.
- **Design and readability:** Are responsibilities, interfaces, names, and comments clear? Explain non-obvious constraints and workarounds without duplicating the code in comments.
- **Operational impact:** Could the change cause excessive logging, unbounded resource use, or performance regressions? Are relevant diagnostic signals available?
- **Scope:** Are unrelated changes avoided, and are generated files regenerated using the repository's tools?

Scale the review to the change. Do not require runtime tests for prose-only edits or unrelated architectural changes for a focused fix. Record validation gaps instead of claiming untested behavior is verified.

## Writing review comments

- Be respectful and discuss the code and its effects, not the author's abilities.
- For a blocking finding, identify the location, triggering condition, impact, and supporting code or rule. Distinguish a demonstrated defect from a question or an unverified concern.
- Distinguish defects introduced or exposed by the change from unrelated pre-existing issues. Report unrelated issues separately rather than making them prerequisites for this PR.
- Label optional suggestions and style preferences as non-blocking. Do not enforce personal capitalization, punctuation, or commit-count preferences as project requirements.
- Ask focused questions when intent or evidence is missing. Avoid speculative findings and repeated comments about the same underlying issue.
- Recognize sound solutions even when they differ from your preferred implementation.

## Final approval checklist

- The implementation and validation support the claims in the PR description; remaining limitations are explicit.
- The applicable [submission requirements](CONTRIBUTING.md#commits-and-prs), issue links, and design-document requirements are satisfied.
- The PR has appropriate kind labels, required checks have passed, and blocking findings are resolved.
- Multiple logically organized commits are acceptable. Do not require contributors to squash a PR into one commit; verify DCO compliance for every commit.

Inspired by the [TiDB Code Review Guide](https://github.com/pingcap/tidb/blob/master/code_review_guide.md).

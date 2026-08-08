# Repository governance enforcement

Milvus repository rules are enforced through required checks. Labels remain
useful feedback, but a label alone is not a merge boundary because it can be
removed and reapplied asynchronously.

## Required checks

Repository administrators must enable Mergify Merge Protections and require the
`Mergify Merge Protections` check in GitHub branch protection or rulesets for
`master` and every active release branch. Configure the check for the Mergify
GitHub App, enforce it for administrators, and do not grant the merge robot a
bypass that can ignore a failing protection.

The same protected branches must require pull requests, dismiss stale approvals
when new reviewable commits are pushed, and require approval of the most recent
reviewable push by someone other than its pusher. Mergify Merge Protections
honors that GitHub setting, so an approval from an earlier commit cannot satisfy
the current-head gate.

The Mergify configuration enforces:

- one non-author Approver approval for every pull request;
- two distinct non-author Approver approvals when a formal Design Doc is
  added, modified, renamed, or removed;
- two distinct non-author Approver approvals for changes to the Mergify,
  Approver-alias, or trusted Design Doc Policy enforcement files;
- a successful `Design Doc Policy` check for feature pull requests and formal
  Design Doc or governance-enforcement changes. Feature pull requests must
  also add, modify, or link a formal Design Doc in Mergify's live PR state;
  this native condition does not depend on a previously successful check run.

Approvers are the members of the `maintainers` alias in `OWNERS_ALIASES`.
Because Mergify cannot count the intersection of a reviewer team and its
approval list, `.github/mergify.yml` enumerates the allowed single reviewers
and reviewer pairs. The trusted workflow reads the proposed
`OWNERS_ALIASES` and Mergify configuration from the pull-request head as data
and fails when that matrix drifts. The same validation remains covered by local
unit tests.

## Design Doc Policy check

The trusted `pull_request_target` workflow never executes pull-request code.
It reads changed blobs through the GitHub API, treats every Markdown file under
the formal Design Doc directory as a Design Doc regardless of legacy filename
or subdirectory, verifies feature Design Doc references, reports header
findings in one maintained comment, validates proposed governance files, and
explicitly publishes the `Design Doc Policy` check on the pull request head
SHA. Its first step marks every matching check on that head SHA as in progress
(or creates one), before checkout or validator tests; failures after that step
fail the check closed. A missing or nonexistent feature Design Doc and
governance drift fail the check; header findings remain
reviewer-visible reminders. Publishing an explicit head-SHA check is required
because the workflow job's own
`pull_request_target` status belongs to the base branch SHA.

The current workflow publishes through the repository's `github-actions` App.
Mergify requires a successful check and rejects any same-name check from that
App that is pending, stale, skipped, neutral, cancelled, timed out, or failed;
therefore a second successful GitHub Actions job cannot mask the trusted
check's non-success state. This is still not a cryptographically dedicated
publisher identity: fork workflows also publish their automatic job checks as
the `github-actions` App, even though they do not receive a write-capable
repository token. For the strongest trust boundary, provision a dedicated
GitHub App for this check and change the Mergify app qualifier to that App.

GitHub check conclusions are keyed to a commit SHA; pull-request titles,
descriptions, and labels are mutable without changing that SHA. The workflow's
first-step invalidation and Mergify's live Design Doc reference condition close
the normal stale-result path, but they cannot make a body-only reference update
atomic with the merge decision. A strictly commit-bound policy must instead
require the feature pull request to add or modify its Design Doc. Supporting
body-only links with a zero-race guarantee requires an enforcement service that
participates directly in the final merge decision rather than another
SHA-bound check.

## Deployment verification

After changing these protections, use test pull requests to verify all of the
following before relying on them:

1. A normal pull request cannot merge with zero Approver approvals and can
   merge after one non-author Approver approves it.
2. A formal Design Doc pull request cannot merge with one approval and can
   merge after two distinct non-author Approvers approve it.
3. Deleting or renaming a formal Design Doc out of its directory still requires
   two approvals.
4. Modifying a legacy Markdown Design Doc whose name does not follow the
   current recommendation still triggers header inspection and two approvals.
5. A feature pull request with a nonexistent Design Doc path fails the Design
   Doc Policy check.
6. Removing a visual blocking label does not allow a failing required check to
   merge.
7. Pushing a new commit dismisses stale approvals and requires a fresh approval
   of the latest reviewable push.
8. Editing the pull-request title, body, or labels without changing its commit
   starts a new evaluation and invalidates the previous policy result when the
   workflow begins; separately verify the body-only-link limitation documented
   above against the chosen deployment policy.
9. Changing `OWNERS_ALIASES` without updating the Mergify Approver matrix, or
   vice versa, fails the trusted policy check.

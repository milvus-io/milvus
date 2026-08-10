# Repository governance enforcement

Milvus repository rules are enforced through required checks. Labels remain
useful feedback, but a label alone is not a merge boundary because it can be
removed and reapplied asynchronously.

## Required checks

Repository administrators must enable Mergify Merge Protections and require the
`Mergify Merge Protections` check in GitHub branch protection or rulesets for
`master`. Configure the check for the Mergify GitHub App, enforce it for
administrators, and do not grant the merge robot a bypass that can ignore a
failing protection.

Release branches use a staged rollout. The validator deliberately requires the
current `base=master` guards exactly, so expansion needs a bootstrap change
rather than a one-step Mergify edit:

1. On `master`, first update the validator to accept both the current
   master-only shape and the intended next-branch shape, while leaving the live
   Mergify rules master-only.
2. Backport the trusted workflow, validator, and matching `.github/mergify.yml`
   snapshot to the target release branch and verify that the workflow publishes
   the explicit head-SHA check there.
3. Expand the default-branch Mergify rules to the release branch, synchronize
   that configuration snapshot to the release branch, and seed the trusted
   check on every open release pull request.
4. Only then require Mergify Merge Protections on the release branch. The
   validator may be tightened to the new exact branch set in a follow-up.

A `pull_request_target` workflow is loaded from the pull request's target
branch; enabling the gate first would either block release pull requests without
evaluating them or leave only an untrusted same-name check.

The same protected branches must require pull requests. GitHub's native review
settings may still be used, but they do not replace the Prow approval flow or
the trusted policy check described below. In particular, `/approve` comments
are not GitHub Review objects and are not dismissed by GitHub's stale-review
setting.

During the initial `master` rollout, the Mergify configuration enforces:

- the existing Prow `approved` label and a successful narrow `Approval Policy`
  check for every pull request;
- the dedicated `approved/design-doc` label for a pull request that adds,
  modifies, renames, removes, or moves a formal Design Doc, together with a
  successful `Design Doc Policy` check;
- a successful `Design Doc Policy` check for changes to the Mergify,
  Approver-alias, workflow, or policy-validator enforcement files;
- the native feature Design Doc condition: feature pull requests must add,
  modify, or link a formal Design Doc in Mergify's live PR state, while
  preserving the repository's existing `[automated]` title exception, and must
  pass the scoped `Design Doc Policy` check.

Approvers are the members of the `maintainers` alias in `OWNERS_ALIASES`.
Mergify cannot inspect `/approve` comment history or determine whether an
approval came from the pull-request author, so it does not calculate the
approval count itself. The narrow `Approval Policy` workflow reads the target
base SHA's `OWNERS_ALIASES` and Prow's current `[APPROVALNOTIFIER]` comment,
extracts the explicit approvals Prow actually recognized, excludes the author,
and requires one valid non-author Approver. The separate `Design Doc Policy`
workflow reuses the same approval-snapshot parser only when a pull request
changes a formal Design Doc and then requires two distinct non-author
Approvers. The ordinary approval gate does not execute or depend on Design Doc
path, blob, metadata, feature-link, default-branch, comment-reminder, or
governance validation; the separate Design Doc workflow may still evaluate an
ordinary PR, but Mergify does not require its result for that PR.

The Prow snapshot follows the repository's current approval behavior: an
`/approve` command adds an approval, `/approve cancel` and `/remove-approve`
remove it, an approved GitHub Review adds it, and a Request Changes review
removes it. A plain `/lgtm` comment alone is not an approval, but Prow can
render an `LGTM` entry when an eligible GitHub Review also carried `/lgtm`; the
policy checks trust that Prow-computed entry. Prow recalculates those inputs and
publishes the resulting approver list through the notifier comment; the trusted
checks count only distinct non-author members of the base-branch Approver set
from that list. They do not independently reinterpret an edited or deleted
human comment before Prow has processed a supported event.
Commenting `/approve` is therefore sufficient; clicking GitHub's Review
`Approve` button is not required.

For ordinary pull requests, the Approval Policy workflow also preserves Prow's
existing manually added `approved` label bypass. It reads the immutable
issue-event history and applies Prow's current provenance rule, with one added
restriction: the current label's latest `labeled` event must come from neither
the Prow account nor the pull-request author. Other existing automation actors
continue to behave as they do under Prow. This compatibility path does not
count toward the two explicit Approvers required for a Design Doc.

The repository's one configured no-human exception is the tested Knowhere
commit-update automation. It applies only to a pull request authored by
`sre-ci-robot` with the exact automated title, exactly one modification to
Knowhere's pinned CMake file, and the existing `ci-passed` label. The workflow
recognizes the same contract that Mergify uses to add its legacy `lgtm` and
`approved` labels. Only a pull request whose trusted author, exact title, and
`ci-passed` label identify it as a possible Knowhere update causes the narrow
Approval checker to request the file list; every other ordinary pull request
avoids that API. The exception does not apply to Design Doc or
repository-governance changes, and ordinary pull requests cannot opt into it by
copying only the title or label.

The shared approval-snapshot parser mirrors the current external Prow
configuration and comment format. It pins both the `sre-ci-robot` login and its
GitHub numeric user ID,
checks that approval links point back to the same pull request, and fails closed
on format drift. If that configuration, account, or format changes, update this
validator and its snapshot tests in the same rollout. Mergify still requires
Prow's own `approved` label, so a mismatch blocks rather than replacing Prow's
file-coverage decision. Because `sre-ci-robot` is a shared user account, its
credentials remain part of the trust boundary; a dedicated Prow GitHub App is
the stronger long-term identity.

The `approved/design-doc` label is maintained by the Design Doc workflow as
visible PR state and is created automatically if the repository does not have
it yet.
The label is not the security boundary: a user with label permission can add it
manually. The same trusted check independently verifies the two approvals and
removes a stale or manually added label when the requirement is not met.

When an enforcement file itself changes, the Design Doc workflow also reads the proposed
`OWNERS_ALIASES` and Mergify configuration from the pull-request head as
untrusted data. It rejects Mergify configuration drift that removes the Prow
label, dedicated Design Doc label, or trusted-check gates. Changes to the
workflows or validators keep the normal one-non-author-Approver threshold, but
their behavior must be reviewed as code rather than inferred from the check
validating itself.
Runtime approval identity always comes from the target base SHA, never from the
pull-request head.

The current repository ownership model resolves Approvers through the
`maintainers` alias. If Milvus later introduces path-specific Approvers outside
that alias or `no_parent_owners` boundaries, extend the validator to resolve the
base branch's full OWNERS tree before enabling that ownership change.

## Approval and Design Doc policy checks

The two trusted workflows run independently for `pull_request_target` state
changes and pull-request conversation-comment changes, and never execute
pull-request code. `Approval Policy` has only the API surface and read
permissions needed to validate the ordinary one-Approver rule. `Design Doc
Policy` separately handles formal-document classification, two approvals,
feature references, governance validation, the dedicated label, and advisory
metadata reminders.

GitHub Review and inline-review-comment events first run the separate
unprivileged `Design Doc Policy Review Signal` workflow. Its completed
`workflow_run` then wakes both trusted default-branch workflows, which refetch
the pull request and consume no signal artifacts or pull-request code. Because
GitHub does not reliably populate `workflow_run.pull_requests` for review
events, the signal's fixed run name carries the PR number; each trusted workflow
accepts it only when the fetched PR's head SHA and branch match the
GitHub-authenticated workflow-run metadata. (`workflow_run.head_repository`
identifies the base repository for fork review events, so it cannot authenticate
the PR's head repository.) Each trusted workflow explicitly publishes its own
check on the pull-request head SHA. The Design Doc workflow reads changed blobs
and approval records through the GitHub API, treats every Markdown file under
the formal Design Doc directory as a Design Doc regardless of legacy filename
or subdirectory, verifies feature Design Doc references, synchronizes the
Design-Doc approval label, reports findings in one maintained comment, and
validates proposed governance files.

The signal workflow has no write permissions, so changing it in a governance
pull request cannot publish a passing policy result or execute code in the
trusted workflows. Such a change can still suppress the optional GitHub Review
relay until it is reviewed and merged. The primary `/approve`,
`/approve cancel`, and `/remove-approve` path uses `issue_comment` directly in
both trusted workflows and does not depend on that signal. A dedicated GitHub
App/webhook is the stronger long-term option if Review UI events must have the
same availability guarantee.

The first step in each workflow marks every matching check on that head SHA as
in progress (or creates one), before checkout or validator tests; failures
after that step fail only that check closed. The checks use different names,
external IDs, concurrency groups, tests, and cleanup paths. Per-step timeouts
leave budget for an always-run cleanup that re-enumerates the same head and
policy identity, so a partially failed start or validator step does not leave a
required check permanently pending. A missing or nonexistent feature Design
Doc, insufficient formal-document approvals, label synchronization failure, or
governance drift fails `Design Doc Policy`. Metadata findings remain
reviewer-visible advisory reminders and do not fail it. Publishing explicit
head-SHA checks is required because each workflow job's own
`pull_request_target` status belongs to the base branch SHA.

A referenced Design Doc path is accepted when it exists at the pull-request
head, the target base SHA, or the base repository's current default branch.
This lets release backports reference the durable Design Doc on the default
branch without copying it into every release branch, while misspelled or
nonexistent paths still fail the policy check. A path that the current pull
request deletes or renames out of the formal Design Doc directory is excluded
from this lookup and cannot satisfy the feature requirement through an older
copy at the base or default branch.

The current workflows publish through the repository's `github-actions` App.
Mergify requires the successful check applicable to each rule and rejects any
same-name check from that App that is pending, stale, skipped, neutral,
cancelled, timed out, or failed; therefore a second successful GitHub Actions
job cannot mask the trusted
check's non-success state. This is still not a cryptographically dedicated
publisher identity: fork workflows also publish their automatic job checks as
the `github-actions` App, even though they do not receive a write-capable
repository token. For the strongest trust boundary, provision a dedicated
GitHub App for this check and change the Mergify app qualifier to that App.

GitHub check conclusions are keyed to a commit SHA; pull-request titles,
descriptions, and labels are mutable without changing that SHA. The Design Doc
workflow's first-step invalidation and Mergify's live reference condition close
the normal stale-result path, but they cannot make a body-only reference update
atomic with the merge decision. A strictly commit-bound policy must instead
require the feature pull request to add or modify its Design Doc. Supporting
body-only links with a zero-race guarantee requires an enforcement service that
participates directly in the final merge decision rather than another
SHA-bound check.

Approval events are also asynchronous: there is a short interval between an
approval or revocation and the trusted workflows marking existing checks in
progress. The repository should keep Mergify's live Prow and dedicated-label
conditions enabled, but a zero-race approval boundary ultimately requires a
dedicated GitHub App or merge service that validates the approval timeline at
the final merge decision.

## Deployment verification

After changing these protections, use test pull requests to verify all of the
following before relying on them:

Before enabling Mergify Merge Protections, trigger both trusted workflows once
on every already-open pull request so each current head has the explicit checks
required for its scope. Repeat the same seeding step when each release branch
is added later.

1. Except for the narrowly identified existing Knowhere automation and the
   preserved non-author, non-Prow added-label path above, a normal pull request
   cannot merge with zero Approver approvals and can merge after one non-author
   Approver comments `/approve`.
2. A formal Design Doc pull request cannot merge with one approval and can
   merge after two distinct non-author Approvers approve it; the workflow adds
   `approved/design-doc` only in the second case.
3. An author self-approval cannot satisfy either threshold even if Prow has
   added its legacy `approved` label.
4. `/approve cancel`, `/remove-approve`, and Request Changes remove the relevant
   approval and remove `approved/design-doc` when fewer than two remain.
5. Editing an unrelated old comment into `/approve` does not count before Prow
   publishes a matching notifier snapshot; editing or deleting a human comment
   does not make either trusted check invent approval state different from
   Prow's.
6. Deleting or renaming a formal Design Doc out of its directory still requires
   two approvals.
7. Modifying a legacy Markdown Design Doc whose name does not follow the
   current recommendation still triggers an advisory metadata inspection and
   two approvals; missing metadata does not block the pull request.
8. A release-branch feature pull request can reference a Design Doc that exists
   only on the repository default branch, while a path absent from the
   pull-request head, target base, and default branch fails the Design Doc Policy
   check.
9. A non-author, non-Prow actor adding the ordinary `approved` label preserves
   the existing non-Design-Doc bypass, while the author or the Prow account
   doing so does not. Manually adding `approved/design-doc` never substitutes
   for the two explicit Design Doc Approvers, and the Design Doc workflow
   restores that label's canonical state.
10. A normal non-Design-Doc pull request remains mergeable when Design Doc
    parsing, blob lookup, link validation, or reminder-comment logic fails,
    because its Mergify rule depends only on `Approval Policy`.
11. Pushing a new commit recalculates the policy while preserving existing
    `/approve` state, matching the current Prow behavior.
12. Editing the pull-request title, body, or labels without changing its commit
    starts a new evaluation and invalidates the previous policy result when the
    workflow begins; separately verify the body-only-link limitation documented
    above against the chosen deployment policy.
13. Weakening or removing the Mergify label/check gates in the same pull request
    fails governance self-validation.

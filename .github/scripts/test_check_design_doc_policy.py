import base64
import importlib.util
import itertools
import json
import os
import pathlib
import re
import sys
import tempfile
import unittest
from unittest import mock


CHECKER_PATH = pathlib.Path(__file__).with_name("check_design_doc_policy.py")
REPOSITORY_ROOT = pathlib.Path(__file__).resolve().parents[2]
SPEC = importlib.util.spec_from_file_location("check_design_doc_policy", CHECKER_PATH)
checker = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = checker
SPEC.loader.exec_module(checker)


VALID_HEADER = """# MEP: Example

- Created: 2026-07-28
- Feature DRI: @feature-dri
- Primary Approver: @primary-reviewer
- Independent Approver: @independent-reviewer
- Design Review: 2026-07-28
- Status: Draft

## Summary
"""
PROW_APPROVERS_UNSET = object()
TEST_APPROVAL_COMMAND = re.compile(
    r"^/([^\s]+)[\t ]*([^\n\r]*)",
    re.MULTILINE,
)


def prow_notification(*approvers, comment_id=700, titles=None):
    titles = ("Approved",) * len(approvers) if titles is None else tuple(titles)
    if len(titles) != len(approvers):
        raise ValueError("Each rendered approver needs one title")
    rendered = ", ".join(
        f'*<a href="https://github.com/milvus-io/milvus/pull/1#" '
        f'title="{title}">{login}</a>*'
        for login, title in zip(approvers, titles)
    )
    return {
        "id": comment_id,
        "user": {
            "login": checker.PROW_BOT_LOGIN,
            "id": checker.PROW_BOT_USER_ID,
        },
        "body": (
            f"{checker.PROW_APPROVAL_NOTIFICATION_PREFIX} This PR is **APPROVED**\n\n"
            f"This pull-request has been approved by: {rendered}\n\n"
            "The full list of commands accepted by this bot follows."
        ),
        "created_at": "2026-08-08T00:00:00Z",
        "updated_at": "2026-08-08T00:00:00Z",
    }


def infer_prow_approvers(comments):
    states = {}
    for comment in comments:
        login = comment["user"]["login"]
        for match in TEST_APPROVAL_COMMAND.finditer(comment.get("body") or ""):
            command = match.group(1).casefold()
            arguments = match.group(2).strip().casefold()
            if command == "approve":
                states[login.casefold()] = "cancel" not in arguments
            elif command == "remove-approve":
                states[login.casefold()] = False
    return tuple(sorted(login for login, approved in states.items() if approved))


class GovernanceConfigTest(unittest.TestCase):
    def test_mergify_uses_prow_and_dedicated_design_doc_labels(self):
        owners_aliases = (REPOSITORY_ROOT / "OWNERS_ALIASES").read_text(
            encoding="utf-8"
        )
        mergify = (REPOSITORY_ROOT / ".github/mergify.yml").read_text(encoding="utf-8")

        self.assertEqual(
            [], checker.validate_approver_governance(owners_aliases, mergify)
        )
        self.assertNotIn("approved-reviews-by", mergify)
        protections = checker.merge_protections_section(mergify)
        general_rule = checker.configuration_rule(protections, "Review / Prow approval")
        self.assertEqual(
            checker.MASTER_ONLY_IF_LINES,
            checker.if_condition_lines(general_rule),
        )
        self.assertEqual(
            checker.GENERAL_REVIEW_SUCCESS_LINES,
            checker.success_condition_lines(general_rule),
        )

        design_rule = checker.configuration_rule(
            protections, "Review / formal Design Doc"
        )
        self.assertEqual(
            (*checker.MASTER_ONLY_IF_LINES, "      - *design_doc_area"),
            checker.if_condition_lines(design_rule),
        )
        self.assertEqual(
            checker.DESIGN_DOC_REVIEW_SUCCESS_LINES,
            checker.success_condition_lines(design_rule),
        )

        governance_rule = checker.configuration_rule(
            protections, "Review / governance enforcement"
        )
        self.assertEqual(
            (*checker.MASTER_ONLY_IF_LINES, "      - *governance_area"),
            checker.if_condition_lines(governance_rule),
        )
        self.assertEqual(
            checker.POLICY_CHECK_SUCCESS_LINES,
            checker.success_condition_lines(governance_rule),
        )

    def test_formal_design_doc_classifier_matches_repository_tree(self):
        mergify = (REPOSITORY_ROOT / ".github/mergify.yml").read_text(encoding="utf-8")
        anchor_match = re.search(
            r"^  design_doc_file: &design_doc_file 'files~=(.+)'$",
            mergify,
            re.MULTILINE,
        )
        self.assertIsNotNone(anchor_match)
        mergify_pattern = re.compile(anchor_match.group(1))

        formal_root = REPOSITORY_ROOT / "docs/design-docs/design_docs"
        paths = sorted(
            path.relative_to(REPOSITORY_ROOT).as_posix()
            for path in formal_root.rglob("*.md")
        )
        self.assertGreater(len(paths), 0)
        for path in paths:
            with self.subTest(path=path):
                self.assertTrue(checker.is_design_doc_path(path))
                self.assertIsNotNone(mergify_pattern.fullmatch(path))

        legacy_paths = [
            "docs/design-docs/design_docs/README.md",
            "docs/design-docs/design_docs/Legacy Topic/Old Design.md",
        ]
        for path in legacy_paths:
            with self.subTest(path=path):
                self.assertTrue(checker.is_design_doc_path(path))
                self.assertIsNotNone(mergify_pattern.fullmatch(path))

    def test_feature_policy_preserves_the_existing_automated_exception(self):
        mergify = (REPOSITORY_ROOT / ".github/mergify.yml").read_text(encoding="utf-8")
        protection = mergify.split("  - name: Docs / feature Design Doc policy", 1)[
            1
        ].split("# PULL REQUEST RULES", 1)[0]
        label_rule = mergify.split(
            "  - name: Blocking PR if feat PR missing design doc", 1
        )[1].split("  - name: Dismiss block label if design doc is provided", 1)[0]
        automated_rule = mergify.split(
            "  - name: Dismiss block label if automated create PR", 1
        )[1].split("  - name: Blocking PR if feat PR missing design doc", 1)[0]

        self.assertIn(r"-title~=\[automated\]", protection)
        self.assertIn(r"-title~=\[automated\]", label_rule)
        self.assertIn("do-not-merge/missing-design-doc", automated_rule)

    def test_review_events_relay_to_the_trusted_policy_workflow(self):
        signal_workflow = (
            REPOSITORY_ROOT / ".github/workflows/design-doc-policy-review-signal.yml"
        ).read_text(encoding="utf-8")
        trusted_workflow = (
            REPOSITORY_ROOT / ".github/workflows/design-doc-policy.yml"
        ).read_text(encoding="utf-8")

        self.assertIn("  pull_request_review:\n", signal_workflow)
        self.assertIn("    types: [submitted, edited, dismissed]\n", signal_workflow)
        self.assertIn("  pull_request_review_comment:\n", signal_workflow)
        self.assertIn("    types: [created, edited, deleted]\n", signal_workflow)
        self.assertIn(
            'run-name: "${{ github.event.pull_request.number }}"\n',
            signal_workflow,
        )
        self.assertIn("  workflow_run:\n", trusted_workflow)
        self.assertIn(
            "    workflows: [Design Doc Policy Review Signal]\n", trusted_workflow
        )
        self.assertIn(
            "github.event.workflow_run.display_title || github.run_id",
            trusted_workflow,
        )
        self.assertNotIn("workflow_run.pull_requests[0]", trusted_workflow)
        self.assertIn(
            "pullResponse.data.head.sha !== workflowRun.head_sha", trusted_workflow
        )
        self.assertIn(
            "pullResponse.data.head.ref !== workflowRun.head_branch", trusted_workflow
        )
        self.assertNotIn("workflowRun.head_repository", trusted_workflow)

    def test_trusted_workflow_bounds_failures_and_uses_least_privilege(self):
        trusted_workflow = (
            REPOSITORY_ROOT / ".github/workflows/design-doc-policy.yml"
        ).read_text(encoding="utf-8")

        self.assertIn("    timeout-minutes: 20\n", trusted_workflow)
        self.assertGreaterEqual(trusted_workflow.count("        timeout-minutes:"), 5)
        self.assertIn("      pull-requests: read\n", trusted_workflow)
        self.assertNotIn("      pull-requests: write\n", trusted_workflow)
        self.assertIn('core.setOutput("head_sha", headSha);', trusted_workflow)
        self.assertIn('core.setOutput("external_id", externalId);', trusted_workflow)
        self.assertIn(
            "${{ always() && steps.validate-policy.outcome != 'success' &&\n"
            "          steps.start-policy-check.outputs.head_sha != '' }}",
            trusted_workflow,
        )
        self.assertNotIn(
            "steps.start-policy-check.outcome == 'success'", trusted_workflow
        )
        self.assertIn(
            "POLICY_HEAD_SHA: ${{ steps.start-policy-check.outputs.head_sha }}",
            trusted_workflow,
        )
        self.assertIn(
            "POLICY_EXTERNAL_ID: "
            "${{ steps.start-policy-check.outputs.external_id }}",
            trusted_workflow,
        )


class GovernanceValidationTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.owners_aliases = (REPOSITORY_ROOT / "OWNERS_ALIASES").read_text(
            encoding="utf-8"
        )
        cls.mergify = (REPOSITORY_ROOT / ".github/mergify.yml").read_text(
            encoding="utf-8"
        )

    def comment_out_rule_line(self, rule_name: str, line: str) -> str:
        marker = f"  - name: {rule_name}"
        rule_start = self.mergify.index(marker)
        rule_end = self.mergify.find("\n  - name:", rule_start + len(marker))
        if rule_end == -1:
            rule_end = len(self.mergify)
        line_start = self.mergify.index(f"\n{line}\n", rule_start, rule_end) + 1
        indentation = line[: len(line) - len(line.lstrip())]
        commented_line = f"{indentation}# {line.lstrip()}"
        return (
            self.mergify[:line_start]
            + commented_line
            + self.mergify[line_start + len(line) :]
        )

    def replace_rule_fragment(
        self, rule_name: str, fragment: str, replacement: str
    ) -> str:
        marker = f"  - name: {rule_name}"
        rule_start = self.mergify.index(marker)
        rule_end = self.mergify.find("\n  - name:", rule_start + len(marker))
        if rule_end == -1:
            rule_end = len(self.mergify)
        fragment_start = self.mergify.index(fragment, rule_start, rule_end)
        return (
            self.mergify[:fragment_start]
            + replacement
            + self.mergify[fragment_start + len(fragment) :]
        )

    def test_detects_governance_changes_on_either_side_of_rename(self):
        for file_info in (
            {"filename": ".github/mergify.yml", "status": "modified"},
            {
                "filename": ".github/workflows/design-doc-policy-review-signal.yml",
                "status": "modified",
            },
            {
                "filename": ".github/workflows/approval-policy.yml",
                "status": "modified",
            },
            {
                "filename": ".github/scripts/test_check_approval_policy.py",
                "status": "modified",
            },
            {
                "filename": "archive/old-policy.py",
                "previous_filename": ".github/scripts/check_approval_policy.py",
                "status": "renamed",
            },
        ):
            with self.subTest(file_info=file_info):
                self.assertTrue(checker.governance_enforcement_changed([file_info]))
        self.assertFalse(
            checker.governance_enforcement_changed(
                [{"filename": "docs/design-docs/README.md", "status": "modified"}]
            )
        )

    def test_rejects_github_review_conditions(self):
        drifted = self.mergify.replace(
            "  # Build and test status conditions",
            "  approved_by_example: &approved_by_example "
            "'approved-reviews-by = example'\n\n"
            "  # Build and test status conditions",
        )
        issues = checker.validate_approver_governance(self.owners_aliases, drifted)
        self.assertTrue(any("Prow approval labels" in issue for issue in issues))

    def test_rejects_merge_protection_reporting_drift(self):
        for original, replacement in (
            ("  reporting_method: check-runs", "  reporting_method: deployments"),
            ("  post_comment: false", "  post_comment: true"),
            ("merge_protections_settings:", "disabled_merge_protections_settings:"),
        ):
            with self.subTest(original=original):
                drifted = self.mergify.replace(original, replacement, 1)
                issues = checker.validate_approver_governance(
                    self.owners_aliases, drifted
                )
                self.assertTrue(
                    any("required check-run configuration" in issue for issue in issues)
                )

    def test_rejects_general_rule_drift(self):
        cases = (
            "      - base=master",
            "      - label=approved",
            "      - '-check-stale = @github-actions/Approval Policy'",
        )
        for line in cases:
            with self.subTest(line=line):
                drifted = self.comment_out_rule_line("Review / Prow approval", line)
                issues = checker.validate_approver_governance(
                    self.owners_aliases, drifted
                )
                self.assertTrue(any("general review rule" in issue for issue in issues))

    def test_rejects_design_doc_rule_drift(self):
        cases = (
            (
                "      - base=master",
                "does not cover the formal Design Doc area",
            ),
            (
                "      - *design_doc_area",
                "does not cover the formal Design Doc area",
            ),
            (
                "      - label=approved/design-doc",
                "do not require exactly the approved/design-doc label",
            ),
        )
        for line, expected in cases:
            with self.subTest(line=line):
                drifted = self.comment_out_rule_line("Review / formal Design Doc", line)
                issues = checker.validate_approver_governance(
                    self.owners_aliases, drifted
                )
                self.assertTrue(any(expected in issue for issue in issues))

    def test_rejects_governance_rule_drift(self):
        cases = (
            (
                "      - base=master",
                "do not require the trusted policy check",
            ),
            (
                "      - *governance_area",
                "do not require the trusted policy check",
            ),
            (
                "      - '-check-stale = @github-actions/Design Doc Policy'",
                "do not require exact Design Doc Policy success",
            ),
        )
        for line, expected in cases:
            with self.subTest(line=line):
                drifted = self.comment_out_rule_line(
                    "Review / governance enforcement", line
                )
                issues = checker.validate_approver_governance(
                    self.owners_aliases, drifted
                )
                self.assertTrue(any(expected in issue for issue in issues))

    def test_rejects_tested_knowhere_automation_drift(self):
        for line in (
            f"      - author={checker.AUTOMATED_KNOWHERE_AUTHOR}",
            f"      - 'title={checker.AUTOMATED_KNOWHERE_TITLE}'",
            f"      - modified-files={checker.AUTOMATED_KNOWHERE_FILE}",
            "      - '#files=1'",
            "      - label=ci-passed",
            "          - lgtm",
            "          - approved",
        ):
            with self.subTest(line=line):
                drifted = self.comment_out_rule_line(
                    checker.AUTOMATED_KNOWHERE_RULE_NAME,
                    line,
                )
                issues = checker.validate_approver_governance(
                    self.owners_aliases, drifted
                )
                self.assertTrue(
                    any(
                        "Knowhere-update approval automation" in issue
                        for issue in issues
                    )
                )

    def test_rejects_automated_feature_compatibility_drift(self):
        cases = (
            (
                checker.AUTOMATED_FEATURE_CLEANUP_RULE_NAME,
                "          - do-not-merge/missing-design-doc",
                "automated-PR cleanup",
            ),
            (
                checker.FEATURE_MISSING_DOC_RULE_NAME,
                r"      - -title~=\[automated\]",
                "automated-title exception",
            ),
        )
        for rule_name, line, expected in cases:
            with self.subTest(rule=rule_name):
                drifted = self.comment_out_rule_line(rule_name, line)
                issues = checker.validate_approver_governance(
                    self.owners_aliases, drifted
                )
                self.assertTrue(any(expected in issue for issue in issues))

    def test_merge_protections_section_cannot_be_disabled(self):
        drifted = self.mergify.replace(
            "\nmerge_protections:\n",
            "\ndisabled_merge_protections:\n",
            1,
        )
        issues = checker.validate_approver_governance(self.owners_aliases, drifted)
        self.assertTrue(any("merge_protections" in issue for issue in issues))

    def test_rule_triggers_reject_extra_never_matching_condition(self):
        cases = (
            (
                "Review / formal Design Doc",
                "    if:\n      - base=master\n      - *design_doc_area",
                "formal Design Doc area",
            ),
            (
                "Review / governance enforcement",
                "    if:\n      - base=master\n      - *governance_area",
                "trusted policy check",
            ),
            (
                "Docs / repository governance policy",
                "    if:\n      - base=master\n      - *governance_area",
                "Governance changes",
            ),
            (
                "Docs / formal Design Doc policy",
                "    if:\n      - base=master\n      - *design_doc_area",
                "Design Doc changes",
            ),
            (
                "Docs / feature Design Doc policy",
                "    if:\n"
                "      - base=master\n"
                "      - or:\n"
                "          - 'title~=^feat:'\n"
                "          - label=kind/feature\n"
                "      - -title~=\\[automated\\]",
                "Feature policy trigger",
            ),
        )
        for rule_name, trigger, expected_issue in cases:
            with self.subTest(rule=rule_name):
                drifted = self.replace_rule_fragment(
                    rule_name,
                    trigger,
                    f"{trigger}\n      - base = __never__",
                )
                issues = checker.validate_approver_governance(
                    self.owners_aliases, drifted
                )
                self.assertTrue(any(expected_issue in issue for issue in issues))

    def test_policy_rules_require_each_trusted_check_state_guard(self):
        rule_names = (
            "Review / Prow approval",
            "Review / governance enforcement",
            "Docs / repository governance policy",
            "Docs / formal Design Doc policy",
            "Docs / feature Design Doc policy",
        )
        states = (
            "failure",
            "neutral",
            "skipped",
            "cancelled",
            "timed-out",
            "pending",
            "stale",
        )
        for rule_name, state in itertools.product(rule_names, states):
            with self.subTest(rule=rule_name, state=state):
                check_name = (
                    "Approval Policy"
                    if rule_name == "Review / Prow approval"
                    else "Design Doc Policy"
                )
                condition = f"      - '-check-{state} = @github-actions/{check_name}'"
                drifted = self.replace_rule_fragment(rule_name, condition, "")
                issues = checker.validate_approver_governance(
                    self.owners_aliases, drifted
                )
                self.assertTrue(issues)

    def test_detects_area_path_drift(self):
        design_drift = self.mergify.replace(
            "  design_doc_area: &design_doc_area "
            "'files~=^docs/design-docs/design_docs/.+\\.md$'",
            "  design_doc_area: &design_doc_area "
            "'files~=^docs/design-docs/drafts/.+\\.md$'",
        )
        self.assertTrue(
            any(
                "Design Doc area matcher" in issue
                for issue in checker.validate_approver_governance(
                    self.owners_aliases, design_drift
                )
            )
        )

        governance_drift = self.mergify.replace(
            "|test_check_design_doc_policy)\\.py)$'",
            ")\\.py)$'",
        )
        self.assertTrue(
            any(
                "governance area matcher" in issue
                for issue in checker.validate_approver_governance(
                    self.owners_aliases, governance_drift
                )
            )
        )

    def test_feature_policy_requires_each_native_design_doc_condition(self):
        rule_start = self.mergify.index("  - name: Docs / feature Design Doc policy")
        rule_end = self.mergify.index("# PULL REQUEST RULES", rule_start)
        for alias in (
            "added_design_doc",
            "modified_design_doc",
            "design_doc_body",
        ):
            with self.subTest(alias=alias):
                condition = f"          - *{alias}\n"
                condition_start = self.mergify.index(condition, rule_start, rule_end)
                drifted = (
                    self.mergify[:condition_start]
                    + self.mergify[condition_start + len(condition) :]
                )
                issues = checker.validate_approver_governance(
                    self.owners_aliases, drifted
                )
                self.assertTrue(
                    any("native Design Doc requirement" in issue for issue in issues)
                )

    def test_changed_governance_is_validated_from_pull_request_head(self):
        client = FakeRepositoryFileClient(
            {
                checker.OWNERS_ALIASES_PATH: self.owners_aliases,
                checker.MERGIFY_CONFIG_PATH: self.mergify,
            }
        )
        issues = checker.validate_changed_governance(
            client,
            "contributor/milvus",
            "head-sha",
            [{"filename": checker.OWNERS_ALIASES_PATH, "status": "modified"}],
        )
        self.assertEqual([], issues)
        self.assertEqual(
            [
                (
                    "contributor/milvus",
                    checker.OWNERS_ALIASES_PATH,
                    "head-sha",
                ),
                (
                    "contributor/milvus",
                    checker.MERGIFY_CONFIG_PATH,
                    "head-sha",
                ),
            ],
            client.requests,
        )


class ApprovalRequirementTest(unittest.TestCase):
    def test_formal_design_doc_change_includes_delete_and_rename_out(self):
        path = "docs/design-docs/design_docs/example.md"
        self.assertTrue(
            checker.formal_design_doc_changed([{"filename": path, "status": "removed"}])
        )
        self.assertTrue(
            checker.formal_design_doc_changed(
                [
                    {
                        "filename": "archive/example.md",
                        "previous_filename": path,
                        "status": "renamed",
                    }
                ]
            )
        )
        self.assertFalse(
            checker.formal_design_doc_changed(
                [
                    {
                        "filename": "docs/design-docs/design_docs/image.png",
                        "status": "added",
                    }
                ]
            )
        )

    def test_evaluation_loads_trusted_approvers_from_base_revision(self):
        owners = "aliases:\n  maintainers:\n    - Bob\n"
        client = FakeRepositoryFileClient({checker.OWNERS_ALIASES_PATH: owners})
        state = checker.PullRequestState(
            head_sha="head",
            base_sha="base",
            head_repository="fork/milvus",
            base_repository="milvus-io/milvus",
            author="author",
            title="docs: test",
            body="",
            labels=(),
        )
        approval = checker.evaluate_design_doc_approval_requirement(
            client,
            "milvus-io/milvus",
            1,
            state,
            [prow_notification("Bob")],
        )
        self.assertEqual(("Bob",), approval.approvers)
        self.assertEqual(
            [("milvus-io/milvus", checker.OWNERS_ALIASES_PATH, "base")],
            client.requests,
        )

    def test_design_doc_evaluation_always_requires_two_approvers(self):
        owners = "aliases:\n  maintainers:\n    - Bob\n    - Carol\n"
        client = FakeRepositoryFileClient({checker.OWNERS_ALIASES_PATH: owners})
        state = checker.PullRequestState(
            head_sha="head",
            base_sha="base",
            head_repository="fork/milvus",
            base_repository="milvus-io/milvus",
            author="author",
            title="docs: test",
            body="",
            labels=(),
        )
        approval = checker.evaluate_design_doc_approval_requirement(
            client,
            "milvus-io/milvus",
            1,
            state,
            [prow_notification("Bob")],
        )
        self.assertFalse(approval.satisfied)

    def test_bot_author_does_not_poison_two_design_doc_approvals(self):
        owners = "aliases:\n  maintainers:\n    - Bob\n    - Carol\n"
        client = FakeRepositoryFileClient({checker.OWNERS_ALIASES_PATH: owners})
        state = checker.PullRequestState(
            head_sha="head",
            base_sha="base",
            head_repository="fork/milvus",
            base_repository="milvus-io/milvus",
            author="mergify[bot]",
            title="docs: test",
            body="",
            labels=(),
        )
        approval = checker.evaluate_design_doc_approval_requirement(
            client,
            "milvus-io/milvus",
            1,
            state,
            [
                prow_notification(
                    "mergify[bot]",
                    "Bob",
                    "Carol",
                    titles=(
                        "Author self-approved",
                        "Approved",
                        "Approved",
                    ),
                )
            ],
        )

        self.assertEqual(("Bob", "Carol"), approval.approvers)
        self.assertTrue(approval.satisfied)


class ApprovalLabelTest(unittest.TestCase):
    class Client:
        def __init__(self):
            self.ensured = []
            self.added = []
            self.removed = []

        def ensure_repository_label(self, repository, name, color, description):
            self.ensured.append((repository, name, color, description))

        def add_pull_request_label(self, repository, pull_number, label):
            self.added.append((repository, pull_number, label))

        def remove_pull_request_label(self, repository, pull_number, label):
            self.removed.append((repository, pull_number, label))

    def approval(self, approvers):
        return checker.ApprovalRequirement(approvers=tuple(approvers))

    def test_adds_dedicated_label_after_two_approvers(self):
        client = self.Client()
        checker.sync_design_doc_approval_label(
            client,
            "milvus-io/milvus",
            7,
            (),
            self.approval(["Bob", "Carol"]),
        )
        self.assertEqual(1, len(client.ensured))
        self.assertEqual(
            [("milvus-io/milvus", 7, checker.DESIGN_DOC_APPROVAL_LABEL)],
            client.added,
        )
        self.assertEqual([], client.removed)

    def test_removes_manual_or_stale_dedicated_label(self):
        for approval in (self.approval(["Bob"]),):
            with self.subTest(approval=approval):
                client = self.Client()
                checker.sync_design_doc_approval_label(
                    client,
                    "milvus-io/milvus",
                    7,
                    (checker.DESIGN_DOC_APPROVAL_LABEL,),
                    approval,
                )
                self.assertEqual(
                    [("milvus-io/milvus", 7, checker.DESIGN_DOC_APPROVAL_LABEL)],
                    client.removed,
                )
                self.assertEqual([], client.added)

    def test_state_comparison_ignores_only_managed_label(self):
        base = checker.PullRequestState(
            "head",
            "base",
            "fork/milvus",
            "milvus-io/milvus",
            "author",
            "docs: test",
            "",
            ("approved",),
        )
        managed_label_added = checker.PullRequestState(
            **{
                **base.__dict__,
                "labels": ("approved", checker.DESIGN_DOC_APPROVAL_LABEL),
            }
        )
        other_label_added = checker.PullRequestState(
            **{**base.__dict__, "labels": ("approved", "kind/feature")}
        )
        self.assertEqual(
            checker.stable_pull_request_state(base),
            checker.stable_pull_request_state(managed_label_added),
        )
        self.assertNotEqual(
            checker.stable_pull_request_state(base),
            checker.stable_pull_request_state(other_label_added),
        )


class EventLoadingTest(unittest.TestCase):
    def load(self, payload):
        with tempfile.NamedTemporaryFile(mode="w", encoding="utf-8") as event_file:
            json.dump(payload, event_file)
            event_file.flush()
            return checker.load_event(event_file.name)

    def test_supports_pull_request_events(self):
        for event_name in (
            "pull_request_target",
            "pull_request_review_comment",
            "pull_request_review",
        ):
            with self.subTest(event=event_name):
                self.assertEqual(
                    ("milvus-io/milvus", 12),
                    self.load(
                        {
                            "repository": {"full_name": "milvus-io/milvus"},
                            "pull_request": {"number": 12},
                        }
                    ),
                )

    def test_supports_issue_comment_for_pull_request_only(self):
        self.assertEqual(
            ("milvus-io/milvus", 12),
            self.load(
                {
                    "repository": {"full_name": "milvus-io/milvus"},
                    "issue": {"number": 12, "pull_request": {"url": "x"}},
                }
            ),
        )
        with self.assertRaisesRegex(RuntimeError, "not a valid pull request"):
            self.load(
                {
                    "repository": {"full_name": "milvus-io/milvus"},
                    "issue": {"number": 12},
                }
            )

    def test_supports_review_signal_workflow_run(self):
        self.assertEqual(
            ("milvus-io/milvus", 12),
            self.load(
                {
                    "repository": {"full_name": "milvus-io/milvus"},
                    "workflow_run": {
                        "display_title": "12",
                        "pull_requests": [],
                    },
                }
            ),
        )
        for display_title in (None, "PR #12", "0"):
            with self.subTest(display_title=display_title):
                with self.assertRaisesRegex(RuntimeError, "not a valid pull request"):
                    self.load(
                        {
                            "repository": {"full_name": "milvus-io/milvus"},
                            "workflow_run": {"display_title": display_title},
                        }
                    )


class GitHubClientApprovalApiTest(unittest.TestCase):
    def test_pull_request_state_includes_author_and_base_repository(self):
        client = checker.GitHubClient("token", "https://api.github.test")
        client.request = lambda *args, **kwargs: {
            "head": {
                "sha": "head-sha",
                "repo": {"full_name": "contributor/milvus"},
            },
            "base": {
                "sha": "base-sha",
                "repo": {"full_name": "milvus-io/milvus"},
            },
            "user": {"login": "contributor"},
            "title": "docs: test",
            "body": None,
            "labels": [{"name": "approved"}],
        }
        state = client.get_pull_request_state("milvus-io/milvus", 8)
        self.assertEqual("contributor", state.author)
        self.assertEqual("milvus-io/milvus", state.base_repository)
        self.assertEqual("contributor/milvus", state.head_repository)
        self.assertEqual(("approved",), state.labels)

    def test_label_creation_tolerates_concurrent_creator(self):
        client = checker.GitHubClient("token", "https://api.github.test")
        calls = []

        def request(method, path, payload=None, allow_not_found=False):
            calls.append((method, path, allow_not_found))
            if len(calls) == 1:
                return None
            if len(calls) == 2:
                raise RuntimeError("HTTP 422: already_exists")
            return {"name": checker.DESIGN_DOC_APPROVAL_LABEL}

        client.request = request
        client.ensure_repository_label(
            "milvus-io/milvus",
            checker.DESIGN_DOC_APPROVAL_LABEL,
            checker.DESIGN_DOC_APPROVAL_LABEL_COLOR,
            checker.DESIGN_DOC_APPROVAL_LABEL_DESCRIPTION,
        )
        self.assertEqual(["GET", "POST", "GET"], [call[0] for call in calls])


class HeaderValidationTest(unittest.TestCase):
    def test_accepts_canonical_header(self):
        self.assertEqual([], checker.validate_header(VALID_HEADER))

    def test_reports_each_missing_field(self):
        required_lines = {
            "Feature DRI": "- Feature DRI: @feature-dri\n",
            "Primary Approver": "- Primary Approver: @primary-reviewer\n",
            "Independent Approver": "- Independent Approver: @independent-reviewer\n",
            "Design Review": "- Design Review: 2026-07-28\n",
        }
        for field, line in required_lines.items():
            with self.subTest(field=field):
                issues = checker.validate_header(VALID_HEADER.replace(line, ""))
                self.assertTrue(any(field in issue for issue in issues))

    def test_rejects_invalid_github_logins(self):
        invalid_logins = [
            "feature-dri",
            "@-feature-dri",
            "@feature-dri-",
            "@feature--dri",
            "@github-login",
            "@" + ("a" * 40),
        ]
        for login in invalid_logins:
            with self.subTest(login=login):
                document = VALID_HEADER.replace("@feature-dri", login)
                issues = checker.validate_header(document)
                self.assertTrue(any("Feature DRI" in issue for issue in issues))

    def test_rejects_placeholder_and_impossible_dates(self):
        for date_value in ("YYYY-MM-DD", "2026-02-30", "2026-7-28"):
            with self.subTest(date_value=date_value):
                document = VALID_HEADER.replace(
                    "2026-07-28\n- Status", f"{date_value}\n- Status"
                )
                issues = checker.validate_header(document)
                self.assertTrue(any("Design Review" in issue for issue in issues))

    def test_rejects_duplicate_fields(self):
        duplicate = "- Feature DRI: @feature-dri\n- Feature DRI: @another-dri\n"
        document = VALID_HEADER.replace("- Feature DRI: @feature-dri\n", duplicate)
        issues = checker.validate_header(document)
        self.assertTrue(any("exactly once" in issue for issue in issues))

    def test_rejects_duplicate_role_logins(self):
        document = VALID_HEADER.replace("@primary-reviewer", "@feature-dri")
        issues = checker.validate_header(document)
        self.assertTrue(any("three distinct GitHub users" in issue for issue in issues))

    def test_ignores_fields_in_fenced_code(self):
        document = VALID_HEADER.replace(
            "- Feature DRI: @feature-dri\n",
            "~~~markdown\n- Feature DRI: @feature-dri\n~~~\n",
        )
        issues = checker.validate_header(document)
        self.assertTrue(any("Feature DRI" in issue for issue in issues))

    def test_does_not_treat_fence_with_trailing_text_as_closing(self):
        document = VALID_HEADER.replace(
            "- Feature DRI: @feature-dri\n",
            "~~~markdown\n"
            "~~~ this is not a closing fence\n"
            "- Feature DRI: @fake-example\n"
            "~~~\n",
        )
        issues = checker.validate_header(document)
        self.assertTrue(any("Feature DRI" in issue for issue in issues))

    def test_does_not_treat_tab_indented_fence_as_closing(self):
        document = VALID_HEADER.replace(
            "- Feature DRI: @feature-dri\n",
            "~~~markdown\n\t~~~\n- Feature DRI: @fake-example\n~~~\n",
        )
        issues = checker.validate_header(document)
        self.assertTrue(any("Feature DRI" in issue for issue in issues))

    def test_ignores_fields_in_html_comments(self):
        document = VALID_HEADER.replace(
            "- Feature DRI: @feature-dri\n",
            "<!--\n- Feature DRI: @feature-dri\n-->\n",
        )
        issues = checker.validate_header(document)
        self.assertTrue(any("Feature DRI" in issue for issue in issues))

    def test_does_not_reconstruct_exact_field_after_html_comment(self):
        document = VALID_HEADER.replace(
            "- Feature DRI: @feature-dri\n",
            "<!--hidden-->- Feature DRI: @fake-example\n",
        )
        issues = checker.validate_header(document)
        self.assertTrue(any("Feature DRI" in issue for issue in issues))

    def test_ignores_fields_hidden_in_cdata(self):
        document = VALID_HEADER.replace(
            "- Feature DRI: @feature-dri\n",
            "<![CDATA[\n- Feature DRI: @fake-example\n]]>\n",
        )
        issues = checker.validate_header(document)
        self.assertTrue(any("Feature DRI" in issue for issue in issues))

    def test_ignores_fields_hidden_in_processing_instruction(self):
        document = VALID_HEADER.replace(
            "- Feature DRI: @feature-dri\n",
            "<?hidden\n- Feature DRI: @fake-example\n?>\n",
        )
        issues = checker.validate_header(document)
        self.assertTrue(any("Feature DRI" in issue for issue in issues))

    def test_does_not_treat_unicode_separator_as_markdown_line_break(self):
        document = VALID_HEADER.replace(
            "- Feature DRI: @feature-dri\n",
            "intro\u2028- Feature DRI: @fake-example\u2028outro\n",
        )
        issues = checker.validate_header(document)
        self.assertTrue(any("Feature DRI" in issue for issue in issues))

    def test_ignores_fields_in_raw_html_code_blocks(self):
        document = VALID_HEADER.replace(
            "- Feature DRI: @feature-dri\n",
            "<pre>\n- Feature DRI: @fake-example\n</pre>\n",
        )
        issues = checker.validate_header(document)
        self.assertTrue(any("Feature DRI" in issue for issue in issues))

    def test_ignores_fields_in_multiline_raw_html_opening_tag(self):
        document = VALID_HEADER.replace(
            "- Feature DRI: @feature-dri\n",
            "<pre\n>\n- Feature DRI: @fake-example\n</pre>\n",
        )
        issues = checker.validate_header(document)
        self.assertTrue(any("Feature DRI" in issue for issue in issues))

    def test_ignores_fields_hidden_in_details(self):
        document = VALID_HEADER.replace(
            "- Feature DRI: @feature-dri\n",
            "<details>\n- Feature DRI: @fake-example\n</details>\n",
        )
        issues = checker.validate_header(document)
        self.assertTrue(any("Feature DRI" in issue for issue in issues))

    def test_stops_at_inline_raw_html_before_metadata(self):
        document = VALID_HEADER.replace(
            "- Feature DRI: @feature-dri\n",
            "intro <details>\n- Feature DRI: @fake-example\n</details>\n",
        )
        issues = checker.validate_header(document)
        self.assertTrue(any("Feature DRI" in issue for issue in issues))

    def test_stops_at_inline_html_heading_before_metadata(self):
        document = VALID_HEADER.replace(
            "- Feature DRI: @feature-dri\n",
            "intro <h2>Summary</h2>\n- Feature DRI: @feature-dri\n",
        )
        issues = checker.validate_header(document)
        self.assertTrue(any("Feature DRI" in issue for issue in issues))

    def test_stops_at_inline_hidden_html_with_form_feed(self):
        document = VALID_HEADER.replace(
            "- Feature DRI: @feature-dri\n",
            "intro <details\f>\n- Feature DRI: @fake-example\n</details>\n",
        )
        issues = checker.validate_header(document)
        self.assertTrue(any("Feature DRI" in issue for issue in issues))

    def test_ignores_fields_hidden_in_nested_html(self):
        document = VALID_HEADER.replace(
            "- Feature DRI: @feature-dri\n",
            "<details>\n"
            "<details>inner</details>\n"
            "- Feature DRI: @fake-example\n"
            "</details>\n",
        )
        issues = checker.validate_header(document)
        self.assertTrue(any("Feature DRI" in issue for issue in issues))

    def test_does_not_treat_non_void_html_slash_as_self_closing(self):
        document = VALID_HEADER.replace(
            "- Feature DRI: @feature-dri\n",
            "<details/>\n- Feature DRI: @fake-example\n</details>\n",
        )
        issues = checker.validate_header(document)
        self.assertTrue(any("Feature DRI" in issue for issue in issues))

    def test_requires_exact_space_after_field_colon(self):
        document = VALID_HEADER.replace(
            "- Feature DRI: @feature-dri\n", "- Feature DRI:@feature-dri\n"
        )
        issues = checker.validate_header(document)
        self.assertTrue(any("Feature DRI" in issue for issue in issues))

    def test_rejects_trailing_whitespace_in_field(self):
        document = VALID_HEADER.replace(
            "- Feature DRI: @feature-dri\n", "- Feature DRI: @feature-dri \n"
        )
        issues = checker.validate_header(document)
        self.assertTrue(any("Feature DRI" in issue for issue in issues))

    def test_rejects_field_after_first_section(self):
        document = VALID_HEADER.replace("- Feature DRI: @feature-dri\n", "")
        document += "\n- Feature DRI: @feature-dri\n"
        issues = checker.validate_header(document)
        self.assertTrue(any("Feature DRI" in issue for issue in issues))

    def test_rejects_field_after_deeper_atx_section(self):
        document = VALID_HEADER.replace(
            "- Feature DRI: @feature-dri\n",
            "### Details\n\n- Feature DRI: @feature-dri\n",
        )
        issues = checker.validate_header(document)
        self.assertTrue(any("Feature DRI" in issue for issue in issues))

    def test_rejects_field_after_second_atx_h1(self):
        document = VALID_HEADER.replace(
            "- Feature DRI: @feature-dri\n",
            "# Architecture\n\n- Feature DRI: @feature-dri\n",
        )
        issues = checker.validate_header(document)
        self.assertTrue(any("Feature DRI" in issue for issue in issues))

    def test_rejects_field_after_setext_h2_section(self):
        document = VALID_HEADER.replace(
            "- Feature DRI: @feature-dri\n",
            "Summary\n-------\n\n- Feature DRI: @feature-dri\n",
        )
        issues = checker.validate_header(document)
        self.assertTrue(any("Feature DRI" in issue for issue in issues))

    def test_rejects_field_after_setext_h2_with_inline_comment(self):
        document = VALID_HEADER.replace(
            "- Feature DRI: @feature-dri\n",
            "Summary <!--hidden-->\n-------\n\n- Feature DRI: @feature-dri\n",
        )
        issues = checker.validate_header(document)
        self.assertTrue(any("Feature DRI" in issue for issue in issues))

    def test_rejects_field_after_second_setext_h1(self):
        document = VALID_HEADER.replace(
            "- Feature DRI: @feature-dri\n",
            "Architecture\n============\n\n- Feature DRI: @feature-dri\n",
        )
        issues = checker.validate_header(document)
        self.assertTrue(any("Feature DRI" in issue for issue in issues))

    def test_allows_angle_bracket_type_in_title(self):
        document = VALID_HEADER.replace("# MEP: Example", "# Support Array<VARCHAR>")
        self.assertEqual([], checker.validate_header(document))

    def test_rejects_field_outside_header_line_limit(self):
        document = VALID_HEADER.replace("- Feature DRI: @feature-dri\n", "").replace(
            "## Summary\n", ""
        )
        document += ("\n" * checker.MAX_HEADER_LINES) + "- Feature DRI: @feature-dri\n"
        issues = checker.validate_header(document)
        self.assertTrue(any("Feature DRI" in issue for issue in issues))


class FileSelectionTest(unittest.TestCase):
    def test_formal_design_doc_classifier(self):
        accepted = [
            "docs/design-docs/design_docs/20260728-example.md",
            "docs/design-docs/design_docs/cdc/20260728-example_design.md",
            "docs/design-docs/design_docs/README.md",
            "docs/design-docs/design_docs/segcore/Search.md",
            "docs/design-docs/design_docs/Legacy Topic/Old Design.md",
        ]
        rejected = [
            "docs/design-docs/design_docs/20260728-example.txt",
            "docs/design-docs/design_docs/../20260728-example.md",
            "docs/design-docs/TEMPLATE.md",
            "docs/user_guides/20260728-example.md",
        ]
        for path in accepted:
            with self.subTest(path=path):
                self.assertTrue(checker.is_design_doc_path(path))
        for path in rejected:
            with self.subTest(path=path):
                self.assertFalse(checker.is_design_doc_path(path))

    def test_selects_changed_markdown_docs_and_skips_deletions(self):
        files = [
            {
                "filename": "docs/design-docs/design_docs/20260728-new.md",
                "status": "added",
                "sha": "1",
            },
            {
                "filename": "docs/design-docs/design_docs/cdc/20260728-nested.md",
                "status": "modified",
                "sha": "2",
            },
            {
                "filename": "docs/design-docs/design_docs/20260728-renamed.md",
                "previous_filename": "docs/old.md",
                "status": "renamed",
                "sha": "3",
            },
            {
                "filename": "docs/design-docs/design_docs/20260728-removed.md",
                "status": "removed",
                "sha": "4",
            },
            {
                "filename": "docs/design-docs/design_docs/segcore/Search.md",
                "status": "modified",
                "sha": "5",
            },
            {"filename": "docs/README.md", "status": "modified", "sha": "5"},
        ]
        selected = checker.select_design_doc_files(files)
        self.assertEqual(
            [
                "docs/design-docs/design_docs/20260728-new.md",
                "docs/design-docs/design_docs/20260728-renamed.md",
                "docs/design-docs/design_docs/cdc/20260728-nested.md",
                "docs/design-docs/design_docs/segcore/Search.md",
            ],
            [file_info["filename"] for file_info in selected],
        )

    def test_skips_rename_out_but_selects_rename_in(self):
        files = [
            {
                "filename": "docs/archive/20260728-old.md",
                "previous_filename": ("docs/design-docs/design_docs/20260728-old.md"),
                "status": "renamed",
                "sha": "old",
            },
            {
                "filename": "docs/design-docs/design_docs/20260728-new.md",
                "previous_filename": "docs/drafts/new.md",
                "status": "renamed",
                "sha": "new",
            },
        ]
        self.assertEqual(
            ["docs/design-docs/design_docs/20260728-new.md"],
            [
                file_info["filename"]
                for file_info in checker.select_design_doc_files(files)
            ],
        )

    def test_legacy_paths_are_selected_but_supporting_files_are_not(self):
        files = [
            {
                "filename": "docs/design-docs/design_docs/README.md",
                "status": "added",
            },
            {
                "filename": "docs/design-docs/design_docs/assets/diagram.png",
                "status": "added",
            },
            {
                "filename": "docs/design-docs/design_docs/Bad/20260728-doc.md",
                "status": "renamed",
                "previous_filename": "docs/drafts/doc.md",
            },
            {
                "filename": "docs/design-docs/design_docs/20260728-valid.md",
                "status": "added",
            },
            {
                "filename": "docs/design-docs/design_docs/legacy.md",
                "status": "removed",
            },
            {
                "filename": "docs/archive/legacy.md",
                "previous_filename": "docs/design-docs/design_docs/legacy.md",
                "status": "renamed",
            },
        ]
        self.assertEqual(
            [
                "docs/design-docs/design_docs/20260728-valid.md",
                "docs/design-docs/design_docs/Bad/20260728-doc.md",
                "docs/design-docs/design_docs/README.md",
            ],
            [
                file_info["filename"]
                for file_info in checker.select_design_doc_files(files)
            ],
        )

    def test_decodes_utf8_blob(self):
        content = VALID_HEADER.encode("utf-8")
        blob = {
            "size": len(content),
            "encoding": "base64",
            "content": base64.b64encode(content).decode("ascii"),
        }
        self.assertEqual(VALID_HEADER, checker.decode_blob(blob))

    def test_reports_every_invalid_changed_document(self):
        client = FakeBlobClient(
            {
                "valid": VALID_HEADER,
                "missing": VALID_HEADER.replace("- Feature DRI: @feature-dri\n", ""),
                "bad-date": VALID_HEADER.replace(
                    "2026-07-28\n- Status", "2026-02-30\n- Status"
                ),
            }
        )
        files = [
            {
                "filename": "docs/design-docs/design_docs/20260728-valid.md",
                "status": "modified",
                "sha": "valid",
            },
            {
                "filename": "docs/design-docs/design_docs/20260728-missing.md",
                "status": "modified",
                "sha": "missing",
            },
            {
                "filename": "docs/design-docs/design_docs/20260728-bad-date.md",
                "status": "added",
                "sha": "bad-date",
            },
        ]
        issues = checker.validate_changed_design_docs(client, "milvus-io/milvus", files)
        self.assertEqual(
            [
                "docs/design-docs/design_docs/20260728-bad-date.md",
                "docs/design-docs/design_docs/20260728-missing.md",
            ],
            sorted(issues),
        )


class FeatureRequirementTest(unittest.TestCase):
    def test_extracts_only_exact_formal_design_doc_lines(self):
        body = """Summary

design doc: docs/design-docs/design_docs/20260728-example.md
- Design Doc: docs/design-docs/design_docs/cdc/20260728-nested_example.md
design doc: docs/design-docs/design_docs/README.md
design doc: docs/design-docs/design_docs/Legacy Topic/Old Design.md
design doc: docs/design-docs/design_docs/../escape.md
inline docs/design-docs/design_docs/20260728-inline.md reference
"""
        self.assertEqual(
            [
                "docs/design-docs/design_docs/20260728-example.md",
                "docs/design-docs/design_docs/Legacy Topic/Old Design.md",
                "docs/design-docs/design_docs/README.md",
                "docs/design-docs/design_docs/cdc/20260728-nested_example.md",
            ],
            checker.extract_design_doc_references(body),
        )

    def test_non_feature_does_not_require_design_doc(self):
        client = mock.Mock()
        self.assertIsNone(
            checker.validate_feature_design_doc_requirement(
                client,
                "milvus-io/milvus",
                "base",
                "contributor/milvus",
                "head",
                "fix: example",
                [],
                "",
                [],
            )
        )
        client.file_exists.assert_not_called()

    def test_automated_title_text_preserves_feature_requirement_exception(self):
        client = mock.Mock()
        issue = checker.validate_feature_design_doc_requirement(
            client,
            "milvus-io/milvus",
            "base",
            "contributor/milvus",
            "head",
            "feat: [automated] example",
            [],
            "",
            [],
        )
        self.assertIsNone(issue)
        client.file_exists.assert_not_called()

    def test_added_or_modified_formal_doc_satisfies_feature_requirement(self):
        for status in ("added", "modified"):
            with self.subTest(status=status):
                client = mock.Mock()
                files = [
                    {
                        "filename": (
                            "docs/design-docs/design_docs/Legacy Topic/Old Design.md"
                        ),
                        "status": status,
                        "sha": "doc",
                    }
                ]
                self.assertIsNone(
                    checker.validate_feature_design_doc_requirement(
                        client,
                        "milvus-io/milvus",
                        "base",
                        "contributor/milvus",
                        "head",
                        "feat: example",
                        [],
                        "",
                        files,
                    )
                )
                client.file_exists.assert_not_called()
                client.get_default_branch.assert_not_called()

    def test_referenced_doc_at_pr_head_satisfies_feature_requirement(self):
        client = mock.Mock()
        client.file_exists.return_value = True
        path = "docs/design-docs/design_docs/20260728-new.md"

        self.assertIsNone(
            checker.validate_feature_design_doc_requirement(
                client,
                "milvus-io/milvus",
                "release-base",
                "contributor/milvus",
                "head",
                "feat: example",
                [],
                f"design doc: {path}",
                [],
            )
        )
        client.file_exists.assert_called_once_with("contributor/milvus", path, "head")
        client.get_default_branch.assert_not_called()

    def test_existing_referenced_doc_satisfies_feature_requirement(self):
        client = mock.Mock()
        client.file_exists.side_effect = [False, True]
        path = "docs/design-docs/design_docs/Legacy Topic/Old Design.md"
        self.assertIsNone(
            checker.validate_feature_design_doc_requirement(
                client,
                "milvus-io/milvus",
                "base",
                "contributor/milvus",
                "head",
                "docs: implementation",
                ["kind/feature"],
                f"design doc: {path}",
                [],
            )
        )
        client.file_exists.assert_has_calls(
            [
                mock.call("contributor/milvus", path, "head"),
                mock.call("milvus-io/milvus", path, "base"),
            ]
        )
        client.get_default_branch.assert_not_called()

    def test_default_branch_doc_satisfies_release_feature_requirement(self):
        client = mock.Mock()
        client.file_exists.side_effect = [False, False, True]
        client.get_default_branch.return_value = "trunk"
        path = "docs/design-docs/design_docs/Legacy Topic/Old Design.md"

        self.assertIsNone(
            checker.validate_feature_design_doc_requirement(
                client,
                "milvus-io/milvus",
                "release-base",
                "contributor/milvus",
                "head",
                "feat: release backport",
                [],
                f"design doc: {path}",
                [],
            )
        )
        client.get_default_branch.assert_called_once_with("milvus-io/milvus")
        self.assertEqual(
            [
                mock.call("contributor/milvus", path, "head"),
                mock.call("milvus-io/milvus", path, "release-base"),
                mock.call("milvus-io/milvus", path, "trunk"),
            ],
            client.file_exists.call_args_list,
        )

    def test_missing_or_nonexistent_reference_fails_feature_requirement(self):
        client = mock.Mock()
        client.file_exists.return_value = False
        client.get_default_branch.return_value = "trunk"
        missing = checker.validate_feature_design_doc_requirement(
            client,
            "milvus-io/milvus",
            "base",
            "contributor/milvus",
            "head",
            "feat: example",
            [],
            "",
            [],
        )
        self.assertIn("must add or update", missing)
        client.file_exists.assert_not_called()
        client.get_default_branch.assert_not_called()

        nonexistent = checker.validate_feature_design_doc_requirement(
            client,
            "milvus-io/milvus",
            "base",
            "contributor/milvus",
            "head",
            "feat: example",
            [],
            "design doc: docs/design-docs/design_docs/20260728-missing.md",
            [],
        )
        self.assertIn("None of the formal design-document paths", nonexistent)
        client.get_default_branch.assert_called_once_with("milvus-io/milvus")
        self.assertEqual(
            [
                mock.call(
                    "contributor/milvus",
                    "docs/design-docs/design_docs/20260728-missing.md",
                    "head",
                ),
                mock.call(
                    "milvus-io/milvus",
                    "docs/design-docs/design_docs/20260728-missing.md",
                    "base",
                ),
                mock.call(
                    "milvus-io/milvus",
                    "docs/design-docs/design_docs/20260728-missing.md",
                    "trunk",
                ),
            ],
            client.file_exists.call_args_list,
        )

    def test_default_branch_is_loaded_once_for_multiple_missing_references(self):
        client = mock.Mock()
        client.file_exists.return_value = False
        client.get_default_branch.return_value = "mainline"

        issue = checker.validate_feature_design_doc_requirement(
            client,
            "milvus-io/milvus",
            "release-base",
            "contributor/milvus",
            "head",
            "feat: example",
            [],
            "\n".join(
                [
                    "design doc: docs/design-docs/design_docs/20260728-a.md",
                    "design doc: docs/design-docs/design_docs/20260728-b.md",
                ]
            ),
            [],
        )

        self.assertIn("None of the formal design-document paths", issue)
        client.get_default_branch.assert_called_once_with("milvus-io/milvus")
        self.assertEqual(6, client.file_exists.call_count)

    def test_default_branch_lookup_failure_propagates(self):
        client = mock.Mock()
        client.file_exists.return_value = False
        client.get_default_branch.side_effect = RuntimeError("repository unavailable")

        with self.assertRaisesRegex(RuntimeError, "repository unavailable"):
            checker.validate_feature_design_doc_requirement(
                client,
                "milvus-io/milvus",
                "release-base",
                "contributor/milvus",
                "head",
                "feat: example",
                [],
                "design doc: docs/design-docs/design_docs/20260728-example.md",
                [],
            )

    def test_deletion_or_rename_out_does_not_satisfy_feature_requirement(self):
        client = mock.Mock()
        client.file_exists.return_value = True
        deleted_path = "docs/design-docs/design_docs/Legacy Topic/Deleted Design.md"
        renamed_path = "docs/design-docs/design_docs/Old Area/Renamed Design.md"
        files = [
            {
                "filename": deleted_path,
                "status": "removed",
                "sha": "deleted",
            },
            {
                "filename": "docs/archive/renamed-design.md",
                "previous_filename": renamed_path,
                "status": "renamed",
                "sha": "renamed",
            },
        ]
        issue = checker.validate_feature_design_doc_requirement(
            client,
            "milvus-io/milvus",
            "base",
            "contributor/milvus",
            "head",
            "feat: example",
            [],
            f"design doc: {deleted_path}\ndesign doc: {renamed_path}",
            files,
        )
        self.assertIn("None of the formal design-document paths", issue)
        client.file_exists.assert_not_called()
        client.get_default_branch.assert_not_called()


class GitHubClientRepositoryTest(unittest.TestCase):
    def test_reads_default_branch_from_repository_metadata(self):
        client = checker.GitHubClient("token", "https://api.github.test")
        client.request = mock.Mock(return_value={"default_branch": "trunk"})

        self.assertEqual("trunk", client.get_default_branch("milvus-io/milvus"))
        client.request.assert_called_once_with("GET", "/repos/milvus-io/milvus")

    def test_rejects_invalid_default_branch_metadata(self):
        client = checker.GitHubClient("token", "https://api.github.test")
        for response in (None, {}, {"default_branch": ""}, {"default_branch": 1}):
            with self.subTest(response=response):
                client.request = mock.Mock(return_value=response)
                with self.assertRaisesRegex(
                    RuntimeError, "invalid repository response"
                ):
                    client.get_default_branch("milvus-io/milvus")


class CommentTest(unittest.TestCase):
    def test_comment_reports_multiple_files_and_escapes_filenames(self):
        comment = checker.build_comment(
            {
                "docs/design-docs/design_docs/z.md": ["Missing field."],
                "docs/design-docs/design_docs/<bad>\n@name.md": ["Invalid field."],
            }
        )
        self.assertIn(checker.COMMENT_MARKER, comment)
        self.assertIn("&lt;bad&gt;\\n@name.md", comment)
        self.assertLess(comment.index("&lt;bad&gt;"), comment.index("z.md"))

    def test_comment_is_capped_when_many_documents_are_invalid(self):
        issues = {
            f"docs/design-docs/design_docs/{index:04d}-{'x' * 100}.md": [
                "Missing field."
            ]
            for index in range(1000)
        }
        comment = checker.build_comment(issues)
        self.assertLessEqual(len(comment), checker.MAX_COMMENT_CHARS)
        self.assertIn("additional design document(s) omitted", comment)

    def test_comment_includes_feature_requirement_failure(self):
        comment = checker.build_comment({}, "Feature Design Doc is missing.")
        self.assertIn("Feature design document requirement", comment)
        self.assertIn("Feature Design Doc is missing.", comment)

    def test_only_selects_owned_marker_comments(self):
        comments = [
            {
                "id": 1,
                "body": checker.COMMENT_PREFIX + "owned",
                "user": {"login": checker.BOT_LOGIN},
            },
            {
                "id": 2,
                "body": checker.COMMENT_PREFIX + "contributor",
                "user": {"login": "contributor"},
            },
            {
                "id": 3,
                "body": "another bot comment\n" + checker.COMMENT_MARKER,
                "user": {"login": checker.BOT_LOGIN},
            },
            {
                "id": 4,
                "body": checker.COMMENT_MARKER + "\n## Previous heading\n",
                "user": {"login": checker.BOT_LOGIN},
            },
        ]
        self.assertEqual(
            [1],
            [comment["id"] for comment in checker.matching_bot_comments(comments)],
        )

    def test_sync_updates_one_bot_comment_and_removes_duplicates(self):
        client = FakeCommentClient()
        checker.sync_comment(client, "milvus-io/milvus", 1, "new body")
        self.assertEqual([(10, "new body")], client.updated)
        self.assertEqual([11], client.deleted)
        self.assertEqual([], client.created)

    def test_sync_removes_reminder_after_success(self):
        client = FakeCommentClient()
        checker.sync_comment(client, "milvus-io/milvus", 1, None)
        self.assertEqual([10, 11], client.deleted)
        self.assertEqual([], client.updated)

    def test_sync_creates_comment_when_no_reminder_exists(self):
        client = FakeCommentClient(comments=[])
        checker.sync_comment(client, "milvus-io/milvus", 1, "new body")
        self.assertEqual(["new body"], client.created)

    def test_sync_does_not_update_unchanged_comment(self):
        body = checker.COMMENT_PREFIX + "same body"
        client = FakeCommentClient(
            comments=[
                {
                    "id": 10,
                    "body": body,
                    "user": {"login": checker.BOT_LOGIN},
                }
            ]
        )
        checker.sync_comment(client, "milvus-io/milvus", 1, body)
        self.assertEqual([], client.updated)
        self.assertEqual([], client.created)
        self.assertEqual([], client.deleted)


class GitHubClientCheckRunTest(unittest.TestCase):
    def test_reuses_and_completes_every_matching_check_on_same_head(self):
        client = checker.GitHubClient("token", "https://api.github.test")
        calls = []

        def request(method, path, payload=None, allow_not_found=False):
            calls.append((method, path, payload))
            if method == "GET":
                return {
                    "check_runs": [
                        {
                            "id": 40,
                            "external_id": "design-doc-policy-pr-7",
                            "app": {"slug": "github-actions"},
                        },
                        {
                            "id": 42,
                            "external_id": "design-doc-policy-pr-7",
                            "app": {"slug": "github-actions"},
                        },
                        {
                            "id": 99,
                            "external_id": "some-other-check",
                            "app": {"slug": "github-actions"},
                        },
                    ]
                }
            if method == "PATCH":
                return None
            raise AssertionError(f"Unexpected request: {method} {path}")

        client.request = request
        check_run_id = client.create_policy_check("milvus-io/milvus", "head", 7)
        self.assertEqual(42, check_run_id)
        self.assertEqual(["PATCH", "PATCH"], [call[0] for call in calls[1:]])
        self.assertTrue(all(call[2]["status"] == "in_progress" for call in calls[1:]))

        client.complete_policy_check(
            "milvus-io/milvus", check_run_id, "failure", "failed", "summary"
        )
        completion_calls = calls[-2:]
        self.assertEqual(
            [
                "/repos/milvus-io/milvus/check-runs/40",
                "/repos/milvus-io/milvus/check-runs/42",
            ],
            [call[1] for call in completion_calls],
        )
        self.assertTrue(
            all(call[2]["conclusion"] == "failure" for call in completion_calls)
        )

    def test_creates_check_when_head_has_no_matching_policy_check(self):
        client = checker.GitHubClient("token", "https://api.github.test")
        calls = []

        def request(method, path, payload=None, allow_not_found=False):
            calls.append((method, path, payload))
            if method == "GET":
                return {"check_runs": []}
            if method == "POST":
                return {"id": 99}
            raise AssertionError(f"Unexpected request: {method} {path}")

        client.request = request
        self.assertEqual(
            99, client.create_policy_check("milvus-io/milvus", "new-head", 8)
        )
        self.assertEqual(["GET", "POST"], [call[0] for call in calls])


class RunTest(unittest.TestCase):
    def run_with_client(
        self,
        client,
        head_sha="head",
        base_sha="base",
        title="docs: test",
        body="",
        labels=None,
        author="contributor",
        event_title=None,
        event_body=None,
        event_labels=None,
    ):
        live_labels = labels if labels is not None else []
        client.configure_default_states(
            head_sha=head_sha,
            base_sha=base_sha,
            title=title,
            body=body,
            labels=live_labels,
            author=author,
        )
        event = {
            "repository": {"full_name": "milvus-io/milvus"},
            "pull_request": {
                "number": 1,
                "head": {
                    "sha": head_sha,
                    "repo": {"full_name": "contributor/milvus"},
                },
                "base": {"sha": base_sha},
                "title": event_title if event_title is not None else title,
                "body": event_body if event_body is not None else body,
                "labels": [
                    {"name": label}
                    for label in (
                        event_labels if event_labels is not None else live_labels
                    )
                ],
            },
        }
        with tempfile.NamedTemporaryFile(mode="w", encoding="utf-8") as event_file:
            json.dump(event, event_file)
            event_file.flush()
            with mock.patch.dict(
                os.environ, {"GH_TOKEN": "test-token"}
            ), mock.patch.object(checker, "GitHubClient", return_value=client):
                with mock.patch("builtins.print"):
                    return checker.run(event_file.name)

    def approval_comment(self, login, body, second):
        return {
            "id": 800 + second,
            "body": body,
            "created_at": f"2026-08-08T00:00:{second:02d}Z",
            "user": {"login": login},
        }

    def test_manual_label_never_counts_toward_the_design_doc_pair(self):
        client = FakeRunClient(
            files=[
                {
                    "filename": "docs/design-docs/design_docs/example.md",
                    "status": "modified",
                    "sha": "valid",
                }
            ],
            documents={"valid": VALID_HEADER},
            refs=("head", "base"),
            comments=[],
            approval_comments=[],
            prow_approvers=("congqixia",),
        )
        self.assertEqual(
            1,
            self.run_with_client(client, labels=[checker.APPROVED_LABEL]),
        )
        self.assertEqual("failure", client.completed_checks[-1][1])
        self.assertIn("(1/2)", client.completed_checks[-1][3])
        self.assertEqual([], client.added_labels)

    def test_governance_change_does_not_recheck_ordinary_approval(self):
        mergify = (REPOSITORY_ROOT / checker.MERGIFY_CONFIG_PATH).read_text(
            encoding="utf-8"
        )
        client = FakeRunClient(
            files=[
                {
                    "filename": checker.MERGIFY_CONFIG_PATH,
                    "status": "modified",
                }
            ],
            documents={},
            refs=("head", "base"),
            comments=[],
            repository_documents={checker.MERGIFY_CONFIG_PATH: mergify},
        )
        self.assertEqual(0, self.run_with_client(client))
        self.assertEqual("success", client.completed_checks[-1][1])
        self.assertNotIn(
            "Non-author Approver requirement", client.completed_checks[-1][3]
        )

    def test_non_doc_policy_path_does_not_parse_prow_approval(self):
        client = FakeRunClient(
            files=[],
            documents={},
            refs=("head", "base"),
            comments=[],
        )
        with mock.patch.object(
            checker,
            "extract_prow_approvers",
            side_effect=AssertionError("ordinary approval parser was called"),
        ):
            self.assertEqual(0, self.run_with_client(client))
        self.assertEqual("success", client.completed_checks[-1][1])

    def test_formal_design_doc_needs_two_and_removes_manual_label(self):
        client = FakeRunClient(
            files=[
                {
                    "filename": "docs/design-docs/design_docs/example.md",
                    "status": "modified",
                    "sha": "valid",
                }
            ],
            documents={"valid": VALID_HEADER},
            refs=("head", "base"),
            comments=[],
            approval_comments=[self.approval_comment("congqixia", "/approve", 1)],
        )
        self.assertEqual(
            1,
            self.run_with_client(client, labels=[checker.DESIGN_DOC_APPROVAL_LABEL]),
        )
        self.assertEqual(
            [
                (
                    "milvus-io/milvus",
                    1,
                    checker.DESIGN_DOC_APPROVAL_LABEL,
                )
            ],
            client.removed_labels,
        )
        self.assertEqual([], client.created)
        self.assertIn("(1/2)", client.completed_checks[-1][3])

    def test_design_doc_counts_only_distinct_non_author_approvers(self):
        design_doc = "docs/design-docs/design_docs/example.md"
        scenarios = {
            "same approver twice": [
                self.approval_comment("congqixia", "/approve", 1),
                self.approval_comment("congqixia", "/approve", 2),
            ],
            "author plus approver": [
                self.approval_comment("contributor", "/approve", 1),
                self.approval_comment("congqixia", "/approve", 2),
            ],
            "outsider plus approver": [
                self.approval_comment("outside-user", "/approve", 1),
                self.approval_comment("congqixia", "/approve", 2),
            ],
        }
        for name, approval_comments in scenarios.items():
            with self.subTest(name=name):
                client = FakeRunClient(
                    files=[
                        {
                            "filename": design_doc,
                            "status": "modified",
                            "sha": "valid",
                        }
                    ],
                    documents={"valid": VALID_HEADER},
                    refs=("head", "base"),
                    comments=[],
                    approval_comments=approval_comments,
                )
                self.assertEqual(1, self.run_with_client(client))
                self.assertEqual([], client.added_labels)
                self.assertIn("(1/2)", client.completed_checks[-1][3])

    def test_formal_design_doc_gets_dedicated_label_after_two_approvers(self):
        client = FakeRunClient(
            files=[
                {
                    "filename": "docs/design-docs/design_docs/example.md",
                    "status": "modified",
                    "sha": "valid",
                }
            ],
            documents={"valid": VALID_HEADER},
            refs=("head", "base"),
            comments=[],
            approval_comments=[
                self.approval_comment("congqixia", "/approve", 1),
                self.approval_comment("czs007", "/approve no-issue", 2),
            ],
        )
        self.assertEqual(0, self.run_with_client(client))
        self.assertEqual(1, len(client.ensured_labels))
        self.assertEqual(
            [
                (
                    "milvus-io/milvus",
                    1,
                    checker.DESIGN_DOC_APPROVAL_LABEL,
                )
            ],
            client.added_labels,
        )

    def test_label_write_failure_completes_policy_check_as_failure(self):
        client = FakeRunClient(
            files=[
                {
                    "filename": "docs/design-docs/design_docs/example.md",
                    "status": "modified",
                    "sha": "valid",
                }
            ],
            documents={"valid": VALID_HEADER},
            refs=("head", "base"),
            comments=[],
            label_error=True,
        )
        with self.assertRaisesRegex(RuntimeError, "label write failed"):
            self.run_with_client(client)
        self.assertEqual("failure", client.completed_checks[-1][1])
        self.assertEqual(
            "Design Doc policy could not be evaluated",
            client.completed_checks[-1][2],
        )

    def test_label_removal_failure_completes_policy_check_as_failure(self):
        client = FakeRunClient(
            files=[
                {
                    "filename": "docs/design-docs/design_docs/example.md",
                    "status": "modified",
                    "sha": "valid",
                }
            ],
            documents={"valid": VALID_HEADER},
            refs=("head", "base"),
            comments=[],
            approval_comments=[self.approval_comment("congqixia", "/approve", 1)],
        )
        client.remove_pull_request_label = mock.Mock(
            side_effect=RuntimeError("label removal failed")
        )
        with self.assertRaisesRegex(RuntimeError, "label removal failed"):
            self.run_with_client(
                client,
                labels=[checker.DESIGN_DOC_APPROVAL_LABEL],
            )
        self.assertEqual("failure", client.completed_checks[-1][1])
        self.assertEqual(
            "Design Doc policy could not be evaluated",
            client.completed_checks[-1][2],
        )

    def test_approval_api_failures_complete_policy_check_as_failure(self):
        for method_name in ("list_issue_comments",):
            with self.subTest(method_name=method_name):
                client = FakeRunClient(
                    files=[
                        {
                            "filename": "docs/design-docs/design_docs/example.md",
                            "status": "modified",
                            "sha": "valid",
                        }
                    ],
                    documents={"valid": VALID_HEADER},
                    refs=("head", "base"),
                    comments=[],
                )
                setattr(
                    client,
                    method_name,
                    mock.Mock(side_effect=RuntimeError("approval API failed")),
                )
                with self.assertRaisesRegex(RuntimeError, "approval API failed"):
                    self.run_with_client(client)
                self.assertEqual("failure", client.completed_checks[-1][1])
                self.assertEqual(
                    "Design Doc policy could not be evaluated",
                    client.completed_checks[-1][2],
                )

    def test_missing_or_malformed_base_approvers_fail_closed(self):
        malformed = "aliases:\n  maintainers: not-a-list\n"
        for name, repository_documents, error in (
            (
                "missing",
                {},
                RuntimeError("OWNERS_ALIASES missing"),
            ),
            (
                "malformed",
                {checker.OWNERS_ALIASES_PATH: malformed},
                None,
            ),
        ):
            with self.subTest(name=name):
                client = FakeRunClient(
                    files=[
                        {
                            "filename": "docs/design-docs/design_docs/example.md",
                            "status": "modified",
                            "sha": "valid",
                        }
                    ],
                    documents={"valid": VALID_HEADER},
                    refs=("head", "base"),
                    comments=[],
                    repository_documents=repository_documents,
                )
                if error is not None:
                    client.get_repository_file = mock.Mock(side_effect=error)
                with self.assertRaisesRegex(
                    RuntimeError, "Could not load trusted Approvers"
                ):
                    self.run_with_client(client)
                self.assertEqual("failure", client.completed_checks[-1][1])

    def test_pull_request_head_cannot_add_a_trusted_approver(self):
        client = FakeRunClient(
            files=[
                {
                    "filename": "docs/design-docs/design_docs/example.md",
                    "status": "modified",
                    "sha": "valid",
                }
            ],
            documents={"valid": VALID_HEADER},
            refs=("head", "base"),
            comments=[],
            approval_comments=[
                self.approval_comment("outside-user", "/approve", 1),
                self.approval_comment("congqixia", "/approve", 2),
            ],
        )
        base_owners = client.repository_documents[checker.OWNERS_ALIASES_PATH]
        requests = []

        def get_repository_file(repository, path, ref):
            requests.append((repository, path, ref))
            if repository == "contributor/milvus":
                return "aliases:\n  maintainers:\n    - outside-user\n"
            return base_owners

        client.get_repository_file = get_repository_file
        self.assertEqual(1, self.run_with_client(client))
        self.assertIn("(1/2)", client.completed_checks[-1][3])
        self.assertEqual(2, len(requests))
        self.assertTrue(
            all(
                request == ("milvus-io/milvus", checker.OWNERS_ALIASES_PATH, "base")
                for request in requests
            )
        )

    def test_cancel_and_remove_approve_revoke_sticky_approval(self):
        for revoke in ("/approve cancel", "/remove-approve"):
            with self.subTest(revoke=revoke):
                client = FakeRunClient(
                    files=[
                        {
                            "filename": "docs/design-docs/design_docs/example.md",
                            "status": "modified",
                            "sha": "valid",
                        }
                    ],
                    documents={"valid": VALID_HEADER},
                    refs=("head", "base"),
                    comments=[],
                    approval_comments=[
                        self.approval_comment("congqixia", "/approve", 1),
                        self.approval_comment("congqixia", revoke, 2),
                    ],
                )
                self.assertEqual(1, self.run_with_client(client))
                self.assertIn("(0/2)", client.completed_checks[-1][3])

    def test_prow_snapshot_change_removes_design_doc_label_fail_closed(self):
        design_doc = "docs/design-docs/design_docs/example.md"
        initial_comments = [
            self.approval_comment("congqixia", "/approve", 1),
            self.approval_comment("czs007", "/approve", 2),
        ]
        changed_comments = [
            *initial_comments,
            self.approval_comment("czs007", "/approve cancel", 3),
        ]
        client = FakeRunClient(
            files=[
                {
                    "filename": design_doc,
                    "status": "modified",
                    "sha": "valid",
                }
            ],
            documents={"valid": VALID_HEADER},
            refs=("head", "base"),
            comments=[],
            approval_comments=initial_comments,
        )
        snapshots = iter(
            (
                [
                    *initial_comments,
                    prow_notification("congqixia", "czs007", comment_id=700),
                ],
                [
                    *changed_comments,
                    prow_notification("congqixia", comment_id=701),
                ],
            )
        )

        def list_issue_comments(repository, pull_number):
            return [*client.comments, *next(snapshots)]

        client.list_issue_comments = list_issue_comments
        self.assertEqual(0, self.run_with_client(client))
        self.assertEqual("neutral", client.completed_checks[-1][1])
        self.assertEqual(
            [("milvus-io/milvus", 1, checker.DESIGN_DOC_APPROVAL_LABEL)],
            client.added_labels,
        )
        self.assertEqual(
            [("milvus-io/milvus", 1, checker.DESIGN_DOC_APPROVAL_LABEL)],
            client.removed_labels,
        )

    def test_stale_run_does_not_mutate_comments(self):
        invalid_design_doc = "docs/design-docs/design_docs/20260728-invalid.md"
        for refs in (("newer-head", "base"), ("head", "newer-base")):
            with self.subTest(refs=refs):
                client = FakeRunClient(
                    files=[
                        {
                            "filename": invalid_design_doc,
                            "status": "modified",
                            "sha": "invalid",
                        }
                    ],
                    documents={
                        "invalid": VALID_HEADER.replace(
                            "- Feature DRI: @feature-dri\n", ""
                        )
                    },
                    refs=refs,
                    comments=[],
                )
                self.assertEqual(0, self.run_with_client(client))
                self.assertEqual([], client.created)
                self.assertEqual([], client.updated)
                self.assertEqual([], client.deleted)
                self.assertEqual("neutral", client.completed_checks[-1][1])

    def test_same_sha_metadata_change_makes_run_stale(self):
        initial_state = checker.PullRequestState(
            head_sha="head",
            base_sha="base",
            head_repository="contributor/milvus",
            base_repository="milvus-io/milvus",
            author="contributor",
            title="docs: test",
            body="",
            labels=(),
        )
        final_state = checker.PullRequestState(
            head_sha="head",
            base_sha="base",
            head_repository="contributor/milvus",
            base_repository="milvus-io/milvus",
            author="contributor",
            title="feat: changed while running",
            body="",
            labels=("kind/feature",),
        )
        client = FakeRunClient(
            files=[],
            documents={},
            refs=("head", "base"),
            comments=[],
            initial_state=initial_state,
            final_state=final_state,
        )
        self.assertEqual(0, self.run_with_client(client))
        self.assertEqual([], client.created)
        self.assertEqual("neutral", client.completed_checks[-1][1])

    def test_metadata_change_after_comment_sync_keeps_check_non_success(self):
        initial_state = checker.PullRequestState(
            head_sha="head",
            base_sha="base",
            head_repository="contributor/milvus",
            base_repository="milvus-io/milvus",
            author="contributor",
            title="docs: test",
            body="",
            labels=(),
        )
        completion_state = checker.PullRequestState(
            head_sha="head",
            base_sha="base",
            head_repository="contributor/milvus",
            base_repository="milvus-io/milvus",
            author="contributor",
            title="feat: changed after comment sync",
            body="design doc: docs/design-docs/design_docs/missing.md",
            labels=("kind/feature",),
        )
        client = FakeRunClient(
            files=[],
            documents={},
            refs=("head", "base"),
            comments=[],
            initial_state=initial_state,
            final_state=initial_state,
            completion_state=completion_state,
        )
        self.assertEqual(0, self.run_with_client(client))
        self.assertEqual("neutral", client.completed_checks[-1][1])

    def test_current_valid_run_removes_only_owned_reminder(self):
        client = FakeRunClient(
            files=[
                {
                    "filename": "docs/design-docs/design_docs/20260728-valid.md",
                    "status": "modified",
                    "sha": "valid",
                }
            ],
            documents={"valid": VALID_HEADER},
            refs=("head", "base"),
            comments=[
                {
                    "id": 10,
                    "body": checker.COMMENT_PREFIX + "old",
                    "user": {"login": checker.BOT_LOGIN},
                },
                {
                    "id": 11,
                    "body": checker.COMMENT_PREFIX + "contributor",
                    "user": {"login": "contributor"},
                },
            ],
        )
        self.assertEqual(0, self.run_with_client(client))
        self.assertEqual([10], client.deleted)
        self.assertEqual([], client.created)
        self.assertEqual([], client.updated)
        self.assertEqual("success", client.completed_checks[-1][1])

    def test_current_invalid_run_creates_owned_reminder(self):
        client = FakeRunClient(
            files=[
                {
                    "filename": "docs/design-docs/design_docs/20260728-invalid.md",
                    "status": "modified",
                    "sha": "invalid",
                }
            ],
            documents={
                "invalid": VALID_HEADER.replace("- Feature DRI: @feature-dri\n", "")
            },
            refs=("head", "base"),
            comments=[],
        )
        self.assertEqual(0, self.run_with_client(client))
        self.assertEqual(1, len(client.created))
        self.assertTrue(client.created[0].startswith(checker.COMMENT_PREFIX))
        self.assertIn("Recommended review metadata", client.created[0])
        self.assertIn("advisory", client.created[0])
        self.assertNotIn("required header fields", client.created[0])
        self.assertEqual([], client.updated)
        self.assertEqual([], client.deleted)
        self.assertEqual("success", client.completed_checks[-1][1])
        self.assertIn("metadata reminders", client.completed_checks[-1][2])

    def test_feature_without_design_doc_fails_after_posting_reminder(self):
        client = FakeRunClient(
            files=[],
            documents={},
            refs=("head", "base"),
            comments=[],
        )
        self.assertEqual(
            1,
            self.run_with_client(
                client,
                title="feat: example",
            ),
        )
        self.assertEqual(1, len(client.created))
        self.assertIn("Feature design document requirement", client.created[0])
        self.assertEqual("failure", client.completed_checks[-1][1])

    def test_legacy_design_doc_path_gets_metadata_reminder_without_path_failure(self):
        client = FakeRunClient(
            files=[
                {
                    "filename": "docs/design-docs/design_docs/README.md",
                    "status": "added",
                    "sha": "legacy",
                }
            ],
            documents={
                "legacy": VALID_HEADER.replace("- Feature DRI: @feature-dri\n", "")
            },
            refs=("head", "base"),
            comments=[],
        )
        self.assertEqual(0, self.run_with_client(client))
        self.assertEqual(1, len(client.created))
        self.assertIn("Recommended review metadata", client.created[0])
        self.assertIn("advisory", client.created[0])
        self.assertEqual("success", client.completed_checks[-1][1])

    def test_live_pull_request_state_overrides_stale_event_payload(self):
        client = FakeRunClient(
            files=[],
            documents={},
            refs=("head", "base"),
            comments=[],
        )
        self.assertEqual(
            1,
            self.run_with_client(
                client,
                title="feat: current title",
                event_title="docs: stale event title",
            ),
        )
        self.assertEqual("failure", client.completed_checks[-1][1])

    def test_governance_drift_fails_and_posts_reminder(self):
        owners_aliases = (REPOSITORY_ROOT / "OWNERS_ALIASES").read_text(
            encoding="utf-8"
        )
        mergify = (
            (REPOSITORY_ROOT / ".github/mergify.yml")
            .read_text(encoding="utf-8")
            .replace(
                "      - label=approved\n",
                "",
                1,
            )
        )
        client = FakeRunClient(
            files=[{"filename": "OWNERS_ALIASES", "status": "modified"}],
            documents={},
            repository_documents={
                checker.OWNERS_ALIASES_PATH: owners_aliases,
                checker.MERGIFY_CONFIG_PATH: mergify,
            },
            refs=("head", "base"),
            comments=[],
        )
        self.assertEqual(1, self.run_with_client(client))
        self.assertEqual("failure", client.completed_checks[-1][1])
        self.assertIn("Repository governance enforcement", client.created[0])


class FakeCommentClient:
    def __init__(self, comments=None):
        self.created = []
        self.updated = []
        self.deleted = []
        self.comments = comments

    def list_issue_comments(self, repository, pull_number):
        if self.comments is not None:
            return self.comments
        return [
            {
                "id": 11,
                "body": checker.COMMENT_PREFIX + "duplicate",
                "user": {"login": checker.BOT_LOGIN},
            },
            {
                "id": 10,
                "body": checker.COMMENT_PREFIX + "old",
                "user": {"login": checker.BOT_LOGIN},
            },
            {
                "id": 12,
                "body": checker.COMMENT_MARKER,
                "user": {"login": "contributor"},
            },
        ]

    def create_comment(self, repository, pull_number, body):
        self.created.append(body)

    def update_comment(self, repository, comment_id, body):
        self.updated.append((comment_id, body))

    def delete_comment(self, repository, comment_id):
        self.deleted.append(comment_id)


class FakeBlobClient:
    def __init__(self, documents):
        self.documents = documents

    def get_blob(self, repository, sha):
        content = self.documents[sha].encode("utf-8")
        return {
            "size": len(content),
            "encoding": "base64",
            "content": base64.b64encode(content).decode("ascii"),
        }


class FakeRepositoryFileClient:
    def __init__(self, documents):
        self.documents = documents
        self.requests = []

    def get_repository_file(self, repository, path, ref):
        self.requests.append((repository, path, ref))
        return self.documents[path]


class FakeRunClient(FakeCommentClient):
    def __init__(
        self,
        files,
        documents,
        refs,
        comments,
        existing_paths=None,
        repository_documents=None,
        initial_state=None,
        final_state=None,
        completion_state=None,
        approval_comments=None,
        prow_approvers=PROW_APPROVERS_UNSET,
        label_error=False,
    ):
        normalized_comments = [
            {
                "created_at": "2026-08-08T00:00:00Z",
                **comment,
            }
            for comment in comments
        ]
        super().__init__(comments=normalized_comments)
        self.files = files
        self.documents = documents
        self.refs = refs
        self.existing_paths = set(existing_paths or [])
        self.repository_documents = {
            checker.OWNERS_ALIASES_PATH: (
                REPOSITORY_ROOT / checker.OWNERS_ALIASES_PATH
            ).read_text(encoding="utf-8"),
            **(repository_documents or {}),
        }
        self.initial_state = initial_state
        self.final_state = final_state
        self.completion_state = completion_state
        self.state_calls = 0
        self.created_checks = []
        self.completed_checks = []
        self.approval_comments = approval_comments
        if self.approval_comments is None:
            self.approval_comments = [
                {
                    "id": 900,
                    "body": "/approve",
                    "created_at": "2026-08-08T00:00:01Z",
                    "user": {"login": "congqixia"},
                },
                {
                    "id": 901,
                    "body": "/approve no-issue",
                    "created_at": "2026-08-08T00:00:02Z",
                    "user": {"login": "czs007"},
                },
            ]
        if prow_approvers is PROW_APPROVERS_UNSET:
            self.prow_approvers = infer_prow_approvers(self.approval_comments)
        else:
            self.prow_approvers = tuple(prow_approvers)
        self.ensured_labels = []
        self.added_labels = []
        self.removed_labels = []
        self.label_error = label_error

    def configure_default_states(self, head_sha, base_sha, title, body, labels, author):
        if self.initial_state is None:
            self.initial_state = checker.PullRequestState(
                head_sha=head_sha,
                base_sha=base_sha,
                head_repository="contributor/milvus",
                base_repository="milvus-io/milvus",
                author=author,
                title=title,
                body=body,
                labels=tuple(sorted(labels)),
            )
        if self.final_state is None:
            self.final_state = checker.PullRequestState(
                head_sha=self.refs[0],
                base_sha=self.refs[1],
                head_repository=self.initial_state.head_repository,
                base_repository=self.initial_state.base_repository,
                author=self.initial_state.author,
                title=self.initial_state.title,
                body=self.initial_state.body,
                labels=self.initial_state.labels,
            )

    def create_policy_check(self, repository, head_sha, pull_number):
        self.created_checks.append((repository, head_sha, pull_number))
        return 99

    def complete_policy_check(
        self, repository, check_run_id, conclusion, title, summary
    ):
        self.completed_checks.append(
            (repository, conclusion, title, summary, check_run_id)
        )

    def list_pull_request_files(self, repository, pull_number):
        return self.files

    def list_issue_comments(self, repository, pull_number):
        return [
            *self.comments,
            *self.approval_comments,
            prow_notification(*self.prow_approvers),
        ]

    def get_blob(self, repository, sha):
        content = self.documents[sha].encode("utf-8")
        return {
            "size": len(content),
            "encoding": "base64",
            "content": base64.b64encode(content).decode("ascii"),
        }

    def get_pull_request_state(self, repository, pull_number):
        self.state_calls += 1
        if self.state_calls == 1:
            return self.initial_state
        if self.state_calls == 2 or self.completion_state is None:
            return self.final_state
        return self.completion_state

    def file_exists(self, repository, path, ref):
        return path in self.existing_paths

    def get_repository_file(self, repository, path, ref):
        return self.repository_documents[path]

    def ensure_repository_label(self, repository, name, color, description):
        self.ensured_labels.append((repository, name, color, description))

    def add_pull_request_label(self, repository, pull_number, label):
        if self.label_error:
            raise RuntimeError("label write failed")
        self.added_labels.append((repository, pull_number, label))

    def remove_pull_request_label(self, repository, pull_number, label):
        self.removed_labels.append((repository, pull_number, label))


if __name__ == "__main__":
    unittest.main()

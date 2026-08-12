import base64
import importlib.util
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


def prow_notification(*approvers, comment_id=700, titles=None, qualified=None):
    titles = ("Approved",) * len(approvers) if titles is None else tuple(titles)
    if len(titles) != len(approvers):
        raise ValueError("Each rendered approver needs one title")
    rendered = ", ".join(
        f'*<a href="https://github.com/milvus-io/milvus/pull/1#" '
        f'title="{title}">{login}</a>*'
        for login, title in zip(approvers, titles)
    )
    qualified = approvers if qualified is None else tuple(qualified)
    owners_rows = "\n".join(
        "- ~~[docs/OWNERS](https://github.com/milvus-io/milvus/blob/master/"
        f"docs/OWNERS)~~ [{login}]"
        for login in qualified
    )
    return {
        "id": comment_id,
        "user": {
            "login": checker.approval_policy.PROW_BOT_LOGIN,
            "id": checker.approval_policy.PROW_BOT_USER_ID,
        },
        "body": (
            f"{checker.approval_policy.PROW_APPROVAL_NOTIFICATION_PREFIX} "
            "This PR is **APPROVED**\n\n"
            f"This pull-request has been approved by: {rendered}\n\n"
            "The full list of commands accepted by this bot follows.\n\n"
            "<details>\n"
            "Needs approval from an approver in each of these files:\n\n"
            f"{owners_rows}\n"
            "</details>"
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


class PolicyConfigurationTest(unittest.TestCase):
    def test_mergify_uses_prow_and_dedicated_design_doc_labels(self):
        mergify = (REPOSITORY_ROOT / ".github/mergify.yml").read_text(encoding="utf-8")

        for rule_name in (
            "Review / Prow approval",
            "Review / formal Design Doc",
            "Docs / formal Design Doc policy",
            "Docs / feature Design Doc policy",
        ):
            self.assertEqual(1, mergify.count(f"  - name: {rule_name}\n"))

    def test_formal_design_doc_classifier_matches_repository_tree(self):
        mergify = (REPOSITORY_ROOT / ".github/mergify.yml").read_text(encoding="utf-8")
        anchor_match = re.search(
            r"^  - &design_doc_area 'files~=(.+)'$",
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

    def test_approve_comments_trigger_the_trusted_policy_workflow_directly(self):
        trusted_workflow = (
            REPOSITORY_ROOT / ".github/workflows/design-doc-policy.yml"
        ).read_text(encoding="utf-8")

        self.assertIn("  issue_comment:\n", trusted_workflow)
        self.assertIn("    types: [created, edited, deleted]\n", trusted_workflow)
        self.assertIn("    branches: [master]\n", trusted_workflow)
        self.assertIn(
            "supportedBase && (featurePullRequest || formalDesignDocChanged);",
            trusted_workflow,
        )
        self.assertIn(
            "steps.start-policy-check.outputs.applicable == 'true'",
            trusted_workflow,
        )

    def test_trusted_workflow_bounds_failures_and_uses_least_privilege(self):
        trusted_workflow = (
            REPOSITORY_ROOT / ".github/workflows/design-doc-policy.yml"
        ).read_text(encoding="utf-8")

        self.assertIn("    timeout-minutes: 20\n", trusted_workflow)
        self.assertGreaterEqual(trusted_workflow.count("        timeout-minutes:"), 4)
        self.assertIn("      pull-requests: read\n", trusted_workflow)
        self.assertNotIn("      pull-requests: write\n", trusted_workflow)
        self.assertIn('core.setOutput("head_sha", headSha);', trusted_workflow)
        self.assertIn('core.setOutput("external_id", externalId);', trusted_workflow)
        self.assertIn(
            "${{ always() &&\n"
            "          steps.validate-policy.outcome != 'success' &&\n"
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
        self.assertLess(
            trusted_workflow.index('core.setOutput("head_sha", headSha);'),
            trusted_workflow.index("github.rest.pulls.listFiles"),
        )
        self.assertIn(
            'check.conclusion === "success"',
            trusted_workflow,
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

    def test_evaluation_uses_the_authenticated_prow_approver_snapshot(self):
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
            "milvus-io/milvus",
            1,
            state,
            [prow_notification("Bob")],
        )
        self.assertEqual(("Bob",), approval.approvers)

    def test_design_doc_evaluation_always_requires_two_approvers(self):
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
            "milvus-io/milvus",
            1,
            state,
            [prow_notification("Bob")],
        )
        self.assertFalse(approval.satisfied)

    def test_bot_author_does_not_poison_two_design_doc_approvals(self):
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

    def test_blob_read_uses_the_bounded_advisory_timeout(self):
        client = checker.GitHubClient("token", "https://api.github.test")
        client.request = mock.Mock(return_value={})

        client.get_blob("milvus-io/milvus", "blob-sha")

        client.request.assert_called_once_with(
            "GET",
            "/repos/milvus-io/milvus/git/blobs/blob-sha",
            timeout=checker.METADATA_BLOB_TIMEOUT_SECONDS,
        )


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
        inspection = checker.validate_changed_design_docs(
            client, "milvus-io/milvus", files
        )
        self.assertEqual(
            [
                "docs/design-docs/design_docs/20260728-bad-date.md",
                "docs/design-docs/design_docs/20260728-missing.md",
            ],
            sorted(inspection.issues),
        )
        self.assertTrue(inspection.complete)
        self.assertEqual((), inspection.warnings)

    def test_missing_changed_blob_is_an_incomplete_advisory_inspection(self):
        inspection = checker.validate_changed_design_docs(
            FakeBlobClient({}),
            "milvus-io/milvus",
            [
                {
                    "filename": "docs/design-docs/design_docs/example.md",
                    "status": "modified",
                }
            ],
        )

        self.assertEqual({}, inspection.issues)
        self.assertFalse(inspection.complete)
        self.assertIn("could not be located", inspection.warnings[0])

    def test_metadata_inspection_has_a_bounded_file_budget(self):
        content = VALID_HEADER.encode("utf-8")
        client = mock.Mock()
        client.get_blob.return_value = {
            "size": len(content),
            "encoding": "base64",
            "content": base64.b64encode(content).decode("ascii"),
        }
        changed_count = checker.MAX_METADATA_INSPECTION_FILES + 3
        files = [
            {
                "filename": (
                    "docs/design-docs/design_docs/" f"{index:04d}-bounded-inspection.md"
                ),
                "status": "modified",
                "sha": f"blob-{index}",
            }
            for index in range(changed_count)
        ]

        inspection = checker.validate_changed_design_docs(
            client, "milvus-io/milvus", files
        )

        self.assertEqual(
            checker.MAX_METADATA_INSPECTION_FILES,
            client.get_blob.call_count,
        )
        self.assertFalse(inspection.complete)
        self.assertIn(
            f"{changed_count - checker.MAX_METADATA_INSPECTION_FILES} additional",
            inspection.warnings[-1],
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
    def test_truncates_oversized_summary_before_publishing_check(self):
        client = checker.GitHubClient("token", "https://api.github.test")
        payloads = []

        def request(method, path, payload=None, allow_not_found=False):
            self.assertEqual("PATCH", method)
            payloads.append(payload)
            return None

        client.request = request
        hard_result = "Hard policy result must be preserved.\n"
        oversized_summary = hard_result + "&" * (checker.MAX_CHECK_SUMMARY_CHARS * 2)

        client.complete_policy_check(
            "milvus-io/milvus",
            99,
            "success",
            "passed with advisory warnings",
            oversized_summary,
        )

        published_summary = payloads[0]["output"]["summary"]
        self.assertLessEqual(len(published_summary), checker.MAX_CHECK_SUMMARY_CHARS)
        self.assertTrue(published_summary.startswith(hard_result))
        self.assertTrue(
            published_summary.endswith(checker.CHECK_SUMMARY_TRUNCATION_NOTICE)
        )

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
            self.run_with_client(
                client, labels=[checker.approval_policy.APPROVED_LABEL]
            ),
        )
        self.assertEqual("failure", client.completed_checks[-1][1])
        self.assertIn("(1/2)", client.completed_checks[-1][3])
        self.assertEqual([], client.added_labels)

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

    def test_two_non_author_non_owners_in_prow_snapshot_do_not_count(self):
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
                self.approval_comment("path-approver-one", "/approve", 1),
                self.approval_comment("path-approver-two", "/approve", 2),
            ],
            prow_qualified=(),
        )
        self.assertEqual(1, self.run_with_client(client))
        self.assertEqual([], client.added_labels)
        self.assertIn("(0/2)", client.completed_checks[-1][3])

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

    def test_metadata_blob_failure_warns_without_changing_hard_pass(self):
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
        client.get_blob = mock.Mock(
            side_effect=RuntimeError("metadata blob read failed")
        )

        self.assertEqual(0, self.run_with_client(client))
        self.assertEqual("success", client.completed_checks[-1][1])
        self.assertIn("metadata blob read failed", client.completed_checks[-1][3])
        self.assertEqual(
            [("milvus-io/milvus", 1, checker.DESIGN_DOC_APPROVAL_LABEL)],
            client.added_labels,
        )

    def test_metadata_blob_failure_does_not_hide_hard_approval_failure(self):
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
        client.get_blob = mock.Mock(
            side_effect=RuntimeError("metadata blob read failed")
        )

        self.assertEqual(1, self.run_with_client(client))
        self.assertEqual("failure", client.completed_checks[-1][1])
        self.assertIn("(1/2)", client.completed_checks[-1][3])
        self.assertIn("metadata blob read failed", client.completed_checks[-1][3])
        self.assertEqual([], client.added_labels)

    def test_metadata_parser_failure_is_advisory_and_preserves_reminder(self):
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
            comments=[
                {
                    "id": 10,
                    "body": checker.COMMENT_PREFIX + "old",
                    "user": {"login": checker.BOT_LOGIN},
                }
            ],
        )
        client.delete_comment = mock.Mock()

        with mock.patch.object(
            checker,
            "validate_header",
            side_effect=RuntimeError("metadata parser failed"),
        ):
            self.assertEqual(0, self.run_with_client(client))

        self.assertEqual("success", client.completed_checks[-1][1])
        self.assertIn("metadata parser failed", client.completed_checks[-1][3])
        client.delete_comment.assert_not_called()

    def test_metadata_decode_failure_is_advisory_and_preserves_reminder(self):
        client = FakeRunClient(
            files=[
                {
                    "filename": "docs/design-docs/design_docs/example.md",
                    "status": "modified",
                    "sha": "invalid",
                }
            ],
            documents={},
            refs=("head", "base"),
            comments=[
                {
                    "id": 10,
                    "body": checker.COMMENT_PREFIX + "old",
                    "user": {"login": checker.BOT_LOGIN},
                }
            ],
        )
        client.get_blob = mock.Mock(
            return_value={"size": 3, "encoding": "base64", "content": "%%%"}
        )
        client.update_comment = mock.Mock()

        self.assertEqual(0, self.run_with_client(client))
        self.assertEqual("success", client.completed_checks[-1][1])
        self.assertIn("not valid base64", client.completed_checks[-1][3])
        self.assertIn("inspection incomplete", client.completed_checks[-1][3])
        client.update_comment.assert_not_called()

    def test_comment_mutation_failures_are_advisory(self):
        owned_comment = {
            "id": 10,
            "body": checker.COMMENT_PREFIX + "old",
            "user": {"login": checker.BOT_LOGIN},
        }
        scenarios = (
            ("create", "# MEP: Missing metadata\n", [], "create_comment"),
            (
                "update",
                "# MEP: Missing metadata\n",
                [owned_comment],
                "update_comment",
            ),
            ("delete", VALID_HEADER, [owned_comment], "delete_comment"),
        )
        for operation, document, comments, method_name in scenarios:
            with self.subTest(operation=operation):
                client = FakeRunClient(
                    files=[
                        {
                            "filename": "docs/design-docs/design_docs/example.md",
                            "status": "modified",
                            "sha": "document",
                        }
                    ],
                    documents={"document": document},
                    refs=("head", "base"),
                    comments=comments,
                )
                error = f"comment {operation} failed"
                setattr(
                    client,
                    method_name,
                    mock.Mock(side_effect=RuntimeError(error)),
                )

                self.assertEqual(0, self.run_with_client(client))
                self.assertEqual("success", client.completed_checks[-1][1])
                self.assertIn(error, client.completed_checks[-1][3])

    def test_comment_failure_does_not_hide_hard_policy_failure(self):
        client = FakeRunClient(
            files=[
                {
                    "filename": "docs/design-docs/design_docs/example.md",
                    "status": "modified",
                    "sha": "invalid",
                }
            ],
            documents={"invalid": "# MEP: Missing metadata\n"},
            refs=("head", "base"),
            comments=[],
            approval_comments=[self.approval_comment("congqixia", "/approve", 1)],
        )
        client.create_comment = mock.Mock(
            side_effect=RuntimeError("comment create failed")
        )

        self.assertEqual(1, self.run_with_client(client))
        self.assertEqual("failure", client.completed_checks[-1][1])
        self.assertIn("(1/2)", client.completed_checks[-1][3])
        self.assertIn("comment create failed", client.completed_checks[-1][3])

    def test_incomplete_metadata_inspection_preserves_existing_reminder(self):
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
            comments=[
                {
                    "id": 10,
                    "body": checker.COMMENT_PREFIX + "old",
                    "user": {"login": checker.BOT_LOGIN},
                }
            ],
        )
        client.get_blob = mock.Mock(
            side_effect=RuntimeError("metadata blob read failed")
        )
        client.delete_comment = mock.Mock()

        self.assertEqual(0, self.run_with_client(client))
        client.delete_comment.assert_not_called()
        self.assertIn("left unchanged", client.completed_checks[-1][3])

    def test_comment_failure_does_not_skip_final_state_race_check(self):
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
            title="feat: changed after comment failure",
            body="design doc: docs/design-docs/design_docs/example.md",
            labels=("kind/feature",),
        )
        client = FakeRunClient(
            files=[
                {
                    "filename": "docs/design-docs/design_docs/example.md",
                    "status": "modified",
                    "sha": "invalid",
                }
            ],
            documents={"invalid": "# MEP: Missing metadata\n"},
            refs=("head", "base"),
            comments=[],
            initial_state=initial_state,
            final_state=initial_state,
            completion_state=completion_state,
        )
        client.create_comment = mock.Mock(
            side_effect=RuntimeError("comment create failed")
        )

        with mock.patch.object(checker, "report_advisory_warning") as report_warning:
            self.assertEqual(0, self.run_with_client(client))

        report_warning.assert_called_once()
        self.assertEqual("neutral", client.completed_checks[-1][1])
        self.assertIn("comment create failed", client.completed_checks[-1][3])

    def test_comment_failure_does_not_skip_final_approval_race_check(self):
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
                    "filename": "docs/design-docs/design_docs/example.md",
                    "status": "modified",
                    "sha": "invalid",
                }
            ],
            documents={"invalid": "# MEP: Missing metadata\n"},
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
        client.list_issue_comments = lambda repository, pull_number: next(snapshots)
        client.create_comment = mock.Mock(
            side_effect=RuntimeError("comment create failed")
        )

        self.assertEqual(0, self.run_with_client(client))
        self.assertEqual("neutral", client.completed_checks[-1][1])
        self.assertIn("comment create failed", client.completed_checks[-1][3])
        self.assertEqual(
            [("milvus-io/milvus", 1, checker.DESIGN_DOC_APPROVAL_LABEL)],
            client.added_labels,
        )
        self.assertEqual(
            [("milvus-io/milvus", 1, checker.DESIGN_DOC_APPROVAL_LABEL)],
            client.removed_labels,
        )

    def test_final_approval_comment_read_failure_remains_fail_closed(self):
        initial_comments = [
            self.approval_comment("congqixia", "/approve", 1),
            self.approval_comment("czs007", "/approve", 2),
        ]
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
            approval_comments=initial_comments,
        )
        snapshots = iter(
            (
                [
                    *initial_comments,
                    prow_notification("congqixia", "czs007", comment_id=700),
                ],
                RuntimeError("final approval comment read failed"),
            )
        )

        def list_issue_comments(repository, pull_number):
            snapshot = next(snapshots)
            if isinstance(snapshot, Exception):
                raise snapshot
            return snapshot

        client.list_issue_comments = list_issue_comments

        with self.assertRaisesRegex(RuntimeError, "final approval comment read failed"):
            self.run_with_client(client)
        self.assertEqual("failure", client.completed_checks[-1][1])
        self.assertEqual(
            "Design Doc policy could not be evaluated",
            client.completed_checks[-1][2],
        )

    def test_hard_failure_summary_retains_prior_advisory_warning(self):
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
        client.get_blob = mock.Mock(
            side_effect=RuntimeError("metadata blob read failed")
        )

        with self.assertRaisesRegex(RuntimeError, "label write failed"):
            self.run_with_client(client)
        self.assertEqual("failure", client.completed_checks[-1][1])
        self.assertIn("label write failed", client.completed_checks[-1][3])
        self.assertIn("metadata blob read failed", client.completed_checks[-1][3])

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

    def test_prow_snapshot_is_not_narrowed_to_a_global_alias(self):
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
        self.assertEqual(0, self.run_with_client(client))
        self.assertIn("(2/2)", client.completed_checks[-1][3])

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


class FakeRunClient(FakeCommentClient):
    def __init__(
        self,
        files,
        documents,
        refs,
        comments,
        existing_paths=None,
        initial_state=None,
        final_state=None,
        completion_state=None,
        approval_comments=None,
        prow_approvers=PROW_APPROVERS_UNSET,
        prow_qualified=PROW_APPROVERS_UNSET,
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
        if prow_qualified is PROW_APPROVERS_UNSET:
            self.prow_qualified = self.prow_approvers
        else:
            self.prow_qualified = tuple(prow_qualified)
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
            prow_notification(
                *self.prow_approvers,
                qualified=self.prow_qualified,
            ),
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

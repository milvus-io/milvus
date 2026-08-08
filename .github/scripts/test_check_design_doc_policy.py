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


class GovernanceConfigTest(unittest.TestCase):
    def test_mergify_approver_matrix_matches_owners_alias(self):
        owners_aliases = (REPOSITORY_ROOT / "OWNERS_ALIASES").read_text(
            encoding="utf-8"
        )
        maintainers: list[str] = []
        in_maintainers = False
        for line in owners_aliases.splitlines():
            if line == "  maintainers:":
                in_maintainers = True
                continue
            if in_maintainers and re.match(r"^  [a-zA-Z0-9_-]+:$", line):
                break
            match = re.match(r"^    - ([A-Za-z0-9-]+)$", line)
            if in_maintainers and match:
                maintainers.append(match.group(1))

        mergify = (REPOSITORY_ROOT / ".github/mergify.yml").read_text(
            encoding="utf-8"
        )
        self.assertEqual([], checker.validate_approver_governance(owners_aliases, mergify))
        anchors = dict(
            re.findall(
                r"^  approved_by_[a-z0-9_]+: &(approved_by_[a-z0-9_]+) "
                r"'approved-reviews-by = ([A-Za-z0-9-]+)'$",
                mergify,
                re.MULTILINE,
            )
        )
        self.assertEqual(set(maintainers), set(anchors.values()))

        general_rule = mergify.split(
            "  - name: Review / non-author Approver", 1
        )[1].split("  - name: Review / formal Design Doc", 1)[0]
        general_anchors = set(
            re.findall(r"\*(approved_by_[a-z0-9_]+)$", general_rule, re.MULTILINE)
        )
        self.assertEqual(set(anchors), general_anchors)
        self.assertIn("'-approved-reviews-by = {{ author }}'", general_rule)

        design_rule = mergify.split(
            "  - name: Review / formal Design Doc", 1
        )[1].split("  - name: Docs / formal Design Doc policy", 1)[0]
        self.assertIn("*design_doc_area", design_rule)
        self.assertIn("'-approved-reviews-by = {{ author }}'", design_rule)

        policy_rule = mergify.split(
            "  - name: Docs / formal Design Doc policy", 1
        )[1].split("  - name: Docs / feature Design Doc policy", 1)[0]
        self.assertIn("*design_doc_area", policy_rule)
        pair_anchor_names: list[tuple[str, str]] = []
        current_pair: list[str] = []
        for line in design_rule.splitlines():
            if line.strip() == "- and:":
                current_pair = []
                continue
            match = re.match(r"^              - \*(approved_by_[a-z0-9_]+)$", line)
            if match:
                current_pair.append(match.group(1))
                if len(current_pair) == 2:
                    pair_anchor_names.append(tuple(current_pair))

        actual_pairs = {
            frozenset((anchors[first], anchors[second]))
            for first, second in pair_anchor_names
        }
        expected_pairs = {
            frozenset(pair) for pair in itertools.combinations(maintainers, 2)
        }
        self.assertEqual(expected_pairs, actual_pairs)

        governance_rule = mergify.split(
            "  - name: Review / governance enforcement", 1
        )[1].split("  - name: Docs / repository governance policy", 1)[0]
        self.assertIn("*governance_area", governance_rule)
        self.assertIn("*TWO_APPROVER_SUCCESS_CONDITIONS", governance_rule)

        governance_policy_rule = mergify.split(
            "  - name: Docs / repository governance policy", 1
        )[1].split("  - name: Docs / formal Design Doc policy", 1)[0]
        self.assertIn("*governance_area", governance_policy_rule)
        self.assertIn("@github-actions/Design Doc Policy", governance_policy_rule)

    def test_formal_design_doc_classifier_matches_repository_tree(self):
        mergify = (REPOSITORY_ROOT / ".github/mergify.yml").read_text(
            encoding="utf-8"
        )
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

    def test_feature_policy_does_not_trust_automated_title_text(self):
        mergify = (REPOSITORY_ROOT / ".github/mergify.yml").read_text(
            encoding="utf-8"
        )
        protection = mergify.split(
            "  - name: Docs / feature Design Doc policy", 1
        )[1].split("# PULL REQUEST RULES", 1)[0]
        label_rule = mergify.split(
            "  - name: Blocking PR if feat PR missing design doc", 1
        )[1].split("  - name: Dismiss block label if design doc is provided", 1)[0]
        automated_rule = mergify.split(
            "  - name: Dismiss block label if automated create PR", 1
        )[1].split("  - name: Blocking PR if feat PR missing design doc", 1)[0]

        self.assertNotIn("automated", protection.casefold())
        self.assertNotIn("automated", label_rule.casefold())
        self.assertNotIn("do-not-merge/missing-design-doc", automated_rule)


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
                "filename": "archive/old-policy.py",
                "previous_filename": ".github/scripts/check_design_doc_policy.py",
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

    def test_detects_approver_anchor_drift(self):
        drifted = self.mergify.replace(
            "  approved_by_weiliu1031: &approved_by_weiliu1031 "
            "'approved-reviews-by = weiliu1031'\n",
            "",
        )
        issues = checker.validate_approver_governance(self.owners_aliases, drifted)
        self.assertTrue(any("OWNERS_ALIASES" in issue for issue in issues))

    def test_detects_incomplete_two_approver_matrix(self):
        drifted = self.mergify.replace(
            "          - and:\n"
            "              - *approved_by_chyezh\n"
            "              - *approved_by_weiliu1031\n",
            "",
        )
        issues = checker.validate_approver_governance(self.owners_aliases, drifted)
        self.assertTrue(any("complete set" in issue for issue in issues))

    def test_rejects_commented_out_design_doc_rule_guards(self):
        cases = (
            (
                "      - *design_doc_area",
                "two-Approver rule does not cover the Design Doc area",
            ),
            (
                "      - '-approved-reviews-by = {{ author }}'",
                "two-Approver rule does not exclude the PR author",
            ),
        )
        for line, expected_issue in cases:
            with self.subTest(line=line):
                drifted = self.comment_out_rule_line(
                    "Review / formal Design Doc", line
                )
                issues = checker.validate_approver_governance(
                    self.owners_aliases, drifted
                )
                self.assertTrue(any(expected_issue in issue for issue in issues))

    def test_rejects_commented_out_governance_rule_guards(self):
        cases = (
            (
                "      - *governance_area",
                "Governance enforcement changes do not require two Approvers",
            ),
            (
                "    success_conditions: *TWO_APPROVER_SUCCESS_CONDITIONS",
                "Governance enforcement changes do not reuse the two-Approver gate",
            ),
        )
        for line, expected_issue in cases:
            with self.subTest(line=line):
                drifted = self.comment_out_rule_line(
                    "Review / governance enforcement", line
                )
                issues = checker.validate_approver_governance(
                    self.owners_aliases, drifted
                )
                self.assertTrue(any(expected_issue in issue for issue in issues))

    def test_rejects_commented_out_general_review_guards(self):
        cases = (
            (
                "          - *approved_by_congqixia",
                "general review rule does not include every Approver",
            ),
            (
                "      - '-approved-reviews-by = {{ author }}'",
                "general review rule does not exclude the PR author",
            ),
        )
        for line, expected_issue in cases:
            with self.subTest(line=line):
                drifted = self.comment_out_rule_line(
                    "Review / non-author Approver", line
                )
                issues = checker.validate_approver_governance(
                    self.owners_aliases, drifted
                )
                self.assertTrue(any(expected_issue in issue for issue in issues))

    def test_merge_protections_section_cannot_be_disabled(self):
        drifted = self.mergify.replace(
            "\nmerge_protections:\n",
            "\ndisabled_merge_protections:\n",
            1,
        )
        issues = checker.validate_approver_governance(self.owners_aliases, drifted)
        self.assertTrue(any("merge_protections" in issue for issue in issues))

    def test_general_review_rule_cannot_be_disabled(self):
        for replacement in ("    if: false", "    if: 'base = __never__'"):
            with self.subTest(replacement=replacement):
                drifted = self.replace_rule_fragment(
                    "Review / non-author Approver",
                    "    if: true",
                    replacement,
                )
                issues = checker.validate_approver_governance(
                    self.owners_aliases, drifted
                )
                self.assertTrue(any("must always apply" in issue for issue in issues))

    def test_governance_review_rule_cannot_be_disabled(self):
        drifted = self.replace_rule_fragment(
            "Review / governance enforcement",
            "    if:\n      - *governance_area",
            "    if: false",
        )
        issues = checker.validate_approver_governance(self.owners_aliases, drifted)
        self.assertTrue(
            any("Governance enforcement changes do not require two Approvers" in issue for issue in issues)
        )

    def test_feature_policy_rule_cannot_be_disabled(self):
        feature_if = (
            "    if:\n"
            "      - or:\n"
            "          - 'title~=^feat:'\n"
            "          - label=kind/feature"
        )
        drifted = self.replace_rule_fragment(
            "Docs / feature Design Doc policy",
            feature_if,
            "    if: false",
        )
        issues = checker.validate_approver_governance(self.owners_aliases, drifted)
        self.assertTrue(any("Feature policy trigger" in issue for issue in issues))

    def test_rule_triggers_reject_extra_never_matching_condition(self):
        cases = (
            (
                "Review / formal Design Doc",
                "    if:\n      - *design_doc_area",
                "two-Approver rule does not cover the Design Doc area",
            ),
            (
                "Review / governance enforcement",
                "    if:\n      - *governance_area",
                "Governance enforcement changes do not require two Approvers",
            ),
            (
                "Docs / repository governance policy",
                "    if:\n      - *governance_area",
                "Governance changes do not require the trusted policy check",
            ),
            (
                "Docs / formal Design Doc policy",
                "    if:\n      - *design_doc_area",
                "Design Doc changes do not require the trusted policy check",
            ),
            (
                "Docs / feature Design Doc policy",
                "    if:\n"
                "      - or:\n"
                "          - 'title~=^feat:'\n"
                "          - label=kind/feature",
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

    def test_general_success_conditions_cannot_be_replaced_with_true(self):
        rule_name = "Review / non-author Approver"
        marker = f"  - name: {rule_name}"
        rule_start = self.mergify.index(marker)
        rule_end = self.mergify.index("\n  - name:", rule_start + len(marker))
        conditions_start = self.mergify.index(
            "    success_conditions:", rule_start, rule_end
        )
        original_block = self.mergify[conditions_start:rule_end]
        original_lines = original_block.splitlines()
        commented_lines = ["    success_conditions: true"]
        for line in original_lines[1:]:
            if not line.strip():
                commented_lines.append(line)
                continue
            indentation = line[: len(line) - len(line.lstrip())]
            commented_lines.append(f"{indentation}# {line.lstrip()}")
        replacement = "\n".join(commented_lines)
        drifted = (
            self.mergify[:conditions_start]
            + replacement
            + self.mergify[rule_end:]
        )
        issues = checker.validate_approver_governance(self.owners_aliases, drifted)
        self.assertTrue(
            any("general review rule does not include every Approver" in issue for issue in issues)
        )

    def test_success_conditions_reject_extra_never_matching_condition(self):
        author_condition = "      - '-approved-reviews-by = {{ author }}'"
        stale_guard = (
            "      - '-check-stale = @github-actions/Design Doc Policy'"
        )
        cases = (
            (
                "Review / non-author Approver",
                author_condition,
                f"      - base = __never__\n{author_condition}",
                "general review rule does not include every Approver",
            ),
            (
                "Review / formal Design Doc",
                author_condition,
                f"      - base = __never__\n{author_condition}",
                "two-Approver matrix is not the complete set",
            ),
            (
                "Review / governance enforcement",
                "    success_conditions: *TWO_APPROVER_SUCCESS_CONDITIONS",
                "    success_conditions:\n"
                "      - *TWO_APPROVER_SUCCESS_CONDITIONS\n"
                "      - base = __never__",
                "Governance enforcement changes do not reuse the two-Approver gate",
            ),
            (
                "Docs / repository governance policy",
                stale_guard,
                f"{stale_guard}\n      - base = __never__",
                "Governance changes do not require exact Design Doc Policy success",
            ),
            (
                "Docs / formal Design Doc policy",
                stale_guard,
                f"{stale_guard}\n      - base = __never__",
                "Design Doc changes do not require exact Design Doc Policy success",
            ),
            (
                "Docs / feature Design Doc policy",
                stale_guard,
                f"{stale_guard}\n      - base = __never__",
                "native Design Doc requirement",
            ),
        )
        for rule_name, condition, replacement, expected_issue in cases:
            with self.subTest(rule=rule_name):
                drifted = self.replace_rule_fragment(
                    rule_name, condition, replacement
                )
                issues = checker.validate_approver_governance(
                    self.owners_aliases, drifted
                )
                self.assertTrue(any(expected_issue in issue for issue in issues))

    def test_detects_design_doc_area_path_drift(self):
        drifted = self.mergify.replace(
            "  design_doc_area: &design_doc_area "
            "'files~=^docs/design-docs/design_docs/'",
            "  design_doc_area: &design_doc_area "
            "'files~=^docs/design-docs/drafts/'",
        )
        issues = checker.validate_approver_governance(self.owners_aliases, drifted)
        self.assertTrue(any("Design Doc area matcher" in issue for issue in issues))

    def test_detects_governance_area_path_drift(self):
        drifted = self.mergify.replace(
            "|test_check_design_doc_policy)\\.py)$'",
            ")\\.py)$'",
        )
        issues = checker.validate_approver_governance(self.owners_aliases, drifted)
        self.assertTrue(any("governance area matcher" in issue for issue in issues))

    def test_rejects_policy_check_name_with_trailing_text(self):
        condition = "'check-success = @github-actions/Design Doc Policy'"
        spoofed_condition = (
            "'check-success = @github-actions/Design Doc Policy Disabled'"
        )
        matches = list(re.finditer(re.escape(condition), self.mergify))
        self.assertEqual(3, len(matches))

        rule_names = (
            "repository governance",
            "formal Design Doc",
            "feature Design Doc",
        )
        for rule_name, match in zip(rule_names, matches):
            with self.subTest(rule=rule_name):
                drifted = (
                    self.mergify[: match.start()]
                    + spoofed_condition
                    + self.mergify[match.end() :]
                )
                issues = checker.validate_approver_governance(
                    self.owners_aliases, drifted
                )
                self.assertTrue(
                    any("Design Doc Policy success" in issue for issue in issues)
                )

    def test_policy_rules_require_each_trusted_check_state_guard(self):
        rule_names = (
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
                condition = (
                    f"      - '-check-{state} = "
                    "@github-actions/Design Doc Policy'"
                )
                drifted = self.replace_rule_fragment(rule_name, condition, "")
                issues = checker.validate_approver_governance(
                    self.owners_aliases, drifted
                )
                self.assertTrue(
                    any("Design Doc Policy success" in issue for issue in issues)
                )

    def test_feature_policy_requires_each_native_design_doc_condition(self):
        rule_start = self.mergify.index(
            "  - name: Docs / feature Design Doc policy"
        )
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
                "previous_filename": (
                    "docs/design-docs/design_docs/20260728-old.md"
                ),
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

    def test_automated_title_text_does_not_bypass_feature_requirement(self):
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
        self.assertIn("must add or update", issue)
        client.file_exists.assert_not_called()

    def test_changed_formal_doc_satisfies_feature_requirement(self):
        client = mock.Mock()
        files = [
            {
                "filename": "docs/design-docs/design_docs/Legacy Topic/Old Design.md",
                "status": "modified",
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

    def test_missing_or_nonexistent_reference_fails_feature_requirement(self):
        client = mock.Mock()
        client.file_exists.return_value = False
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
        self.assertTrue(
            all(call[2]["status"] == "in_progress" for call in calls[1:])
        )

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

    def test_stale_run_does_not_mutate_comments(self):
        for refs in (("newer-head", "base"), ("head", "newer-base")):
            with self.subTest(refs=refs):
                client = FakeRunClient(
                    files=[
                        {
                            "filename": "docs/design-docs/design_docs/20260728-invalid.md",
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
            title="docs: test",
            body="",
            labels=(),
        )
        final_state = checker.PullRequestState(
            head_sha="head",
            base_sha="base",
            head_repository="contributor/milvus",
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
            title="docs: test",
            body="",
            labels=(),
        )
        completion_state = checker.PullRequestState(
            head_sha="head",
            base_sha="base",
            head_repository="contributor/milvus",
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
        self.assertEqual([], client.updated)
        self.assertEqual([], client.deleted)
        self.assertEqual("success", client.completed_checks[-1][1])

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

    def test_legacy_design_doc_path_gets_header_reminder_without_path_failure(self):
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
        self.assertIn("Header validation", client.created[0])
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
        mergify = (REPOSITORY_ROOT / ".github/mergify.yml").read_text(
            encoding="utf-8"
        ).replace(
            "  approved_by_weiliu1031: &approved_by_weiliu1031 "
            "'approved-reviews-by = weiliu1031'\n",
            "",
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
    ):
        super().__init__(comments=comments)
        self.files = files
        self.documents = documents
        self.refs = refs
        self.existing_paths = set(existing_paths or [])
        self.repository_documents = repository_documents or {}
        self.initial_state = initial_state
        self.final_state = final_state
        self.completion_state = completion_state
        self.state_calls = 0
        self.created_checks = []
        self.completed_checks = []

    def configure_default_states(self, head_sha, base_sha, title, body, labels):
        if self.initial_state is None:
            self.initial_state = checker.PullRequestState(
                head_sha=head_sha,
                base_sha=base_sha,
                head_repository="contributor/milvus",
                title=title,
                body=body,
                labels=tuple(sorted(labels)),
            )
        if self.final_state is None:
            self.final_state = checker.PullRequestState(
                head_sha=self.refs[0],
                base_sha=self.refs[1],
                head_repository=self.initial_state.head_repository,
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


if __name__ == "__main__":
    unittest.main()

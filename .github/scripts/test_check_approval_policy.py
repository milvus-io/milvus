import importlib.util
import json
import pathlib
import sys
import tempfile
import unittest
from unittest import mock


CHECKER_PATH = pathlib.Path(__file__).with_name("check_approval_policy.py")
REPOSITORY_ROOT = pathlib.Path(__file__).resolve().parents[2]
SPEC = importlib.util.spec_from_file_location("check_approval_policy", CHECKER_PATH)
checker = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = checker
SPEC.loader.exec_module(checker)


def pull_state(
    *,
    author="contributor",
    title="fix: example",
    labels=(),
    head_sha="head",
    base_sha="base",
    base_ref="master",
):
    return checker.PullRequestState(
        head_sha=head_sha,
        base_sha=base_sha,
        head_ref="feature",
        base_ref=base_ref,
        head_repository="contributor/milvus",
        base_repository="milvus-io/milvus",
        author=author,
        title=title,
        labels=tuple(sorted(labels)),
    )


def prow_notification(
    *approvers,
    comment_id=700,
    repository="milvus-io/milvus",
    titles=None,
    approved=True,
    qualified=None,
    manual_bypass=False,
):
    titles = ("Approved",) * len(approvers) if titles is None else tuple(titles)
    if len(titles) != len(approvers):
        raise ValueError("Each rendered approver needs one title")
    rendered = ", ".join(
        f'*<a href="https://github.com/{repository}/pull/1#issuecomment-1" '
        f'title="{title}">{login}</a>*'
        for login, title in zip(approvers, titles)
    )
    qualified = approvers if qualified is None else tuple(qualified)
    owners_rows = "\n".join(
        f"- ~~[path-{index}/OWNERS](https://github.com/{repository}/blob/master/"
        f"path-{index}/OWNERS)~~ [{login}]"
        for index, login in enumerate(qualified, start=1)
    )
    bypass = f"{checker.PROW_MANUAL_APPROVAL_BYPASS}\n\n" if manual_bypass else ""
    return {
        "id": comment_id,
        "user": {
            "login": checker.PROW_BOT_LOGIN,
            "id": checker.PROW_BOT_USER_ID,
        },
        "body": (
            f"{checker.PROW_APPROVAL_NOTIFICATION_PREFIX} "
            f"This PR is **{'APPROVED' if approved else 'NOT APPROVED'}**\n\n"
            f"{bypass}"
            f"This pull-request has been approved by: {rendered}\n\n"
            "The full list of commands accepted by this bot follows.\n\n"
            "<details>\n"
            "Needs approval from an approver in each of these files:\n\n"
            f"{owners_rows}\n"
            "</details>"
        ),
    }


def approved_label_event(
    actor_login,
    *,
    actor_id=123,
    actor_type="User",
    event_id=900,
    event="labeled",
):
    return {
        "id": event_id,
        "event": event,
        "label": {"name": checker.APPROVED_LABEL},
        "actor": {
            "login": actor_login,
            "id": actor_id,
            "type": actor_type,
        },
    }


class ProwApprovalSnapshotTest(unittest.TestCase):
    def parse(self, comments, author="alice"):
        return checker.extract_prow_approvers(
            comments,
            author,
            "milvus-io/milvus",
            1,
        )

    def test_latest_snapshot_filters_the_author_and_non_owners(self):
        comments = [
            prow_notification("Alice", "Bob", comment_id=700),
            prow_notification(
                "Alice",
                "Carol",
                "outsider",
                comment_id=701,
                qualified=("Alice", "Carol"),
            ),
        ]
        self.assertEqual(("Carol",), self.parse(comments))

    def test_manual_approved_label_bypass_never_counts_approve_actors(self):
        self.assertEqual(
            (),
            self.parse(
                [
                    prow_notification(
                        "Bob",
                        "outsider",
                        qualified=("Bob",),
                        manual_bypass=True,
                    )
                ]
            ),
        )

    def test_rejects_owners_rows_without_matching_approval(self):
        with self.assertRaisesRegex(RuntimeError, "without a matching approval"):
            self.parse([prow_notification("Bob", qualified=("Carol",))])

    def test_accepts_github_app_bot_logins_and_filters_the_bot_author(self):
        for bot_author in ("mergify[bot]", "dependabot[bot]"):
            with self.subTest(bot_author=bot_author):
                notification = prow_notification(
                    bot_author,
                    "Bob",
                    titles=("Author self-approved", "Approved"),
                )
                self.assertEqual(
                    ("Bob",),
                    self.parse(
                        [notification],
                        author=bot_author,
                    ),
                )

    def test_rejects_malformed_github_app_bot_logins(self):
        for login in (
            "mergify[bot]extra",
            "mergify[Bot]",
            "mergify[]",
            "[bot]",
            "mergify[bot][bot]",
            "mergify[bot]</a><img>",
        ):
            with self.subTest(login=login):
                with self.assertRaisesRegex(RuntimeError, "unknown format"):
                    self.parse([prow_notification(login)])

    def test_github_actor_logins_obey_the_github_login_length_limit(self):
        longest_user_login = "a" * 39
        self.assertEqual(
            (longest_user_login,),
            self.parse(
                [prow_notification(longest_user_login)],
                author="author",
            ),
        )

        with self.assertRaisesRegex(RuntimeError, "unknown format"):
            self.parse([prow_notification("a" * 40)])

        longest_bot_login = f"{'a' * 34}[bot]"
        self.assertEqual(
            (),
            self.parse(
                [
                    prow_notification(
                        longest_bot_login,
                        titles=("Author self-approved",),
                    )
                ],
                author=longest_bot_login,
            ),
        )

        too_long_bot_login = f"{'a' * 35}[bot]"
        with self.assertRaisesRegex(RuntimeError, "unknown format"):
            self.parse([prow_notification(too_long_bot_login)])

    def test_missing_snapshot_means_no_prow_approval(self):
        self.assertEqual((), self.parse([]))

    def test_not_approved_snapshot_never_counts_rendered_users(self):
        self.assertEqual(
            (),
            self.parse(
                [
                    prow_notification(
                        "mergify[bot]",
                        titles=("Author self-approved",),
                        approved=False,
                    )
                ],
                author="mergify[bot]",
            ),
        )

    def test_human_cannot_forge_a_snapshot(self):
        forged = prow_notification("Bob")
        forged["user"] = {"login": "contributor", "id": 123}
        self.assertEqual((), self.parse([forged]))

    def test_rejects_partial_bot_identity_matches(self):
        changed = prow_notification("Bob")
        changed["user"]["id"] = 123
        with self.assertRaisesRegex(RuntimeError, "identity changed"):
            self.parse([changed])

    def test_accepts_prow_review_titles(self):
        for title in ("Approved", "LGTM", "Author self-approved"):
            with self.subTest(title=title):
                notification = prow_notification("Bob")
                notification["body"] = notification["body"].replace(
                    'title="Approved"', f'title="{title}"'
                )
                self.assertEqual(("Bob",), self.parse([notification]))

    def test_rejects_unknown_format_cross_pr_links_and_duplicates(self):
        malformed = prow_notification("Bob")
        malformed["body"] = malformed["body"].replace(
            'title="Approved"', 'title="Unknown"'
        )
        with self.assertRaisesRegex(RuntimeError, "unknown format"):
            self.parse([malformed])

        cross_pr = prow_notification("Bob")
        cross_pr["body"] = cross_pr["body"].replace("/pull/1#", "/pull/2#")
        with self.assertRaisesRegex(RuntimeError, "another pull request"):
            self.parse([cross_pr])

        with self.assertRaisesRegex(RuntimeError, "repeats an approver"):
            self.parse([prow_notification("Bob", "bob")])


class KnowhereAutomationCompatibilityTest(unittest.TestCase):
    def state(self):
        return pull_state(
            title="[automated] Update Knowhere Commit",
            labels=(checker.APPROVED_LABEL, checker.KNOWHERE_UPDATE_REQUIRED_LABEL),
        )

    def test_accepts_only_the_mergify_app_actor(self):
        event = approved_label_event(
            checker.MERGIFY_BOT_LOGIN,
            actor_id=checker.MERGIFY_BOT_USER_ID,
            actor_type="Bot",
        )
        self.assertTrue(checker.exact_knowhere_automation(self.state(), [event]))

        for changed_event in (
            approved_label_event("reviewer"),
            approved_label_event(
                checker.MERGIFY_BOT_LOGIN,
                actor_id=123,
                actor_type="Bot",
            ),
            approved_label_event(
                "renamed-mergify[bot]",
                actor_id=checker.MERGIFY_BOT_USER_ID,
                actor_type="Bot",
            ),
        ):
            with self.subTest(changed_event=changed_event):
                self.assertFalse(
                    checker.exact_knowhere_automation(self.state(), [changed_event])
                )

    def test_rejects_invalid_actor_types_and_inconsistent_history(self):
        with self.assertRaisesRegex(RuntimeError, "valid actor"):
            checker.approved_label_actor(
                [approved_label_event("reviewer", actor_type=None)]
            )
        with self.assertRaisesRegex(RuntimeError, "no corresponding issue event"):
            checker.approved_label_actor([])
        with self.assertRaisesRegex(RuntimeError, "state conflicts"):
            checker.approved_label_actor(
                [approved_label_event("reviewer", event="unlabeled")]
            )


class GitHubClientTest(unittest.TestCase):
    def test_pull_request_state_includes_refs_author_and_base_repository(self):
        client = checker.GitHubClient("token", "https://api.github.test")
        client.request = lambda *args, **kwargs: {
            "head": {
                "sha": "head-sha",
                "ref": "feature",
                "repo": {"full_name": "contributor/milvus"},
            },
            "base": {
                "sha": "base-sha",
                "ref": "master",
                "repo": {"full_name": "milvus-io/milvus"},
            },
            "user": {"login": "contributor"},
            "title": "fix: test",
            "labels": [{"name": "approved"}],
        }
        state = client.get_pull_request_state("milvus-io/milvus", 8)
        self.assertEqual("contributor", state.author)
        self.assertEqual("master", state.base_ref)
        self.assertEqual("milvus-io/milvus", state.base_repository)
        self.assertEqual(("approved",), state.labels)

    def test_check_identity_is_independent(self):
        client = checker.GitHubClient("token", "https://api.github.test")
        calls = []

        def request(method, path, payload=None):
            calls.append((method, path, payload))
            if method == "GET":
                return {"check_runs": []}
            if method == "POST":
                return {"id": 99}
            raise AssertionError(f"Unexpected request: {method} {path}")

        client.request = request
        self.assertEqual(
            99,
            client.begin_policy_check("milvus-io/milvus", "head", 8),
        )
        payload = calls[-1][2]
        self.assertEqual("Approval Policy", payload["name"])
        self.assertEqual("approval-policy-pr-8", payload["external_id"])
        self.assertNotIn("Design Doc", json.dumps(calls))


class FakeClient:
    def __init__(
        self,
        *,
        states=None,
        comment_snapshots=None,
        event_snapshots=None,
    ):
        self.states = list(states or [pull_state()])
        self.comment_snapshots = list(
            comment_snapshots
            if comment_snapshots is not None
            else [[prow_notification("Bob")]]
        )
        self.event_snapshots = list(event_snapshots or [[]])
        self.state_calls = 0
        self.comment_calls = 0
        self.event_calls = 0
        self.begun = []
        self.completed = []

    @staticmethod
    def snapshot(values, index):
        return values[min(index, len(values) - 1)]

    def get_pull_request_state(self, repository, pull_number):
        state = self.snapshot(self.states, self.state_calls)
        self.state_calls += 1
        return state

    def list_issue_comments(self, repository, pull_number):
        comments = self.snapshot(self.comment_snapshots, self.comment_calls)
        self.comment_calls += 1
        return comments

    def list_issue_events(self, repository, pull_number):
        events = self.snapshot(self.event_snapshots, self.event_calls)
        self.event_calls += 1
        return events

    def begin_policy_check(self, repository, head_sha, pull_number):
        self.begun.append((repository, head_sha, pull_number))
        return 99

    def complete_policy_check(
        self, repository, check_run_id, conclusion, title, summary
    ):
        self.completed.append((repository, check_run_id, conclusion, title, summary))


class RunTest(unittest.TestCase):
    def run_with_client(self, client):
        event = {
            "repository": {"full_name": "milvus-io/milvus"},
            "pull_request": {"number": 1},
        }
        with tempfile.NamedTemporaryFile(mode="w", encoding="utf-8") as event_file:
            json.dump(event, event_file)
            event_file.flush()
            with mock.patch.object(checker, "policy_client", return_value=client):
                with mock.patch("builtins.print"):
                    return checker.run(event_file.name)

    def test_normal_prow_approval_passes_without_event_api(self):
        client = FakeClient()
        self.assertEqual(0, self.run_with_client(client))
        self.assertEqual("success", client.completed[-1][2])
        self.assertIn("@Bob", client.completed[-1][4])
        self.assertEqual(0, client.event_calls)

    def test_author_approval_does_not_count(self):
        state = pull_state(author="Alice")
        client = FakeClient(
            states=[state],
            comment_snapshots=[[prow_notification("Alice")]],
        )
        self.assertEqual(1, self.run_with_client(client))
        self.assertEqual("failure", client.completed[-1][2])
        self.assertIn("(0/1)", client.completed[-1][4])

    def test_non_author_without_owners_qualification_does_not_count(self):
        client = FakeClient(
            comment_snapshots=[[prow_notification("path-approver", qualified=())]]
        )
        self.assertEqual(1, self.run_with_client(client))
        self.assertEqual("failure", client.completed[-1][2])
        self.assertIn("(0/1)", client.completed[-1][4])

    def test_github_app_bot_author_does_not_poison_a_valid_approval(self):
        bot_author = "mergify[bot]"
        state = pull_state(author=bot_author)
        notification = prow_notification(
            bot_author,
            "Bob",
            titles=("Author self-approved", "Approved"),
        )
        client = FakeClient(
            states=[state],
            comment_snapshots=[[notification]],
        )

        self.assertEqual(0, self.run_with_client(client))
        self.assertEqual("success", client.completed[-1][2])
        self.assertIn("@Bob", client.completed[-1][4])
        self.assertEqual(0, client.event_calls)

    def test_generic_manual_approved_label_does_not_count(self):
        state = pull_state(labels=(checker.APPROVED_LABEL,))
        client = FakeClient(
            states=[state],
            comment_snapshots=[[prow_notification()]],
            event_snapshots=[[approved_label_event("reviewer")]],
        )
        self.assertEqual(1, self.run_with_client(client))
        self.assertEqual("failure", client.completed[-1][2])
        self.assertEqual(0, client.event_calls)

    def test_existing_knowhere_mergify_automation_is_preserved(self):
        state = pull_state(
            author="sre-ci-robot",
            title="[automated] Update Knowhere Commit",
            labels=(checker.APPROVED_LABEL, checker.KNOWHERE_UPDATE_REQUIRED_LABEL),
        )
        event = approved_label_event(
            checker.MERGIFY_BOT_LOGIN,
            actor_id=checker.MERGIFY_BOT_USER_ID,
            actor_type="Bot",
        )
        client = FakeClient(
            states=[state],
            comment_snapshots=[[prow_notification()]],
            event_snapshots=[[event]],
        )
        self.assertEqual(0, self.run_with_client(client))
        self.assertEqual("success", client.completed[-1][2])
        self.assertIn("Knowhere-update automation", client.completed[-1][4])
        self.assertEqual(2, client.event_calls)

    def test_knowhere_label_actor_change_makes_the_result_stale(self):
        state = pull_state(
            title="[automated] Update Knowhere Commit",
            labels=(checker.APPROVED_LABEL, checker.KNOWHERE_UPDATE_REQUIRED_LABEL),
        )
        client = FakeClient(
            states=[state],
            comment_snapshots=[[prow_notification()]],
            event_snapshots=[
                [
                    approved_label_event(
                        checker.MERGIFY_BOT_LOGIN,
                        actor_id=checker.MERGIFY_BOT_USER_ID,
                        actor_type="Bot",
                    )
                ],
                [approved_label_event("reviewer")],
            ],
        )
        self.assertEqual(0, self.run_with_client(client))
        self.assertEqual("neutral", client.completed[-1][2])

    def test_release_pr_is_not_evaluated_until_the_workflow_is_backported(self):
        client = FakeClient(states=[pull_state(base_ref="2.6")])
        self.assertEqual(0, self.run_with_client(client))
        self.assertEqual([], client.begun)
        self.assertEqual([], client.completed)

    def test_unsupported_base_does_not_publish_or_evaluate_a_check(self):
        client = FakeClient(states=[pull_state(base_ref="experimental")])
        self.assertEqual(0, self.run_with_client(client))
        self.assertEqual([], client.begun)
        self.assertEqual([], client.completed)
        self.assertEqual(0, client.comment_calls)
        self.assertEqual(0, client.event_calls)

    def test_pull_request_state_change_completes_the_check_neutral(self):
        client = FakeClient(states=[pull_state(), pull_state(head_sha="new-head")])
        self.assertEqual(0, self.run_with_client(client))
        self.assertEqual("neutral", client.completed[-1][2])
        self.assertEqual(1, client.comment_calls)

    def test_approval_snapshot_change_completes_the_check_neutral(self):
        client = FakeClient(
            comment_snapshots=[
                [prow_notification("Bob", comment_id=700)],
                [prow_notification(comment_id=701)],
            ]
        )
        self.assertEqual(0, self.run_with_client(client))
        self.assertEqual("neutral", client.completed[-1][2])

    def test_api_failure_fails_the_published_check_closed(self):
        client = FakeClient()
        client.list_issue_comments = mock.Mock(
            side_effect=RuntimeError("comments unavailable")
        )
        with self.assertRaisesRegex(RuntimeError, "comments unavailable"):
            self.run_with_client(client)
        self.assertEqual("failure", client.completed[-1][2])
        self.assertEqual(
            "Approval policy could not be evaluated", client.completed[-1][3]
        )


class EventAndWorkflowTest(unittest.TestCase):
    def load(self, event):
        with tempfile.NamedTemporaryFile(mode="w", encoding="utf-8") as event_file:
            json.dump(event, event_file)
            event_file.flush()
            return checker.load_event(event_file.name)

    def test_loads_pull_request_and_issue_comment_events(self):
        repository = {"full_name": "milvus-io/milvus"}
        for event in (
            {"repository": repository, "pull_request": {"number": 7}},
            {
                "repository": repository,
                "issue": {"number": 7, "pull_request": {}},
            },
        ):
            with self.subTest(event=event):
                self.assertEqual(("milvus-io/milvus", 7), self.load(event))

    def test_workflow_has_independent_identity_scope_and_cleanup(self):
        workflow = (
            REPOSITORY_ROOT / ".github/workflows/approval-policy.yml"
        ).read_text(encoding="utf-8")
        self.assertIn("name: Approval Policy\n", workflow)
        self.assertIn("    branches: [master]\n", workflow)
        self.assertIn("  issue_comment:\n", workflow)
        self.assertIn("approval-policy-${{", workflow)
        self.assertIn('check_name: "Approval Policy"', workflow)
        self.assertIn("`approval-policy-pr-${pullNumber}`", workflow)
        self.assertIn("Fail any unfinished head-SHA approval check", workflow)
        self.assertIn("python3 .github/scripts/check_approval_policy.py", workflow)
        self.assertIn("issues: read", workflow)
        self.assertIn("pull-requests: read", workflow)
        self.assertNotIn("issues: write", workflow)
        self.assertNotIn("pull-requests: write", workflow)

    def test_knowhere_auto_approval_keeps_its_existing_conditions(self):
        mergify = (REPOSITORY_ROOT / ".github/mergify.yml").read_text(encoding="utf-8")
        rule = mergify.split(
            "  - name: Assign the 'lgtm' and 'approved' labels following the "
            "successful testing of the 'Update Knowhere Commit'",
            1,
        )[1]

        self.assertIn("      - 'title~=Update Knowhere Commit'\n", rule)
        self.assertIn("      - label=ci-passed\n", rule)
        self.assertNotIn("      - author=sre-ci-robot\n", rule)
        self.assertNotIn("      - '#files=1'\n", rule)


if __name__ == "__main__":
    unittest.main()

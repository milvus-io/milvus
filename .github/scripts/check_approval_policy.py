#!/usr/bin/env python3
"""Publish the narrow, non-author approval policy check for master PRs."""

import argparse
import datetime
import html
import json
import os
import re
import sys
import urllib.error
import urllib.parse
import urllib.request
from dataclasses import dataclass
from typing import Any


POLICY_CHECK_NAME = "Approval Policy"
POLICY_EXTERNAL_ID_PREFIX = "approval-policy"
APPROVED_LABEL = "approved"
PROW_BOT_LOGIN = "sre-ci-robot"
PROW_BOT_USER_ID = 56469371
PROW_APPROVAL_NOTIFICATION_PREFIX = "[APPROVALNOTIFIER]"
MERGIFY_BOT_LOGIN = "mergify[bot]"
MERGIFY_BOT_USER_ID = 37929162
KNOWHERE_UPDATE_TITLE_FRAGMENT = "Update Knowhere Commit"
KNOWHERE_UPDATE_REQUIRED_LABEL = "ci-passed"
API_VERSION = "2022-11-28"

PROW_APPROVED_HEADER = "[APPROVALNOTIFIER] This PR is **APPROVED**"
PROW_NOT_APPROVED_HEADER = "[APPROVALNOTIFIER] This PR is **NOT APPROVED**"
PROW_APPROVAL_LINE_PATTERN = re.compile(
    r"(?m)^This pull-request has been approved by:(?P<approvals>[^\r\n]*)$"
)
PROW_MANUAL_APPROVAL_BYPASS = (
    "Approval requirements bypassed by manually added approval."
)
GITHUB_USER_LOGIN_FRAGMENT = r"[A-Za-z0-9](?:[A-Za-z0-9]|-(?=[A-Za-z0-9])){0,38}"
# GitHub App actors append the five-character "[bot]" suffix; cap the stem at
# 34 characters so the full login keeps GitHub's 39-character limit.
GITHUB_APP_BOT_LOGIN_FRAGMENT = (
    r"[A-Za-z0-9](?:[A-Za-z0-9]|-(?=[A-Za-z0-9])){0,33}\[bot\]"
)
GITHUB_ACTOR_LOGIN_FRAGMENT = (
    rf"(?:{GITHUB_USER_LOGIN_FRAGMENT}|{GITHUB_APP_BOT_LOGIN_FRAGMENT})"
)
PROW_APPROVAL_LINK_PATTERN = re.compile(
    r'\*<a href="([^"\r\n]+)" '
    r'title="(?:Approved|LGTM|Author self-approved)">'
    rf"({GITHUB_ACTOR_LOGIN_FRAGMENT})</a>\*"
)
PROW_APPROVED_OWNERS_LINE_PATTERN = re.compile(
    r"(?m)^- ~~\[[^\r\n]+\]\([^\r\n]+\)~~ " r"\[(?P<approvers>[^\]\r\n]+)\]$"
)
GITHUB_ACTOR_LOGIN_PATTERN = re.compile(rf"^{GITHUB_ACTOR_LOGIN_FRAGMENT}$")
TARGET_BRANCH = "master"


@dataclass(frozen=True)
class PullRequestState:
    head_sha: str
    base_sha: str
    head_ref: str
    base_ref: str
    head_repository: str
    base_repository: str
    author: str
    title: str
    labels: tuple[str, ...]


@dataclass(frozen=True)
class ApprovalResult:
    """The ordinary one-Approver result captured from one GitHub snapshot."""

    approvers: tuple[str, ...]
    automated_knowhere_update: bool = False

    @property
    def satisfied(self) -> bool:
        return bool(self.approvers or self.automated_knowhere_update)


class GitHubClient:
    """GitHub API surface required by the ordinary approval policy only."""

    def __init__(self, token: str, api_url: str) -> None:
        self.token = token
        self.api_url = api_url.rstrip("/")
        self._policy_check_groups: dict[int, tuple[int, ...]] = {}

    def request(
        self,
        method: str,
        path: str,
        payload: dict[str, Any] | None = None,
    ) -> Any:
        body = json.dumps(payload).encode("utf-8") if payload is not None else None
        request = urllib.request.Request(
            f"{self.api_url}{path}",
            data=body,
            method=method,
            headers={
                "Accept": "application/vnd.github+json",
                "Authorization": f"Bearer {self.token}",
                "Content-Type": "application/json",
                "User-Agent": "milvus-approval-policy-check",
                "X-GitHub-Api-Version": API_VERSION,
            },
        )

        try:
            with urllib.request.urlopen(request, timeout=30) as response:
                response_body = response.read()
        except urllib.error.HTTPError as error:
            message = f"HTTP {error.code}"
            try:
                error_body = json.loads(error.read(4096).decode("utf-8"))
                if isinstance(error_body, dict) and error_body.get("message"):
                    message = f"{message}: {error_body['message']}"
            except (UnicodeDecodeError, json.JSONDecodeError):
                pass
            raise RuntimeError(
                f"GitHub API request failed ({method} {path}): {message}"
            ) from error
        except urllib.error.URLError as error:
            raise RuntimeError(
                f"GitHub API request failed ({method} {path}): {error.reason}"
            ) from error

        if not response_body:
            return None
        try:
            return json.loads(response_body.decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError) as error:
            raise RuntimeError(
                f"GitHub API returned invalid JSON ({method} {path})"
            ) from error

    def get_pull_request_state(
        self, repository: str, pull_number: int
    ) -> PullRequestState:
        repository_path = urllib.parse.quote(repository, safe="/")
        pull_request = self.request(
            "GET", f"/repos/{repository_path}/pulls/{pull_number}"
        )
        try:
            if not isinstance(pull_request, dict):
                raise TypeError("invalid pull request")
            head = pull_request["head"]
            base = pull_request["base"]
            if not isinstance(head, dict) or not isinstance(base, dict):
                raise TypeError("invalid refs")

            head_repository_info = head.get("repo")
            head_repository = (
                head_repository_info.get("full_name")
                if isinstance(head_repository_info, dict)
                else repository
            )
            base_repository_info = base.get("repo")
            base_repository = (
                base_repository_info.get("full_name")
                if isinstance(base_repository_info, dict)
                else repository
            )
            author_info = pull_request.get("user")
            author = author_info.get("login") if isinstance(author_info, dict) else None
            head_sha = head.get("sha")
            base_sha = base.get("sha")
            head_ref = head.get("ref")
            base_ref = base.get("ref")
            required_strings = (
                head_sha,
                base_sha,
                head_ref,
                base_ref,
                head_repository,
                base_repository,
                author,
            )
            if any(
                not isinstance(value, str) or not value for value in required_strings
            ):
                raise TypeError("invalid pull-request state")

            labels = tuple(
                sorted(
                    label["name"]
                    for label in pull_request.get("labels", [])
                    if isinstance(label, dict) and isinstance(label.get("name"), str)
                )
            )
            return PullRequestState(
                head_sha=head_sha,
                base_sha=base_sha,
                head_ref=head_ref,
                base_ref=base_ref,
                head_repository=head_repository,
                base_repository=base_repository,
                author=author,
                title=str(pull_request.get("title", "")),
                labels=labels,
            )
        except (KeyError, TypeError) as error:
            raise RuntimeError(
                "GitHub returned an invalid pull-request response"
            ) from error

    def list_issue_comments(
        self, repository: str, pull_number: int
    ) -> list[dict[str, Any]]:
        repository_path = urllib.parse.quote(repository, safe="/")
        comments: list[dict[str, Any]] = []
        for page in range(1, 101):
            page_items = self.request(
                "GET",
                f"/repos/{repository_path}/issues/{pull_number}/comments"
                f"?per_page=100&page={page}",
            )
            if not isinstance(page_items, list):
                raise RuntimeError("GitHub returned an invalid issue-comment list")
            comments.extend(page_items)
            if len(page_items) < 100:
                return comments
        raise RuntimeError("The pull request has too many comments to evaluate safely")

    def list_issue_events(
        self, repository: str, pull_number: int
    ) -> list[dict[str, Any]]:
        repository_path = urllib.parse.quote(repository, safe="/")
        events: list[dict[str, Any]] = []
        for page in range(1, 101):
            page_items = self.request(
                "GET",
                f"/repos/{repository_path}/issues/{pull_number}/events"
                f"?per_page=100&page={page}",
            )
            if not isinstance(page_items, list):
                raise RuntimeError("GitHub returned an invalid issue-event list")
            events.extend(page_items)
            if len(page_items) < 100:
                return events
        raise RuntimeError(
            "The pull request has too many issue events to evaluate safely"
        )

    def begin_policy_check(
        self, repository: str, head_sha: str, pull_number: int
    ) -> int:
        repository_path = urllib.parse.quote(repository, safe="/")
        encoded_sha = urllib.parse.quote(head_sha, safe="")
        encoded_name = urllib.parse.quote(POLICY_CHECK_NAME, safe="")
        external_id = f"{POLICY_EXTERNAL_ID_PREFIX}-pr-{pull_number}"
        existing_check_ids: list[int] = []

        for page in range(1, 11):
            response = self.request(
                "GET",
                f"/repos/{repository_path}/commits/{encoded_sha}/check-runs"
                f"?check_name={encoded_name}&filter=all&per_page=100&page={page}",
            )
            if not isinstance(response, dict) or not isinstance(
                response.get("check_runs"), list
            ):
                raise RuntimeError("GitHub returned an invalid check-run list")
            check_runs = response["check_runs"]
            for check_run in check_runs:
                app = check_run.get("app") if isinstance(check_run, dict) else None
                check_run_id = (
                    check_run.get("id") if isinstance(check_run, dict) else None
                )
                if (
                    isinstance(check_run_id, int)
                    and check_run.get("external_id") == external_id
                    and isinstance(app, dict)
                    and app.get("slug") == "github-actions"
                ):
                    existing_check_ids.append(check_run_id)
            if len(check_runs) < 100:
                break
        else:
            raise RuntimeError("Too many Approval Policy check runs on one commit")

        output = {
            "title": "Approval policy is being evaluated",
            "summary": (
                "Validating that a non-author Milvus Approver approved through "
                "the existing Prow-compatible flow."
            ),
        }
        if existing_check_ids:
            check_run_ids = tuple(sorted(set(existing_check_ids)))
            started_at = utc_now()
            for check_run_id in check_run_ids:
                self.request(
                    "PATCH",
                    f"/repos/{repository_path}/check-runs/{check_run_id}",
                    {
                        "status": "in_progress",
                        "started_at": started_at,
                        "output": output,
                    },
                )
            primary_check_run_id = check_run_ids[-1]
            self._policy_check_groups[primary_check_run_id] = check_run_ids
            return primary_check_run_id

        check_run = self.request(
            "POST",
            f"/repos/{repository_path}/check-runs",
            {
                "name": POLICY_CHECK_NAME,
                "head_sha": head_sha,
                "status": "in_progress",
                "external_id": external_id,
                "output": output,
            },
        )
        check_run_id = check_run.get("id") if isinstance(check_run, dict) else None
        if not isinstance(check_run_id, int):
            raise RuntimeError("GitHub returned an invalid check-run response")
        self._policy_check_groups[check_run_id] = (check_run_id,)
        return check_run_id

    def complete_policy_check(
        self,
        repository: str,
        check_run_id: int,
        conclusion: str,
        title: str,
        summary: str,
    ) -> None:
        repository_path = urllib.parse.quote(repository, safe="/")
        check_run_ids = self._policy_check_groups.pop(check_run_id, (check_run_id,))
        completed_at = utc_now()
        for grouped_check_run_id in check_run_ids:
            self.request(
                "PATCH",
                f"/repos/{repository_path}/check-runs/{grouped_check_run_id}",
                {
                    "status": "completed",
                    "conclusion": conclusion,
                    "completed_at": completed_at,
                    "output": {"title": title, "summary": summary},
                },
            )


def utc_now() -> str:
    return (
        datetime.datetime.now(datetime.timezone.utc).isoformat().replace("+00:00", "Z")
    )


def extract_prow_approvers(
    issue_comments: list[dict[str, Any]],
    author: str,
    repository: str,
    pull_number: int,
) -> tuple[str, ...]:
    """Read the explicit approval set last computed by Prow's approve plugin."""

    notifications: list[tuple[int, str]] = []
    for comment in issue_comments:
        if not isinstance(comment, dict):
            raise RuntimeError("GitHub returned an invalid issue comment")
        user = comment.get("user")
        login = user.get("login") if isinstance(user, dict) else None
        user_id = user.get("id") if isinstance(user, dict) else None
        body = comment.get("body")
        if not isinstance(body, str) or not body.startswith(
            PROW_APPROVAL_NOTIFICATION_PREFIX
        ):
            continue

        login_matches = (
            isinstance(login, str) and login.casefold() == PROW_BOT_LOGIN.casefold()
        )
        id_matches = user_id == PROW_BOT_USER_ID
        if not login_matches and not id_matches:
            continue
        if not login_matches or not id_matches:
            raise RuntimeError("The Prow approval notifier identity changed")
        comment_id = comment.get("id")
        if not isinstance(comment_id, int) or comment_id <= 0:
            raise RuntimeError(
                "GitHub returned a Prow approval notification without a valid ID"
            )
        notifications.append((comment_id, body))

    if not notifications:
        return ()

    _, notification = max(notifications, key=lambda item: item[0])
    first_line = notification.splitlines()[0] if notification else ""
    approval_lines = list(PROW_APPROVAL_LINE_PATTERN.finditer(notification))
    if (
        first_line not in {PROW_APPROVED_HEADER, PROW_NOT_APPROVED_HEADER}
        or len(approval_lines) != 1
    ):
        raise RuntimeError("The Prow approval notification has an unknown format")

    rendered_approvals = approval_lines[0].group("approvals").strip()
    approval_links = list(PROW_APPROVAL_LINK_PATTERN.finditer(rendered_approvals))
    if rendered_approvals != ", ".join(match.group(0) for match in approval_links):
        raise RuntimeError("The Prow approval notification has an unknown format")

    expected_href_prefix = f"https://github.com/{repository}/pull/{pull_number}#"
    if any(
        not match.group(1).startswith(expected_href_prefix) for match in approval_links
    ):
        raise RuntimeError(
            "The Prow approval notification links to another pull request"
        )

    normalized_notified_logins = [match.group(2).casefold() for match in approval_links]
    if len(normalized_notified_logins) != len(set(normalized_notified_logins)):
        raise RuntimeError("The Prow approval notification repeats an approver")

    if first_line == PROW_NOT_APPROVED_HEADER:
        return ()

    # Prow's "approved by" line contains every current /approve actor, even a
    # user who is not an Approver for any changed path.  The struck-through
    # OWNERS rows are the part of the same trusted notification where Prow
    # reports which of those actors actually matched the repository OWNERS
    # rules.  Count only that intersection, and never inherit Prow's generic
    # manually-added-label bypass.
    if PROW_MANUAL_APPROVAL_BYPASS in notification:
        return ()

    qualified_logins: set[str] = set()
    for owners_match in PROW_APPROVED_OWNERS_LINE_PATTERN.finditer(notification):
        rendered_logins = owners_match.group("approvers")
        for login in rendered_logins.split(","):
            if GITHUB_ACTOR_LOGIN_PATTERN.fullmatch(login) is None:
                raise RuntimeError(
                    "The Prow approval notification has an unknown OWNERS format"
                )
            qualified_logins.add(login.casefold())

    notified_logins_by_key = {
        match.group(2).casefold(): match.group(2) for match in approval_links
    }
    if not qualified_logins.issubset(notified_logins_by_key):
        raise RuntimeError(
            "The Prow approval notification lists an OWNERS approver without "
            "a matching approval"
        )

    author_login = author.casefold()
    return tuple(
        sorted(
            (
                login
                for normalized_login, login in notified_logins_by_key.items()
                if normalized_login != author_login
                and normalized_login in qualified_logins
            ),
            key=str.casefold,
        )
    )


def possible_knowhere_automation(state: PullRequestState) -> bool:
    return (
        APPROVED_LABEL in state.labels
        and KNOWHERE_UPDATE_REQUIRED_LABEL in state.labels
        and KNOWHERE_UPDATE_TITLE_FRAGMENT in state.title
    )


def approved_label_actor(issue_events: list[dict[str, Any]]) -> tuple[str, int, str]:
    approved_label_events: list[tuple[int, dict[str, Any]]] = []
    for event in issue_events:
        if not isinstance(event, dict):
            raise RuntimeError("GitHub returned an invalid issue event")
        event_name = event.get("event")
        label = event.get("label")
        label_name = label.get("name") if isinstance(label, dict) else None
        if event_name not in {"labeled", "unlabeled"} or label_name != APPROVED_LABEL:
            continue
        event_id = event.get("id")
        if not isinstance(event_id, int) or event_id <= 0:
            raise RuntimeError(
                "GitHub returned an approved-label event without a valid ID"
            )
        approved_label_events.append((event_id, event))

    if not approved_label_events:
        raise RuntimeError(
            "The current approved label has no corresponding issue event"
        )

    _, latest_event = max(approved_label_events, key=lambda item: item[0])
    if latest_event.get("event") != "labeled":
        raise RuntimeError(
            "The approved label state conflicts with its latest issue event"
        )

    actor = latest_event.get("actor")
    actor_login = actor.get("login") if isinstance(actor, dict) else None
    actor_id = actor.get("id") if isinstance(actor, dict) else None
    actor_type = actor.get("type") if isinstance(actor, dict) else None
    if (
        not isinstance(actor_login, str)
        or not actor_login
        or not isinstance(actor_id, int)
        or actor_id <= 0
        or not isinstance(actor_type, str)
    ):
        raise RuntimeError(
            "GitHub returned an approved-label event without a valid actor"
        )
    return actor_login, actor_id, actor_type


def exact_knowhere_automation(
    state: PullRequestState, issue_events: list[dict[str, Any]] | None
) -> bool:
    if not possible_knowhere_automation(state):
        return False
    if issue_events is None:
        raise RuntimeError("The candidate Knowhere label history was not loaded")
    actor_login, actor_id, actor_type = approved_label_actor(issue_events)
    return (
        actor_login.casefold() == MERGIFY_BOT_LOGIN.casefold()
        and actor_id == MERGIFY_BOT_USER_ID
        and actor_type == "Bot"
    )


def evaluate_ordinary_approval(
    client: GitHubClient,
    repository: str,
    pull_number: int,
    state: PullRequestState,
    knowhere_events: list[dict[str, Any]] | None,
) -> ApprovalResult:
    """Evaluate one ordinary approval snapshot without Design Doc APIs."""

    approvers = extract_prow_approvers(
        client.list_issue_comments(repository, pull_number),
        state.author,
        repository,
        pull_number,
    )
    return ApprovalResult(
        approvers=approvers,
        automated_knowhere_update=exact_knowhere_automation(state, knowhere_events),
    )


def stable_pull_request_state(state: PullRequestState) -> tuple[Any, ...]:
    return (
        state.head_sha,
        state.base_sha,
        state.head_ref,
        state.base_ref,
        state.head_repository,
        state.base_repository,
        state.author.casefold(),
        state.title,
        state.labels,
    )


def build_check_summary(approval: ApprovalResult) -> str:
    lines = ["## Approval policy"]
    if approval.automated_knowhere_update:
        lines.extend(
            [
                "- Non-author Approver requirement: existing "
                "Knowhere-update automation applies",
                "  - The current `approved` label was added by the trusted "
                "Mergify App after the existing title and `ci-passed` rule matched",
            ]
        )
    else:
        approvers = (
            ", ".join(f"@{login}" for login in approval.approvers)
            if approval.approvers
            else "none"
        )
        lines.extend(
            [
                f"- Non-author Approver requirement: "
                f"{'passed' if approval.satisfied else 'failed'} "
                f"({len(approval.approvers)}/1)",
                f"  - Current valid Approvers: {approvers}",
            ]
        )
    return "\n".join(lines)


def load_event(path: str) -> tuple[str, int]:
    with open(path, encoding="utf-8") as event_file:
        event = json.load(event_file)
    try:
        repository = str(event["repository"]["full_name"])
        pull_request = event.get("pull_request")
        issue = event.get("issue")
        if isinstance(pull_request, dict):
            pull_number = int(pull_request["number"])
        elif isinstance(issue, dict) and isinstance(issue.get("pull_request"), dict):
            pull_number = int(issue["number"])
        else:
            raise KeyError("pull_request")
        if not repository or pull_number <= 0:
            raise ValueError("invalid repository or pull request number")
    except (KeyError, TypeError, ValueError) as error:
        raise RuntimeError("The workflow event is not a valid pull request") from error
    return repository, pull_number


def policy_client() -> GitHubClient:
    token = os.environ.get("GH_TOKEN") or os.environ.get("GITHUB_TOKEN")
    if not token:
        raise RuntimeError("GH_TOKEN or GITHUB_TOKEN is required")
    return GitHubClient(
        token=token,
        api_url=os.environ.get("GITHUB_API_URL", "https://api.github.com"),
    )


def complete_stale_policy_check(
    client: GitHubClient, repository: str, check_run_id: int
) -> None:
    client.complete_policy_check(
        repository,
        check_run_id,
        "neutral",
        "Approval policy evaluation became stale",
        "The pull request or approval snapshot changed while this evaluation "
        "was running. A newer workflow run will evaluate the current state.",
    )


def run(event_path: str) -> int:
    repository, pull_number = load_event(event_path)
    client = policy_client()
    initial_state = client.get_pull_request_state(repository, pull_number)
    if initial_state.base_ref != TARGET_BRANCH:
        print(
            "Approval Policy does not apply to pull requests targeting "
            f"{initial_state.base_ref}."
        )
        return 0

    check_run_id = client.begin_policy_check(
        repository, initial_state.head_sha, pull_number
    )
    try:
        knowhere_events = (
            client.list_issue_events(repository, pull_number)
            if possible_knowhere_automation(initial_state)
            else None
        )
        approval = evaluate_ordinary_approval(
            client,
            repository,
            pull_number,
            initial_state,
            knowhere_events,
        )

        current_state = client.get_pull_request_state(repository, pull_number)
        if stable_pull_request_state(current_state) != stable_pull_request_state(
            initial_state
        ):
            complete_stale_policy_check(client, repository, check_run_id)
            print(
                "The pull request changed while approval was being evaluated; "
                "skipping stale check completion."
            )
            return 0

        final_knowhere_events = (
            client.list_issue_events(repository, pull_number)
            if possible_knowhere_automation(current_state)
            else None
        )
        final_approval = evaluate_ordinary_approval(
            client,
            repository,
            pull_number,
            current_state,
            final_knowhere_events,
        )
        if final_approval != approval:
            complete_stale_policy_check(client, repository, check_run_id)
            print(
                "The approval snapshot changed while this evaluation was "
                "running; a newer workflow run will evaluate it."
            )
            return 0

        client.complete_policy_check(
            repository,
            check_run_id,
            "success" if approval.satisfied else "failure",
            (
                "Approval policy passed"
                if approval.satisfied
                else "Required non-author approval is missing"
            ),
            build_check_summary(approval),
        )
        return 0 if approval.satisfied else 1
    except Exception as error:
        try:
            client.complete_policy_check(
                repository,
                check_run_id,
                "failure",
                "Approval policy could not be evaluated",
                f"The trusted approval workflow failed: `{html.escape(str(error))}`",
            )
        except Exception as completion_error:
            print(
                f"Could not publish the failed approval check: {completion_error}",
                file=sys.stderr,
            )
        raise


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--event", required=True, help="GitHub event JSON path")
    arguments = parser.parse_args()
    try:
        return run(arguments.event)
    except Exception as error:
        print(f"Approval policy check failed: {error}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())

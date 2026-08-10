#!/usr/bin/env python3
"""Publish the narrow, non-author approval policy check for master PRs."""

import argparse
import base64
import binascii
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
TARGET_BRANCH = "master"
APPROVED_LABEL = "approved"
OWNERS_ALIASES_PATH = "OWNERS_ALIASES"
PROW_BOT_LOGIN = "sre-ci-robot"
PROW_BOT_USER_ID = 56469371
PROW_APPROVAL_NOTIFICATION_PREFIX = "[APPROVALNOTIFIER]"
AUTOMATED_KNOWHERE_AUTHOR = "sre-ci-robot"
AUTOMATED_KNOWHERE_TITLE = "[automated] Update Knowhere Commit"
AUTOMATED_KNOWHERE_FILE = "internal/core/thirdparty/knowhere/CMakeLists.txt"
AUTOMATED_KNOWHERE_REQUIRED_LABEL = "ci-passed"
MAX_OWNERS_ALIASES_BYTES = 1024 * 1024
API_VERSION = "2022-11-28"

REVIEW_SIGNAL_RUN_TITLE_PATTERN = re.compile(r"^([1-9][0-9]*)$")
PROW_APPROVAL_HEADER_PATTERN = re.compile(
    r"\[APPROVALNOTIFIER\] This PR is \*\*(?:NOT )?APPROVED\*\*"
)
PROW_APPROVAL_LINE_PATTERN = re.compile(
    r"(?m)^This pull-request has been approved by:(?P<approvals>[^\r\n]*)$"
)
PROW_APPROVAL_LINK_PATTERN = re.compile(
    r'\*<a href="([^"\r\n]+)" '
    r'title="(?:Approved|LGTM|Author self-approved)">'
    r"([A-Za-z0-9](?:[A-Za-z0-9]|-(?=[A-Za-z0-9])){0,38})</a>\*"
)


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
    automated_knowhere_update: bool
    manual_approval_actor: str | None = None

    @property
    def satisfied(self) -> bool:
        return bool(
            self.approvers
            or self.automated_knowhere_update
            or self.manual_approval_actor is not None
        )


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

    def get_owners_aliases(self, repository: str, ref: str) -> str:
        """Load only OWNERS_ALIASES, pinned to the target base SHA."""

        repository_path = urllib.parse.quote(repository, safe="/")
        file_path = urllib.parse.quote(OWNERS_ALIASES_PATH, safe="/")
        encoded_ref = urllib.parse.quote(ref, safe="")
        content = self.request(
            "GET",
            f"/repos/{repository_path}/contents/{file_path}?ref={encoded_ref}",
        )
        if not isinstance(content, dict) or content.get("type") != "file":
            raise RuntimeError(
                "GitHub returned an invalid OWNERS_ALIASES file response"
            )
        try:
            return decode_repository_file(content)
        except ValueError as error:
            raise RuntimeError(f"Could not read OWNERS_ALIASES: {error}") from error

    def list_pull_request_files(
        self, repository: str, pull_number: int
    ) -> list[dict[str, Any]]:
        """List files only after the caller identifies a Knowhere candidate."""

        repository_path = urllib.parse.quote(repository, safe="/")
        files: list[dict[str, Any]] = []
        for page in range(1, 31):
            page_items = self.request(
                "GET",
                f"/repos/{repository_path}/pulls/{pull_number}/files"
                f"?per_page=100&page={page}",
            )
            if not isinstance(page_items, list):
                raise RuntimeError("GitHub returned an invalid pull-request file list")
            files.extend(page_items)
            if len(page_items) < 100:
                return files
        raise RuntimeError(
            "The pull request has at least 3000 files, so the exact Knowhere "
            "exception cannot be verified"
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


def decode_repository_file(content: dict[str, Any]) -> str:
    size = content.get("size")
    if not isinstance(size, int) or size < 0:
        raise ValueError("The file size could not be verified.")
    if size > MAX_OWNERS_ALIASES_BYTES:
        raise ValueError(
            f"The file is larger than the "
            f"{MAX_OWNERS_ALIASES_BYTES // 1024} KiB validation limit."
        )
    if content.get("encoding") != "base64" or not isinstance(
        content.get("content"), str
    ):
        raise ValueError("The file content could not be decoded.")

    encoded = "".join(content["content"].splitlines())
    try:
        decoded = base64.b64decode(encoded, validate=True)
    except (binascii.Error, ValueError) as error:
        raise ValueError("The file content is not valid base64.") from error
    if len(decoded) != size:
        raise ValueError("The file content size did not match GitHub metadata.")
    try:
        return decoded.decode("utf-8")
    except UnicodeDecodeError as error:
        raise ValueError("The file content is not valid UTF-8.") from error


def extract_maintainers(owners_aliases: str) -> tuple[str, ...]:
    """Parse the trusted maintainers alias without a YAML dependency."""

    maintainers: list[str] = []
    in_maintainers = False
    for line in owners_aliases.splitlines():
        if line == "  maintainers:":
            if in_maintainers:
                raise ValueError("OWNERS_ALIASES defines maintainers more than once")
            in_maintainers = True
            continue
        if not in_maintainers:
            continue
        if re.match(r"^  [A-Za-z0-9_-]+:$", line):
            break
        if not line.strip() or line.lstrip().startswith("#"):
            continue
        match = re.fullmatch(r"    - ([A-Za-z0-9-]+)", line)
        if match is None:
            raise ValueError("OWNERS_ALIASES has an invalid maintainers entry")
        maintainers.append(match.group(1))

    if not in_maintainers or not maintainers:
        raise ValueError("OWNERS_ALIASES does not define any maintainers")
    if len({login.casefold() for login in maintainers}) != len(maintainers):
        raise ValueError("OWNERS_ALIASES contains duplicate maintainers")
    return tuple(maintainers)


def load_trusted_approvers(
    client: GitHubClient, state: PullRequestState
) -> tuple[str, ...]:
    """Load Approvers from OWNERS_ALIASES at the immutable PR base SHA."""

    try:
        owners_aliases = client.get_owners_aliases(
            state.base_repository, state.base_sha
        )
        return extract_maintainers(owners_aliases)
    except (RuntimeError, ValueError) as error:
        raise RuntimeError(
            "Could not load trusted Approvers from the target base revision: "
            f"{error}"
        ) from error


def extract_prow_approvers(
    issue_comments: list[dict[str, Any]],
    maintainers: tuple[str, ...] | list[str],
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
        PROW_APPROVAL_HEADER_PATTERN.fullmatch(first_line) is None
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

    notified_logins = set(normalized_notified_logins)
    trusted_logins = {login.casefold(): login for login in maintainers}
    author_login = author.casefold()
    return tuple(
        sorted(
            (
                canonical_login
                for normalized_login, canonical_login in trusted_logins.items()
                if normalized_login != author_login
                and normalized_login in notified_logins
            ),
            key=str.casefold,
        )
    )


def extract_non_author_manual_approval_actor(
    issue_events: list[dict[str, Any]], author: str
) -> str | None:
    """Preserve non-author manual approved labels without trusting Prow itself."""

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

    if (
        actor_id == PROW_BOT_USER_ID
        or actor_login.casefold() == PROW_BOT_LOGIN.casefold()
        or actor_login.casefold() == author.casefold()
    ):
        return None
    return actor_login


def possible_knowhere_automation(state: PullRequestState) -> bool:
    """Decide whether the file-list API is allowed for this PR snapshot."""

    return (
        state.author.casefold() == AUTOMATED_KNOWHERE_AUTHOR.casefold()
        and state.title == AUTOMATED_KNOWHERE_TITLE
        and AUTOMATED_KNOWHERE_REQUIRED_LABEL in state.labels
    )


def exact_knowhere_automation(
    state: PullRequestState, files: list[dict[str, Any]] | None
) -> bool:
    if not possible_knowhere_automation(state):
        return False
    if files is None:
        raise RuntimeError("The candidate Knowhere update file list was not loaded")
    return (
        len(files) == 1
        and isinstance(files[0], dict)
        and files[0].get("filename") == AUTOMATED_KNOWHERE_FILE
        and files[0].get("status") == "modified"
    )


def evaluate_ordinary_approval(
    client: GitHubClient,
    repository: str,
    pull_number: int,
    state: PullRequestState,
    knowhere_files: list[dict[str, Any]] | None,
) -> ApprovalResult:
    """Evaluate one ordinary approval snapshot without Design Doc APIs."""

    maintainers = load_trusted_approvers(client, state)
    approvers = extract_prow_approvers(
        client.list_issue_comments(repository, pull_number),
        maintainers,
        state.author,
        repository,
        pull_number,
    )
    automated_knowhere_update = exact_knowhere_automation(state, knowhere_files)
    manual_approval_actor = None
    if (
        not automated_knowhere_update
        and not approvers
        and APPROVED_LABEL in state.labels
    ):
        manual_approval_actor = extract_non_author_manual_approval_actor(
            client.list_issue_events(repository, pull_number), state.author
        )
    return ApprovalResult(
        approvers=approvers,
        automated_knowhere_update=automated_knowhere_update,
        manual_approval_actor=manual_approval_actor,
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
                "- Non-author Approver requirement: existing tested "
                "Knowhere-update automation applies",
                "  - Restricted to @sre-ci-robot, the exact automated title, "
                "one modified Knowhere file, and `ci-passed`",
            ]
        )
    elif approval.manual_approval_actor is not None:
        lines.extend(
            [
                "- Non-author Approver requirement: existing manual "
                "approved-label path applies",
                "  - The current `approved` label was last added by a "
                "non-author, non-Prow actor "
                f"@{approval.manual_approval_actor}",
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
        workflow_run = event.get("workflow_run")
        if isinstance(pull_request, dict):
            pull_number = int(pull_request["number"])
        elif isinstance(issue, dict) and isinstance(issue.get("pull_request"), dict):
            pull_number = int(issue["number"])
        elif isinstance(workflow_run, dict):
            display_title = workflow_run.get("display_title")
            if not isinstance(display_title, str):
                raise KeyError("workflow_run.display_title")
            match = REVIEW_SIGNAL_RUN_TITLE_PATTERN.fullmatch(display_title)
            if match is None:
                raise ValueError("invalid workflow_run.display_title")
            pull_number = int(match.group(1))
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
            f"Approval Policy applies only to pull requests targeting {TARGET_BRANCH}."
        )
        return 0

    check_run_id = client.begin_policy_check(
        repository, initial_state.head_sha, pull_number
    )
    try:
        knowhere_files = (
            client.list_pull_request_files(repository, pull_number)
            if possible_knowhere_automation(initial_state)
            else None
        )
        approval = evaluate_ordinary_approval(
            client,
            repository,
            pull_number,
            initial_state,
            knowhere_files,
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

        final_approval = evaluate_ordinary_approval(
            client,
            repository,
            pull_number,
            current_state,
            knowhere_files,
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

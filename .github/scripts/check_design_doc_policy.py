#!/usr/bin/env python3
"""Enforce Prow approval and Design Doc policy and maintain one PR reminder."""

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


DESIGN_DOC_TREE_PREFIX = "docs/design-docs/design_docs/"
DESIGN_DOC_REFERENCE_PATTERN = re.compile(
    rf"(?im)^[ \t]*(?:[-*][ \t]+)?design doc:[ \t]*"
    rf"({re.escape(DESIGN_DOC_TREE_PREFIX)}[^\r\n]*?\.md)[ \t]*$"
)
COMMENT_MARKER = "<!-- milvus-design-doc-policy-check -->"
COMMENT_PREFIX = f"{COMMENT_MARKER}\n## Design document policy check\n"
POLICY_CHECK_NAME = "Design Doc Policy"
BOT_LOGIN = "github-actions[bot]"
OWNERS_ALIASES_PATH = "OWNERS_ALIASES"
MERGIFY_CONFIG_PATH = ".github/mergify.yml"
DESIGN_DOC_AREA_MATCHER = r"files~=^docs/design-docs/design_docs/.+\.md$"
GOVERNANCE_AREA_MATCHER = (
    r"files~=^(OWNERS_ALIASES|\.github/mergify\.yml|"
    r"\.github/workflows/design-doc-policy(-review-signal)?\.yml|"
    r"\.github/scripts/(check_design_doc_policy|test_check_design_doc_policy)\.py)$"
)
POLICY_CHECK_SUCCESS_LINE = (
    "      - 'check-success = @github-actions/Design Doc Policy'"
)
POLICY_CHECK_STATE_GUARD_LINES = tuple(
    f"      - '-check-{state} = @github-actions/Design Doc Policy'"
    for state in (
        "failure",
        "neutral",
        "skipped",
        "cancelled",
        "timed-out",
        "pending",
        "stale",
    )
)
POLICY_CHECK_SUCCESS_LINES = (
    POLICY_CHECK_SUCCESS_LINE,
    *POLICY_CHECK_STATE_GUARD_LINES,
)
APPROVED_LABEL_LINE = "      - label=approved"
DESIGN_DOC_APPROVAL_LABEL = "approved/design-doc"
DESIGN_DOC_APPROVAL_LABEL_LINE = f"      - label={DESIGN_DOC_APPROVAL_LABEL}"
DESIGN_DOC_APPROVAL_LABEL_COLOR = "0ffa16"
DESIGN_DOC_APPROVAL_LABEL_DESCRIPTION = (
    "Two distinct non-author Approvers approved a formal Design Doc change."
)
GENERAL_REVIEW_SUCCESS_LINES = (
    APPROVED_LABEL_LINE,
    *POLICY_CHECK_SUCCESS_LINES,
)
DESIGN_DOC_REVIEW_SUCCESS_LINES = (DESIGN_DOC_APPROVAL_LABEL_LINE,)
MASTER_ONLY_IF_LINES = ("      - base=master",)
FEATURE_POLICY_IF_LINES = (
    "      - base=master",
    "      - or:",
    "          - 'title~=^feat:'",
    "          - label=kind/feature",
)
FEATURE_POLICY_SUCCESS_LINES = (
    "      - or:",
    "          - *added_design_doc",
    "          - *modified_design_doc",
    "          - *design_doc_body",
    *POLICY_CHECK_SUCCESS_LINES,
)
GOVERNANCE_ENFORCEMENT_PATHS = frozenset(
    {
        OWNERS_ALIASES_PATH,
        MERGIFY_CONFIG_PATH,
        ".github/workflows/design-doc-policy.yml",
        ".github/workflows/design-doc-policy-review-signal.yml",
        ".github/scripts/check_design_doc_policy.py",
        ".github/scripts/test_check_design_doc_policy.py",
    }
)
MAX_HEADER_LINES = 50
MAX_BLOB_BYTES = 1024 * 1024
MAX_COMMENT_CHARS = 60_000
API_VERSION = "2022-11-28"

FIELD_SPECS = (
    ("Feature DRI", "login", "- Feature DRI: @github-login"),
    ("Primary Approver", "login", "- Primary Approver: @github-login"),
    (
        "Independent Approver",
        "login",
        "- Independent Approver: @github-login",
    ),
    ("Design Review", "date", "- Design Review: YYYY-MM-DD"),
)
FIELD_PATTERNS = {
    name: re.compile(rf"^- {re.escape(name)}: (.*?)$") for name, _, _ in FIELD_SPECS
}


@dataclass(frozen=True)
class PullRequestState:
    head_sha: str
    base_sha: str
    head_repository: str
    base_repository: str
    author: str
    title: str
    body: str
    labels: tuple[str, ...]


@dataclass(frozen=True)
class ApprovalRequirement:
    approvers: tuple[str, ...]
    required: int
    formal_design_doc: bool
    governance_enforcement: bool

    @property
    def satisfied(self) -> bool:
        return len(self.approvers) >= self.required


SLASH_COMMAND_PATTERN = re.compile(r"^/([^\s]+)[\t ]*([^\n\r]*)", re.MULTILINE)
REVIEW_SIGNAL_RUN_TITLE_PATTERN = re.compile(r"^([1-9][0-9]*)$")
LOGIN_PATTERN = re.compile(r"^@[A-Za-z0-9](?:[A-Za-z0-9]|-(?=[A-Za-z0-9])){0,38}$")
LOGIN_PLACEHOLDERS = {
    "@github-handle",
    "@github-login",
    "@name",
    "@username",
}
DATE_PATTERN = re.compile(r"^\d{4}-\d{2}-\d{2}$")
ATX_HEADING = re.compile(r"^ {0,3}(#{1,6})(?:[ \t]|$)")
SETEXT_H1_UNDERLINE = re.compile(r"^ {0,3}=+[ \t]*$")
SETEXT_H2_UNDERLINE = re.compile(r"^ {0,3}-+[ \t]*$")
OPENING_FENCE = re.compile(r"^ {0,3}((?:\x60){3,}|~{3,})(.*)$")
RAW_HTML_BLOCK_START = re.compile(
    r"^ {0,3}<(?:/?[A-Za-z][A-Za-z0-9-]*|![A-Za-z][A-Za-z0-9-]*)" r"(?:[ \t\f/>]|$)",
    re.IGNORECASE,
)
INLINE_HIDDEN_HTML_START = re.compile(
    r"(?:<\?|<!\[CDATA\[|"
    r"<(?:details|h[1-6]|pre|script|style|template|textarea)(?:[ \t\f/>]|$))",
    re.IGNORECASE,
)


class GitHubClient:
    def __init__(self, token: str, api_url: str) -> None:
        self.token = token
        self.api_url = api_url.rstrip("/")
        self._policy_check_groups: dict[int, tuple[int, ...]] = {}

    def request(
        self,
        method: str,
        path: str,
        payload: dict[str, Any] | None = None,
        allow_not_found: bool = False,
    ) -> Any:
        body = None
        if payload is not None:
            body = json.dumps(payload).encode("utf-8")

        request = urllib.request.Request(
            f"{self.api_url}{path}",
            data=body,
            method=method,
            headers={
                "Accept": "application/vnd.github+json",
                "Authorization": f"Bearer {self.token}",
                "Content-Type": "application/json",
                "User-Agent": "milvus-design-doc-policy-check",
                "X-GitHub-Api-Version": API_VERSION,
            },
        )

        try:
            with urllib.request.urlopen(request, timeout=30) as response:
                response_body = response.read()
        except urllib.error.HTTPError as error:
            if allow_not_found and error.code == 404:
                return None
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

    def list_pull_request_files(
        self, repository: str, pull_number: int
    ) -> list[dict[str, Any]]:
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
            "The pull request has at least 3000 files, so GitHub cannot expose "
            "a complete file list for header validation"
        )

    def get_blob(self, repository: str, sha: str) -> dict[str, Any]:
        repository_path = urllib.parse.quote(repository, safe="/")
        blob = self.request("GET", f"/repos/{repository_path}/git/blobs/{sha}")
        if not isinstance(blob, dict):
            raise RuntimeError("GitHub returned an invalid blob response")
        return blob

    def get_repository_file(self, repository: str, path: str, ref: str) -> str:
        repository_path = urllib.parse.quote(repository, safe="/")
        file_path = urllib.parse.quote(path, safe="/")
        encoded_ref = urllib.parse.quote(ref, safe="")
        content = self.request(
            "GET",
            f"/repos/{repository_path}/contents/{file_path}?ref={encoded_ref}",
        )
        if not isinstance(content, dict) or content.get("type") != "file":
            raise RuntimeError(f"GitHub returned an invalid file response for {path}")
        try:
            return decode_blob(content)
        except ValueError as error:
            raise RuntimeError(f"Could not read {path}: {error}") from error

    def file_exists(self, repository: str, path: str, ref: str) -> bool:
        repository_path = urllib.parse.quote(repository, safe="/")
        file_path = urllib.parse.quote(path, safe="/")
        encoded_ref = urllib.parse.quote(ref, safe="")
        content = self.request(
            "GET",
            f"/repos/{repository_path}/contents/{file_path}?ref={encoded_ref}",
            allow_not_found=True,
        )
        return isinstance(content, dict) and content.get("type") == "file"

    def get_default_branch(self, repository: str) -> str:
        repository_path = urllib.parse.quote(repository, safe="/")
        repository_info = self.request("GET", f"/repos/{repository_path}")
        default_branch = (
            repository_info.get("default_branch")
            if isinstance(repository_info, dict)
            else None
        )
        if not isinstance(default_branch, str) or not default_branch:
            raise RuntimeError("GitHub returned an invalid repository response")
        return default_branch

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
        raise RuntimeError("The pull request has too many comments to update safely")

    def list_pull_request_review_comments(
        self, repository: str, pull_number: int
    ) -> list[dict[str, Any]]:
        repository_path = urllib.parse.quote(repository, safe="/")
        comments: list[dict[str, Any]] = []
        for page in range(1, 101):
            page_items = self.request(
                "GET",
                f"/repos/{repository_path}/pulls/{pull_number}/comments"
                f"?per_page=100&page={page}",
            )
            if not isinstance(page_items, list):
                raise RuntimeError(
                    "GitHub returned an invalid pull-request review-comment list"
                )
            comments.extend(page_items)
            if len(page_items) < 100:
                return comments
        raise RuntimeError(
            "The pull request has too many review comments to evaluate safely"
        )

    def list_pull_request_reviews(
        self, repository: str, pull_number: int
    ) -> list[dict[str, Any]]:
        repository_path = urllib.parse.quote(repository, safe="/")
        reviews: list[dict[str, Any]] = []
        for page in range(1, 101):
            page_items = self.request(
                "GET",
                f"/repos/{repository_path}/pulls/{pull_number}/reviews"
                f"?per_page=100&page={page}",
            )
            if not isinstance(page_items, list):
                raise RuntimeError(
                    "GitHub returned an invalid pull-request review list"
                )
            reviews.extend(page_items)
            if len(page_items) < 100:
                return reviews
        raise RuntimeError("The pull request has too many reviews to evaluate safely")

    def get_pull_request_state(
        self, repository: str, pull_number: int
    ) -> PullRequestState:
        repository_path = urllib.parse.quote(repository, safe="/")
        pull_request = self.request(
            "GET", f"/repos/{repository_path}/pulls/{pull_number}"
        )
        try:
            head_repository_info = pull_request["head"].get("repo")
            if isinstance(head_repository_info, dict):
                head_repository = head_repository_info.get("full_name")
                if not isinstance(head_repository, str) or not head_repository:
                    raise TypeError("invalid head repository")
            else:
                head_repository = repository
            base_repository_info = pull_request["base"].get("repo")
            if isinstance(base_repository_info, dict):
                base_repository = base_repository_info.get("full_name")
                if not isinstance(base_repository, str) or not base_repository:
                    raise TypeError("invalid base repository")
            else:
                base_repository = repository
            author_info = pull_request.get("user")
            author = author_info.get("login") if isinstance(author_info, dict) else None
            if not isinstance(author, str) or not author:
                raise TypeError("invalid pull-request author")
            head_sha = pull_request["head"]["sha"]
            base_sha = pull_request["base"]["sha"]
            if not isinstance(head_sha, str) or not head_sha:
                raise TypeError("invalid head SHA")
            if not isinstance(base_sha, str) or not base_sha:
                raise TypeError("invalid base SHA")
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
                head_repository=head_repository,
                base_repository=base_repository,
                author=author,
                title=str(pull_request.get("title", "")),
                body=str(pull_request.get("body") or ""),
                labels=labels,
            )
        except (KeyError, TypeError) as error:
            raise RuntimeError(
                "GitHub returned an invalid pull-request response"
            ) from error

    def ensure_repository_label(
        self,
        repository: str,
        name: str,
        color: str,
        description: str,
    ) -> None:
        repository_path = urllib.parse.quote(repository, safe="/")
        label_path = urllib.parse.quote(name, safe="")
        existing = self.request(
            "GET",
            f"/repos/{repository_path}/labels/{label_path}",
            allow_not_found=True,
        )
        if existing is not None:
            if not isinstance(existing, dict) or existing.get("name") != name:
                raise RuntimeError(
                    "GitHub returned an invalid repository-label response"
                )
            return
        try:
            created = self.request(
                "POST",
                f"/repos/{repository_path}/labels",
                {"name": name, "color": color, "description": description},
            )
        except RuntimeError as create_error:
            raced_label = self.request(
                "GET",
                f"/repos/{repository_path}/labels/{label_path}",
                allow_not_found=True,
            )
            if isinstance(raced_label, dict) and raced_label.get("name") == name:
                return
            raise create_error
        if not isinstance(created, dict) or created.get("name") != name:
            raise RuntimeError("GitHub returned an invalid created-label response")

    def add_pull_request_label(
        self, repository: str, pull_number: int, label: str
    ) -> None:
        repository_path = urllib.parse.quote(repository, safe="/")
        self.request(
            "POST",
            f"/repos/{repository_path}/issues/{pull_number}/labels",
            {"labels": [label]},
        )

    def remove_pull_request_label(
        self, repository: str, pull_number: int, label: str
    ) -> None:
        repository_path = urllib.parse.quote(repository, safe="/")
        label_path = urllib.parse.quote(label, safe="")
        self.request(
            "DELETE",
            f"/repos/{repository_path}/issues/{pull_number}/labels/{label_path}",
            allow_not_found=True,
        )

    def create_policy_check(
        self, repository: str, head_sha: str, pull_number: int
    ) -> int:
        repository_path = urllib.parse.quote(repository, safe="/")
        encoded_sha = urllib.parse.quote(head_sha, safe="")
        encoded_name = urllib.parse.quote(POLICY_CHECK_NAME, safe="")
        external_id = f"design-doc-policy-pr-{pull_number}"
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
            raise RuntimeError("Too many Design Doc Policy check runs on one commit")

        if existing_check_ids:
            check_run_ids = tuple(sorted(set(existing_check_ids)))
            for check_run_id in check_run_ids:
                self.request(
                    "PATCH",
                    f"/repos/{repository_path}/check-runs/{check_run_id}",
                    {
                        "status": "in_progress",
                        "started_at": datetime.datetime.now(datetime.timezone.utc)
                        .isoformat()
                        .replace("+00:00", "Z"),
                        "output": {
                            "title": "Design Doc policy is being evaluated",
                            "summary": "Validating Prow approvals, feature "
                            "documentation, repository governance, and changed "
                            "Design Doc headers.",
                        },
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
                "output": {
                    "title": "Design Doc policy is being evaluated",
                    "summary": "Validating Prow approvals, feature documentation, "
                    "repository governance, and changed Design Doc headers.",
                },
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
        completed_at = (
            datetime.datetime.now(datetime.timezone.utc)
            .isoformat()
            .replace("+00:00", "Z")
        )
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

    def create_comment(self, repository: str, pull_number: int, body: str) -> None:
        repository_path = urllib.parse.quote(repository, safe="/")
        self.request(
            "POST",
            f"/repos/{repository_path}/issues/{pull_number}/comments",
            {"body": body},
        )

    def update_comment(self, repository: str, comment_id: int, body: str) -> None:
        repository_path = urllib.parse.quote(repository, safe="/")
        self.request(
            "PATCH",
            f"/repos/{repository_path}/issues/comments/{comment_id}",
            {"body": body},
        )

    def delete_comment(self, repository: str, comment_id: int) -> None:
        repository_path = urllib.parse.quote(repository, safe="/")
        self.request("DELETE", f"/repos/{repository_path}/issues/comments/{comment_id}")


def is_design_doc_path(path: str) -> bool:
    if not path.startswith(DESIGN_DOC_TREE_PREFIX) or not path.endswith(".md"):
        return False

    relative_path = path[len(DESIGN_DOC_TREE_PREFIX) :]
    parts = relative_path.split("/")
    return bool(relative_path) and all(
        part not in {"", ".", ".."} and not any(char in part for char in "\r\n\t")
        for part in parts
    )


def formal_design_doc_changed(files: list[dict[str, Any]]) -> bool:
    return any(
        isinstance(file_info.get(field), str) and is_design_doc_path(file_info[field])
        for file_info in files
        for field in ("filename", "previous_filename")
    )


def _timeline_timestamp(value: Any, source: str) -> datetime.datetime:
    if not isinstance(value, str) or not value:
        raise RuntimeError(f"GitHub returned a {source} without a timestamp")
    try:
        timestamp = datetime.datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as error:
        raise RuntimeError(
            f"GitHub returned a {source} with an invalid timestamp"
        ) from error
    if timestamp.tzinfo is None:
        raise RuntimeError(f"GitHub returned a {source} without a timezone")
    return timestamp


def _slash_approval_updates(body: str) -> list[bool]:
    updates: list[bool] = []
    for match in SLASH_COMMAND_PATTERN.finditer(body):
        command = match.group(1).casefold()
        arguments = match.group(2).strip().casefold()
        if command == "approve":
            updates.append("cancel" not in arguments)
        elif command == "remove-approve":
            updates.append(False)
    return updates


def reconstruct_active_approvers(
    issue_comments: list[dict[str, Any]],
    review_comments: list[dict[str, Any]],
    reviews: list[dict[str, Any]],
    maintainers: list[str],
    author: str,
) -> tuple[str, ...]:
    """Rebuild Prow-compatible sticky approval state from the full PR timeline."""

    timeline: list[tuple[datetime.datetime, int, int, str, dict[str, Any]]] = []
    sources = (
        (review_comments, 0, "review comment", "created_at"),
        (issue_comments, 1, "issue comment", "created_at"),
        (reviews, 2, "review", "submitted_at"),
    )
    for items, source_priority, source, timestamp_field in sources:
        for index, item in enumerate(items):
            if not isinstance(item, dict):
                raise RuntimeError(f"GitHub returned an invalid {source}")
            if (
                source == "review"
                and item.get("submitted_at") is None
                and str(item.get("state", "")).upper() == "PENDING"
            ):
                continue
            timeline.append(
                (
                    _timeline_timestamp(item.get(timestamp_field), source),
                    source_priority,
                    index,
                    source,
                    item,
                )
            )
    timeline.sort(key=lambda event: event[:3])

    approval_state: dict[str, bool] = {}
    for _, _, _, source, item in timeline:
        user = item.get("user")
        login = user.get("login") if isinstance(user, dict) else None
        if not isinstance(login, str) or not login:
            raise RuntimeError(f"GitHub returned a {source} without an author")
        normalized_login = login.casefold()

        if source == "review":
            state = item.get("state")
            if not isinstance(state, str):
                raise RuntimeError("GitHub returned a review without a state")
            normalized_state = state.upper()
            if normalized_state == "APPROVED":
                approval_state[normalized_login] = True
            elif normalized_state == "CHANGES_REQUESTED":
                approval_state[normalized_login] = False

        body = item.get("body")
        if body is None:
            body = ""
        if not isinstance(body, str):
            raise RuntimeError(f"GitHub returned a {source} with an invalid body")
        for approved in _slash_approval_updates(body):
            approval_state[normalized_login] = approved

    trusted_logins = {login.casefold(): login for login in maintainers}
    author_login = author.casefold()
    return tuple(
        sorted(
            (
                canonical_login
                for normalized_login, canonical_login in trusted_logins.items()
                if normalized_login != author_login
                and approval_state.get(normalized_login, False)
            ),
            key=str.casefold,
        )
    )


def evaluate_approval_requirement(
    client: GitHubClient,
    state: PullRequestState,
    files: list[dict[str, Any]],
    issue_comments: list[dict[str, Any]],
    review_comments: list[dict[str, Any]],
    reviews: list[dict[str, Any]],
) -> ApprovalRequirement:
    try:
        owners_aliases = client.get_repository_file(
            state.base_repository, OWNERS_ALIASES_PATH, state.base_sha
        )
        maintainers = extract_maintainers(owners_aliases)
    except (RuntimeError, ValueError) as error:
        raise RuntimeError(
            "Could not load trusted Approvers from the target base revision: "
            f"{error}"
        ) from error

    formal_design_doc = formal_design_doc_changed(files)
    governance_enforcement = governance_enforcement_changed(files)
    required = 2 if formal_design_doc or governance_enforcement else 1
    return ApprovalRequirement(
        approvers=reconstruct_active_approvers(
            issue_comments,
            review_comments,
            reviews,
            maintainers,
            state.author,
        ),
        required=required,
        formal_design_doc=formal_design_doc,
        governance_enforcement=governance_enforcement,
    )


def sync_design_doc_approval_label(
    client: GitHubClient,
    repository: str,
    pull_number: int,
    labels: tuple[str, ...],
    approval: ApprovalRequirement,
) -> None:
    label_present = DESIGN_DOC_APPROVAL_LABEL in labels
    should_be_present = approval.formal_design_doc and approval.satisfied
    if should_be_present and not label_present:
        client.ensure_repository_label(
            repository,
            DESIGN_DOC_APPROVAL_LABEL,
            DESIGN_DOC_APPROVAL_LABEL_COLOR,
            DESIGN_DOC_APPROVAL_LABEL_DESCRIPTION,
        )
        client.add_pull_request_label(
            repository, pull_number, DESIGN_DOC_APPROVAL_LABEL
        )
    elif label_present and not should_be_present:
        client.remove_pull_request_label(
            repository, pull_number, DESIGN_DOC_APPROVAL_LABEL
        )


def stable_pull_request_state(state: PullRequestState) -> tuple[Any, ...]:
    """Ignore only the label managed by this workflow during race detection."""

    return (
        state.head_sha,
        state.base_sha,
        state.head_repository,
        state.base_repository,
        state.author.casefold(),
        state.title,
        state.body,
        tuple(label for label in state.labels if label != DESIGN_DOC_APPROVAL_LABEL),
    )


def extract_design_doc_references(body: str) -> list[str]:
    return sorted(
        {
            path
            for path in DESIGN_DOC_REFERENCE_PATTERN.findall(body)
            if is_design_doc_path(path)
        }
    )


def is_feature_pull_request(title: str, labels: list[str]) -> bool:
    return title.startswith("feat:") or "kind/feature" in labels


def select_design_doc_files(
    files: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    selected: dict[str, dict[str, Any]] = {}
    for file_info in files:
        filename = file_info.get("filename")
        status = file_info.get("status")
        if (
            isinstance(filename, str)
            and status != "removed"
            and is_design_doc_path(filename)
        ):
            selected[filename] = file_info
    return [selected[path] for path in sorted(selected)]


def validate_feature_design_doc_requirement(
    client: GitHubClient,
    base_repository: str,
    base_sha: str,
    head_repository: str,
    head_sha: str,
    title: str,
    labels: list[str],
    body: str,
    files: list[dict[str, Any]],
) -> str | None:
    if not is_feature_pull_request(title, labels):
        return None

    if select_design_doc_files(files):
        return None

    references = extract_design_doc_references(body)
    if not references:
        return (
            "This feature pull request must add or update a formal design "
            "document, or include an exact `design doc: "
            "docs/design-docs/design_docs/<path>.md` "
            "line in its description."
        )

    removed_paths: set[str] = set()
    for file_info in files:
        filename = file_info.get("filename")
        previous_filename = file_info.get("previous_filename")
        status = file_info.get("status")
        if status == "removed" and isinstance(filename, str):
            removed_paths.add(filename)
        if status == "renamed" and isinstance(previous_filename, str):
            removed_paths.add(previous_filename)

    unresolved_paths: list[str] = []
    for path in references:
        if path in removed_paths:
            continue
        if client.file_exists(head_repository, path, head_sha) or client.file_exists(
            base_repository, path, base_sha
        ):
            return None
        unresolved_paths.append(path)

    if unresolved_paths:
        default_branch = client.get_default_branch(base_repository)
        for path in unresolved_paths:
            if client.file_exists(base_repository, path, default_branch):
                return None

    return (
        "None of the formal design-document paths listed in the pull request "
        "description exists at the pull request head, target base, or repository "
        "default branch. Correct the path or add the document to this repository."
    )


def strip_html_comments(line: str, comment_open: bool) -> tuple[str, bool]:
    output: list[str] = []
    cursor = 0

    while cursor < len(line):
        if comment_open:
            comment_end = line.find("-->", cursor)
            if comment_end == -1:
                return "".join(output), True
            cursor = comment_end + 3
            comment_open = False
            continue

        comment_start = line.find("<!--", cursor)
        if comment_start == -1:
            output.append(line[cursor:])
            break
        output.append(line[cursor:comment_start])
        cursor = comment_start + 4
        comment_open = True

    return "".join(output), comment_open


def iter_metadata_lines(document: str) -> list[str]:
    metadata: list[str] = []
    comment_open = False
    previous_visible_line = ""
    title_seen = False
    markdown_lines = document.replace("\r\n", "\n").replace("\r", "\n").split("\n")

    for line_number, raw_line in enumerate(markdown_lines):
        if line_number >= MAX_HEADER_LINES:
            break

        if line_number == 0:
            raw_line = raw_line.lstrip("\ufeff")

        line_touches_comment = comment_open or "<!--" in raw_line
        line, comment_open = strip_html_comments(raw_line, comment_open)
        if RAW_HTML_BLOCK_START.match(line) or INLINE_HIDDEN_HTML_START.search(line):
            break

        opening_match = OPENING_FENCE.match(line)
        if opening_match:
            marker = opening_match.group(1)
            info_string = opening_match.group(2)
            if marker[0] != "\x60" or "\x60" not in info_string:
                break

        heading_match = ATX_HEADING.match(line)
        if heading_match:
            if len(heading_match.group(1)) == 1 and not title_seen:
                title_seen = True
            else:
                break
        if previous_visible_line.strip():
            if SETEXT_H1_UNDERLINE.match(line):
                if title_seen:
                    break
                title_seen = True
            elif SETEXT_H2_UNDERLINE.match(line):
                break
        metadata.append("" if line_touches_comment else line)
        previous_visible_line = line

    return metadata


def is_valid_login(value: str) -> bool:
    return (
        LOGIN_PATTERN.fullmatch(value) is not None
        and value.casefold() not in LOGIN_PLACEHOLDERS
    )


def is_valid_date(value: str) -> bool:
    if DATE_PATTERN.fullmatch(value) is None:
        return False
    try:
        datetime.date.fromisoformat(value)
    except ValueError:
        return False
    return True


def validate_header(document: str) -> list[str]:
    values = {name: [] for name, _, _ in FIELD_SPECS}
    for line in iter_metadata_lines(document):
        for name, _, _ in FIELD_SPECS:
            match = FIELD_PATTERNS[name].fullmatch(line)
            if match:
                values[name].append(match.group(1))
                break

    issues: list[str] = []
    for name, value_type, expected_line in FIELD_SPECS:
        field_values = values[name]
        expected = f"<code>{html.escape(expected_line)}</code>"
        if not field_values:
            issues.append(f"Missing <code>{name}</code>; expected {expected}.")
            continue
        if len(field_values) > 1:
            issues.append(
                f"<code>{name}</code> must appear exactly once; expected {expected}."
            )
            continue

        value = field_values[0]
        valid = is_valid_login(value) if value_type == "login" else is_valid_date(value)
        if not valid:
            issues.append(
                f"<code>{name}</code> has an invalid value; expected {expected}."
            )

    role_names = ("Feature DRI", "Primary Approver", "Independent Approver")
    role_values = [values[name][0] for name in role_names if len(values[name]) == 1]
    if (
        len(role_values) == len(role_names)
        and all(is_valid_login(value) for value in role_values)
        and len({value.casefold() for value in role_values}) != len(role_values)
    ):
        issues.append(
            "<code>Feature DRI</code>, <code>Primary Approver</code>, and "
            "<code>Independent Approver</code> must name three distinct GitHub users."
        )

    return issues


def decode_blob(blob: dict[str, Any]) -> str:
    size = blob.get("size")
    if not isinstance(size, int) or size < 0:
        raise ValueError("The file size could not be verified.")
    if size > MAX_BLOB_BYTES:
        raise ValueError(
            f"The file is larger than the {MAX_BLOB_BYTES // 1024} KiB "
            "validation limit."
        )
    if blob.get("encoding") != "base64" or not isinstance(blob.get("content"), str):
        raise ValueError("The file content could not be decoded.")

    encoded = "".join(blob["content"].splitlines())
    try:
        content = base64.b64decode(encoded, validate=True)
    except (binascii.Error, ValueError) as error:
        raise ValueError("The file content is not valid base64.") from error
    if len(content) != size:
        raise ValueError("The file content size did not match GitHub metadata.")
    try:
        return content.decode("utf-8")
    except UnicodeDecodeError as error:
        raise ValueError("The file content is not valid UTF-8.") from error


def governance_enforcement_changed(files: list[dict[str, Any]]) -> bool:
    for file_info in files:
        for field in ("filename", "previous_filename"):
            path = file_info.get(field)
            if isinstance(path, str) and path in GOVERNANCE_ENFORCEMENT_PATHS:
                return True
    return False


def extract_maintainers(owners_aliases: str) -> list[str]:
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
    return maintainers


def merge_protections_section(mergify: str) -> str:
    markers = list(re.finditer(r"^merge_protections:$", mergify, re.MULTILINE))
    if len(markers) != 1:
        raise ValueError(
            "Mergify must define exactly one active merge_protections section"
        )

    section_start = markers[0].end()
    next_section = re.search(
        r"^[A-Za-z_][A-Za-z0-9_-]*:",
        mergify[section_start:],
        re.MULTILINE,
    )
    section_end = (
        section_start + next_section.start()
        if next_section is not None
        else len(mergify)
    )
    return mergify[section_start:section_end]


def configuration_rule(merge_protections: str, name: str) -> str:
    marker = f"  - name: {name}"
    matches = list(
        re.finditer(rf"^{re.escape(marker)}$", merge_protections, re.MULTILINE)
    )
    if len(matches) != 1:
        raise ValueError(
            f"Mergify must define the {name!r} rule exactly once under "
            "merge_protections"
        )

    rule_start = matches[0].end()
    next_rule = re.search(
        r"^  - name: .+$",
        merge_protections[rule_start:],
        re.MULTILINE,
    )
    rule_end = (
        rule_start + next_rule.start()
        if next_rule is not None
        else len(merge_protections)
    )
    return merge_protections[rule_start:rule_end]


def has_exact_shared_anchor(mergify: str, name: str, matcher: str) -> bool:
    expected_line = f"  {name}: &{name} '{matcher}'"
    key_pattern = re.compile(rf"^  {re.escape(name)}:")
    anchor_pattern = re.compile(rf"&{re.escape(name)}(?=\s|$)")
    key_definitions = [line for line in mergify.splitlines() if key_pattern.match(line)]
    anchor_definitions = [
        line for line in mergify.splitlines() if anchor_pattern.search(line)
    ]
    return key_definitions == [expected_line] and anchor_definitions == [expected_line]


def rule_block_lines(rule: str, declaration: str) -> tuple[str, ...] | None:
    lines = rule.splitlines()
    markers = [index for index, line in enumerate(lines) if line == declaration]
    if len(markers) != 1:
        return None

    conditions: list[str] = []
    for line in lines[markers[0] + 1 :]:
        if not line.strip() or line.lstrip().startswith("#"):
            continue
        indentation = len(line) - len(line.lstrip())
        if indentation <= 4:
            break
        conditions.append(line)
    return tuple(conditions)


def if_condition_lines(rule: str) -> tuple[str, ...] | None:
    return rule_block_lines(rule, "    if:")


def success_condition_lines(rule: str) -> tuple[str, ...] | None:
    return rule_block_lines(rule, "    success_conditions:")


def has_exact_rule_scalar(rule: str, field: str, value: str) -> bool:
    expected_line = f"    {field}: {value}"
    definitions = [
        line
        for line in rule.splitlines()
        if re.match(rf"^    {re.escape(field)}:", line)
    ]
    return definitions == [expected_line]


def validate_approver_governance(
    owners_aliases: str,
    mergify: str,
) -> list[str]:
    issues: list[str] = []
    try:
        extract_maintainers(owners_aliases)
    except ValueError as error:
        return [str(error)]

    if not has_exact_shared_anchor(mergify, "design_doc_area", DESIGN_DOC_AREA_MATCHER):
        issues.append("The Mergify Design Doc area matcher must use the canonical path")
    if not has_exact_shared_anchor(mergify, "governance_area", GOVERNANCE_AREA_MATCHER):
        issues.append(
            "The Mergify governance area matcher must cover every enforcement file"
        )

    if re.search(r"approved-reviews-by|approved_by_", mergify):
        issues.append(
            "Mergify must use Prow approval labels instead of GitHub review Approver "
            "conditions"
        )

    try:
        merge_protections = merge_protections_section(mergify)
    except ValueError as error:
        issues.append(str(error))
        return issues

    try:
        general_rule = configuration_rule(
            merge_protections,
            "Review / Prow approval",
        )
        if if_condition_lines(general_rule) != MASTER_ONLY_IF_LINES:
            issues.append(
                "The general review rule must apply exactly to master during "
                "the staged rollout"
            )
        if success_condition_lines(general_rule) != GENERAL_REVIEW_SUCCESS_LINES:
            issues.append(
                "The general review rule must require the Prow approved label and "
                "exact trusted Design Doc Policy success"
            )
    except ValueError as error:
        issues.append(str(error))

    try:
        design_rule = configuration_rule(
            merge_protections,
            "Review / formal Design Doc",
        )
        if if_condition_lines(design_rule) != (
            *MASTER_ONLY_IF_LINES,
            "      - *design_doc_area",
        ):
            issues.append(
                "The Design Doc approval-label rule does not cover the formal "
                "Design Doc area"
            )
        if success_condition_lines(design_rule) != DESIGN_DOC_REVIEW_SUCCESS_LINES:
            issues.append(
                "Formal Design Doc changes do not require exactly the "
                f"{DESIGN_DOC_APPROVAL_LABEL} label"
            )
    except ValueError as error:
        issues.append(str(error))

    try:
        governance_review_rule = configuration_rule(
            merge_protections,
            "Review / governance enforcement",
        )
        if if_condition_lines(governance_review_rule) != (
            *MASTER_ONLY_IF_LINES,
            "      - *governance_area",
        ):
            issues.append(
                "Governance enforcement changes do not require the trusted policy "
                "check"
            )
        if (
            success_condition_lines(governance_review_rule)
            != POLICY_CHECK_SUCCESS_LINES
        ):
            issues.append(
                "Governance enforcement changes do not require exact Design Doc "
                "Policy success"
            )
    except ValueError as error:
        issues.append(str(error))

    try:
        governance_policy_rule = configuration_rule(
            merge_protections,
            "Docs / repository governance policy",
        )
        if if_condition_lines(governance_policy_rule) != (
            *MASTER_ONLY_IF_LINES,
            "      - *governance_area",
        ):
            issues.append("Governance changes do not require the trusted policy check")
        if (
            success_condition_lines(governance_policy_rule)
            != POLICY_CHECK_SUCCESS_LINES
        ):
            issues.append(
                "Governance changes do not require exact Design Doc Policy success"
            )
    except ValueError as error:
        issues.append(str(error))

    try:
        design_policy_rule = configuration_rule(
            merge_protections,
            "Docs / formal Design Doc policy",
        )
        if if_condition_lines(design_policy_rule) != (
            *MASTER_ONLY_IF_LINES,
            "      - *design_doc_area",
        ):
            issues.append("Design Doc changes do not require the trusted policy check")
        if success_condition_lines(design_policy_rule) != POLICY_CHECK_SUCCESS_LINES:
            issues.append(
                "Design Doc changes do not require exact Design Doc Policy success"
            )
    except ValueError as error:
        issues.append(str(error))

    try:
        feature_policy_rule = configuration_rule(
            merge_protections,
            "Docs / feature Design Doc policy",
        )
        if if_condition_lines(feature_policy_rule) != FEATURE_POLICY_IF_LINES:
            issues.append(
                "Feature policy trigger must be exactly feat: title or "
                "kind/feature label"
            )
        if success_condition_lines(feature_policy_rule) != FEATURE_POLICY_SUCCESS_LINES:
            issues.append(
                "Feature changes do not require the native Design Doc requirement "
                "and exact Design Doc Policy success"
            )
    except ValueError as error:
        issues.append(str(error))

    return issues


def validate_changed_governance(
    client: GitHubClient,
    head_repository: str,
    head_sha: str,
    files: list[dict[str, Any]],
) -> list[str]:
    if not governance_enforcement_changed(files):
        return []
    try:
        owners_aliases = client.get_repository_file(
            head_repository, OWNERS_ALIASES_PATH, head_sha
        )
        mergify = client.get_repository_file(
            head_repository, MERGIFY_CONFIG_PATH, head_sha
        )
    except RuntimeError as error:
        return [str(error)]
    return validate_approver_governance(owners_aliases, mergify)


def validate_changed_design_docs(
    client: GitHubClient,
    repository: str,
    files: list[dict[str, Any]],
) -> dict[str, list[str]]:
    issues: dict[str, list[str]] = {}
    for file_info in select_design_doc_files(files):
        filename = file_info["filename"]
        blob_sha = file_info.get("sha")
        if not isinstance(blob_sha, str) or not blob_sha:
            issues[filename] = ["The changed file content could not be located."]
            continue

        try:
            document = decode_blob(client.get_blob(repository, blob_sha))
        except ValueError as error:
            issues[filename] = [str(error)]
            continue

        file_issues = validate_header(document)
        if file_issues:
            issues[filename] = file_issues
    return issues


def build_comment(
    issues: dict[str, list[str]],
    feature_requirement_issue: str | None = None,
    governance_issues: list[str] | None = None,
) -> str:
    governance_issues = governance_issues or []
    intro = [
        COMMENT_MARKER,
        "## Design document policy check",
        "",
    ]
    if feature_requirement_issue is not None:
        intro.extend(
            [
                "### Feature design document requirement",
                "",
                feature_requirement_issue,
                "",
            ]
        )
    if governance_issues:
        intro.extend(
            [
                "### Repository governance enforcement",
                "",
                "The proposed governance files do not preserve the trusted "
                "Approver policy:",
                "",
                *[f"- {html.escape(issue)}" for issue in governance_issues],
                "",
            ]
        )
    if issues:
        intro.extend(
            [
                "### Header validation",
                "",
                "The following changed design documents have missing or invalid "
                "required header fields:",
                "",
            ]
        )

    blocks: list[list[str]] = []
    for filename in sorted(issues):
        visible_filename = json.dumps(filename, ensure_ascii=False)[1:-1]
        safe_filename = html.escape(visible_filename, quote=True)
        block = [f"- <code>{safe_filename}</code>"]
        for issue in issues[filename]:
            block.append(f"  - {issue}")
        blocks.append(block)

    tail = [""]
    if issues:
        tail.extend(
            [
                "Place each required field within the first 50 lines and before "
                "the first section heading. Fields inside code fences, raw or hidden "
                "HTML, or HTML comments do not count.",
                "",
            ]
        )
    tail.extend(
        [
            "See `CONTRIBUTING.md` and `docs/design-docs/README.md` for the "
            "formal Design Doc policy.",
            "",
            "This reminder will be removed automatically after the pull request "
            "complies.",
        ]
    )

    full_comment = "\n".join(
        intro + [line for block in blocks for line in block] + tail
    )
    if len(full_comment) <= MAX_COMMENT_CHARS:
        return full_comment

    included: list[list[str]] = []
    for block in blocks:
        proposed = included + [block]
        omitted = len(blocks) - len(proposed)
        omission = [
            f"- ... {omitted} additional design document(s) omitted from this "
            "comment; fix the listed files, then rerun the check to see more."
        ]
        candidate = "\n".join(
            intro + [line for item in proposed for line in item] + omission + tail
        )
        if len(candidate) > MAX_COMMENT_CHARS:
            break
        included = proposed

    omitted = len(blocks) - len(included)
    omission = [
        f"- ... {omitted} additional design document(s) omitted from this "
        "comment; fix the listed files, then rerun the check to see more."
    ]
    return "\n".join(
        intro + [line for block in included for line in block] + omission + tail
    )


def build_check_summary(
    issues: dict[str, list[str]],
    feature_requirement_issue: str | None,
    governance_issues: list[str] | None = None,
    approval: ApprovalRequirement | None = None,
) -> str:
    governance_issues = governance_issues or []
    lines = ["## Design Doc policy"]
    if approval is not None:
        approval_result = "passed" if approval.satisfied else "failed"
        approvers = (
            ", ".join(f"@{login}" for login in approval.approvers)
            if approval.approvers
            else "none"
        )
        lines.extend(
            [
                f"- Non-author Approver requirement: {approval_result} "
                f"({len(approval.approvers)}/{approval.required})",
                f"  - Current valid Approvers: {approvers}",
            ]
        )
    if feature_requirement_issue is None:
        lines.append("- Feature Design Doc requirement: passed or not applicable")
    else:
        lines.extend(
            [
                "- Feature Design Doc requirement: failed",
                f"  - {feature_requirement_issue}",
            ]
        )

    if governance_issues:
        lines.append("- Repository governance enforcement: failed")
        for issue in governance_issues:
            lines.append(f"  - {issue}")
    else:
        lines.append("- Repository governance enforcement: passed or not applicable")

    if not issues:
        lines.append("- Changed Design Doc headers: passed or not applicable")
    else:
        lines.append(
            f"- Changed Design Doc headers: reminders posted for {len(issues)} file(s)"
        )
        for filename in sorted(issues)[:50]:
            safe_filename = filename.replace("`", "\\`")
            lines.append(f"  - `{safe_filename}`")
        if len(issues) > 50:
            lines.append(f"  - ... and {len(issues) - 50} more")

    return "\n".join(lines)


def matching_bot_comments(
    comments: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    matches = []
    for comment in comments:
        user = comment.get("user")
        if (
            isinstance(user, dict)
            and user.get("login") == BOT_LOGIN
            and str(comment.get("body", "")).startswith(COMMENT_PREFIX)
            and isinstance(comment.get("id"), int)
        ):
            matches.append(comment)
    return sorted(matches, key=lambda comment: comment["id"])


def sync_comment(
    client: GitHubClient,
    repository: str,
    pull_number: int,
    body: str | None,
    existing_comments: list[dict[str, Any]] | None = None,
) -> None:
    all_comments = (
        existing_comments
        if existing_comments is not None
        else client.list_issue_comments(repository, pull_number)
    )
    comments = matching_bot_comments(all_comments)
    if body is None:
        for comment in comments:
            client.delete_comment(repository, comment["id"])
        return

    if comments:
        first = comments[0]
        if first.get("body") != body:
            client.update_comment(repository, first["id"], body)
        for duplicate in comments[1:]:
            client.delete_comment(repository, duplicate["id"])
        return

    client.create_comment(repository, pull_number, body)


def load_event(
    path: str,
) -> tuple[str, int]:
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
    except (KeyError, TypeError, ValueError) as error:
        raise RuntimeError("The workflow event is not a valid pull request") from error
    return repository, pull_number


def complete_stale_policy_check(
    client: GitHubClient,
    repository: str,
    check_run_id: int,
) -> None:
    client.complete_policy_check(
        repository,
        check_run_id,
        "neutral",
        "Design Doc policy evaluation became stale",
        "The pull request changed while this evaluation was running. "
        "A newer workflow run will evaluate the current head.",
    )


def run(event_path: str) -> int:
    token = os.environ.get("GH_TOKEN") or os.environ.get("GITHUB_TOKEN")
    if not token:
        raise RuntimeError("GH_TOKEN or GITHUB_TOKEN is required")

    repository, pull_number = load_event(event_path)
    client = GitHubClient(
        token=token,
        api_url=os.environ.get("GITHUB_API_URL", "https://api.github.com"),
    )
    initial_state = client.get_pull_request_state(repository, pull_number)
    check_run_id = client.create_policy_check(
        repository, initial_state.head_sha, pull_number
    )

    try:
        files = client.list_pull_request_files(repository, pull_number)
        issues = validate_changed_design_docs(client, repository, files)
        governance_issues = validate_changed_governance(
            client,
            initial_state.head_repository,
            initial_state.head_sha,
            files,
        )
        feature_requirement_issue = validate_feature_design_doc_requirement(
            client,
            repository,
            initial_state.base_sha,
            initial_state.head_repository,
            initial_state.head_sha,
            initial_state.title,
            list(initial_state.labels),
            initial_state.body,
            files,
        )

        existing_comments = client.list_issue_comments(repository, pull_number)
        review_comments = client.list_pull_request_review_comments(
            repository, pull_number
        )
        reviews = client.list_pull_request_reviews(repository, pull_number)
        approval = evaluate_approval_requirement(
            client,
            initial_state,
            files,
            existing_comments,
            review_comments,
            reviews,
        )
        current_state = client.get_pull_request_state(repository, pull_number)
        if stable_pull_request_state(current_state) != stable_pull_request_state(
            initial_state
        ):
            complete_stale_policy_check(client, repository, check_run_id)
            print(
                "The pull request changed while this evaluation was running; "
                "skipping stale comment update."
            )
            return 0

        sync_design_doc_approval_label(
            client,
            repository,
            pull_number,
            current_state.labels,
            approval,
        )

        body = (
            build_comment(
                issues,
                feature_requirement_issue,
                governance_issues,
            )
            if (issues or feature_requirement_issue is not None or governance_issues)
            else None
        )
        sync_comment(
            client,
            repository,
            pull_number,
            body,
            existing_comments=existing_comments,
        )

        final_state = client.get_pull_request_state(repository, pull_number)
        if stable_pull_request_state(final_state) != stable_pull_request_state(
            initial_state
        ):
            complete_stale_policy_check(client, repository, check_run_id)
            print(
                "The pull request changed while the policy comment was being "
                "updated; skipping stale check completion."
            )
            return 0

        final_approval = evaluate_approval_requirement(
            client,
            final_state,
            files,
            client.list_issue_comments(repository, pull_number),
            client.list_pull_request_review_comments(repository, pull_number),
            client.list_pull_request_reviews(repository, pull_number),
        )
        if final_approval != approval:
            if approval.formal_design_doc or final_approval.formal_design_doc:
                # The label may have been synchronized from an approval snapshot
                # that changed while this run was active. Remove it fail-closed;
                # the queued event for the new snapshot will restore it if valid.
                client.remove_pull_request_label(
                    repository, pull_number, DESIGN_DOC_APPROVAL_LABEL
                )
            complete_stale_policy_check(client, repository, check_run_id)
            print(
                "The approval timeline changed while this evaluation was running; "
                "a newer workflow run will evaluate it."
            )
            return 0

        failed = (
            not approval.satisfied
            or feature_requirement_issue is not None
            or bool(governance_issues)
        )
        if not approval.satisfied:
            check_title = "Required non-author approval is missing"
        elif failed:
            check_title = "Design Doc policy failed"
        elif issues:
            check_title = "Design Doc policy passed with header reminders"
        else:
            check_title = "Design Doc policy passed"
        client.complete_policy_check(
            repository,
            check_run_id,
            "failure" if failed else "success",
            check_title,
            build_check_summary(
                issues,
                feature_requirement_issue,
                governance_issues,
                approval,
            ),
        )

        if issues:
            print(f"Posted header reminders for {len(issues)} design document(s).")
        else:
            print("All changed design document headers are valid.")
        if feature_requirement_issue is not None:
            print("The feature design document requirement is not satisfied.")
        else:
            print(
                "The feature design document requirement is satisfied or does "
                "not apply."
            )
        if governance_issues:
            print("Repository governance enforcement is inconsistent.")
        else:
            print("Repository governance enforcement is valid or does not apply.")
        if approval.satisfied:
            print(
                f"The non-author Approver requirement is satisfied "
                f"({len(approval.approvers)}/{approval.required})."
            )
        else:
            print(
                f"The non-author Approver requirement is not satisfied "
                f"({len(approval.approvers)}/{approval.required})."
            )
        return 1 if failed else 0
    except Exception as error:
        try:
            client.complete_policy_check(
                repository,
                check_run_id,
                "failure",
                "Design Doc policy could not be evaluated",
                f"The trusted policy workflow failed: `{html.escape(str(error))}`",
            )
        except Exception as completion_error:
            print(
                f"Could not publish the failed policy check: {completion_error}",
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
        print(f"Design document policy check failed: {error}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())

#!/usr/bin/env python3
"""Enforce the Design Doc policy and maintain one PR reminder."""

import argparse
import base64
import binascii
import datetime
import html
import itertools
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
DESIGN_DOC_AREA_MATCHER = "files~=^docs/design-docs/design_docs/"
GOVERNANCE_AREA_MATCHER = (
    r"files~=^(OWNERS_ALIASES|\.github/mergify\.yml|"
    r"\.github/workflows/design-doc-policy\.yml|"
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
AUTHOR_EXCLUSION_LINE = "      - '-approved-reviews-by = {{ author }}'"
FEATURE_POLICY_IF_LINES = (
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
    title: str
    body: str
    labels: tuple[str, ...]
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

    def get_pull_request_state(
        self, repository: str, pull_number: int
    ) -> PullRequestState:
        repository_path = urllib.parse.quote(repository, safe="/")
        pull_request = self.request(
            "GET", f"/repos/{repository_path}/pulls/{pull_number}"
        )
        try:
            head_repository_info = pull_request["head"].get("repo")
            head_repository = (
                str(head_repository_info["full_name"])
                if isinstance(head_repository_info, dict)
                else repository
            )
            labels = tuple(
                sorted(
                    str(label["name"])
                    for label in pull_request.get("labels", [])
                    if isinstance(label, dict) and "name" in label
                )
            )
            return PullRequestState(
                head_sha=str(pull_request["head"]["sha"]),
                base_sha=str(pull_request["base"]["sha"]),
                head_repository=head_repository,
                title=str(pull_request.get("title", "")),
                body=str(pull_request.get("body") or ""),
                labels=labels,
            )
        except (KeyError, TypeError) as error:
            raise RuntimeError(
                "GitHub returned an invalid pull-request response"
            ) from error

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
                            "summary": "Validating feature documentation, repository "
                            "governance, and changed Design Doc headers.",
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
                    "summary": "Validating feature documentation, repository "
                    "governance, and changed Design Doc headers.",
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
        check_run_ids = self._policy_check_groups.pop(
            check_run_id, (check_run_id,)
        )
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
    if len(set(maintainers)) != len(maintainers):
        raise ValueError("OWNERS_ALIASES contains duplicate maintainers")
    return maintainers


def merge_protections_section(mergify: str) -> str:
    markers = list(
        re.finditer(r"^merge_protections:$", mergify, re.MULTILINE)
    )
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
    key_definitions = [
        line for line in mergify.splitlines() if key_pattern.match(line)
    ]
    anchor_definitions = [
        line for line in mergify.splitlines() if anchor_pattern.search(line)
    ]
    return key_definitions == [expected_line] and anchor_definitions == [expected_line]


def rule_block_lines(rule: str, declaration: str) -> tuple[str, ...] | None:
    lines = rule.splitlines()
    markers = [
        index
        for index, line in enumerate(lines)
        if line == declaration
    ]
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
        maintainers = extract_maintainers(owners_aliases)
    except ValueError as error:
        return [str(error)]

    if not has_exact_shared_anchor(
        mergify, "design_doc_area", DESIGN_DOC_AREA_MATCHER
    ):
        issues.append(
            "The Mergify Design Doc area matcher must use the canonical path"
        )
    if not has_exact_shared_anchor(
        mergify, "governance_area", GOVERNANCE_AREA_MATCHER
    ):
        issues.append(
            "The Mergify governance area matcher must cover every enforcement file"
        )

    anchor_matches = re.findall(
        r"^  (approved_by_[a-z0-9_]+): &(approved_by_[a-z0-9_]+) "
        r"'approved-reviews-by = ([A-Za-z0-9-]+)'$",
        mergify,
        re.MULTILINE,
    )
    if any(key != anchor for key, anchor, _ in anchor_matches):
        issues.append("Mergify Approver anchor keys and anchor names must match")
    anchor_names = [anchor for _, anchor, _ in anchor_matches]
    anchor_logins = [login for _, _, login in anchor_matches]
    if len(set(anchor_names)) != len(anchor_names) or len(set(anchor_logins)) != len(
        anchor_logins
    ):
        issues.append("Mergify contains duplicate Approver anchors or logins")
    anchors = dict(zip(anchor_names, anchor_logins))
    if set(maintainers) != set(anchor_logins):
        issues.append(
            "Mergify Approver anchors do not match OWNERS_ALIASES maintainers"
        )

    try:
        merge_protections = merge_protections_section(mergify)
    except ValueError as error:
        issues.append(str(error))
        return issues

    try:
        general_rule = configuration_rule(
            merge_protections,
            "Review / non-author Approver",
        )
        if not has_exact_rule_scalar(general_rule, "if", "true"):
            issues.append("The general review rule must always apply")

        general_conditions = success_condition_lines(general_rule)
        general_anchor_names: list[str] = []
        if general_conditions is not None:
            for line in general_conditions[1:-1]:
                match = re.fullmatch(
                    r"          - \*(approved_by_[a-z0-9_]+)", line
                )
                if match is not None:
                    general_anchor_names.append(match.group(1))
        general_anchor_structure_valid = (
            general_conditions is not None
            and len(general_conditions) == len(anchors) + 2
            and general_conditions[0] == "      - or:"
            and len(general_anchor_names) == len(anchors)
            and set(general_anchor_names) == set(anchors)
        )
        if not general_anchor_structure_valid:
            issues.append("The general review rule does not include every Approver")
        if not (
            general_conditions is not None
            and general_conditions.count(AUTHOR_EXCLUSION_LINE) == 1
            and general_conditions[-1] == AUTHOR_EXCLUSION_LINE
        ):
            issues.append("The general review rule does not exclude the PR author")
    except ValueError as error:
        issues.append(str(error))

    try:
        design_rule = configuration_rule(
            merge_protections,
            "Review / formal Design Doc",
        )
        if if_condition_lines(design_rule) != ("      - *design_doc_area",):
            issues.append("The two-Approver rule does not cover the Design Doc area")

        design_conditions = rule_block_lines(
            design_rule,
            "    success_conditions: &TWO_APPROVER_SUCCESS_CONDITIONS",
        )
        has_author_exclusion = (
            design_conditions is not None
            and design_conditions.count(AUTHOR_EXCLUSION_LINE) == 1
            and design_conditions[-1] == AUTHOR_EXCLUSION_LINE
        )
        if not has_author_exclusion:
            issues.append("The two-Approver rule does not exclude the PR author")

        pair_anchor_names: list[tuple[str, str]] = []
        matrix_structure_valid = (
            design_conditions is not None
            and len(design_conditions) > 1
            and design_conditions[0] == "      - or:"
        )
        if design_conditions is None:
            matrix_lines: tuple[str, ...] = ()
        elif has_author_exclusion:
            matrix_lines = design_conditions[1:-1]
        else:
            matrix_lines = design_conditions[1:]

        if len(matrix_lines) % 3 != 0:
            matrix_structure_valid = False
        else:
            for index in range(0, len(matrix_lines), 3):
                and_line, first_line, second_line = matrix_lines[index : index + 3]
                first_match = re.fullmatch(
                    r"              - \*(approved_by_[a-z0-9_]+)", first_line
                )
                second_match = re.fullmatch(
                    r"              - \*(approved_by_[a-z0-9_]+)", second_line
                )
                if (
                    and_line != "          - and:"
                    or first_match is None
                    or second_match is None
                ):
                    matrix_structure_valid = False
                    continue
                pair_anchor_names.append(
                    (first_match.group(1), second_match.group(1))
                )

        unknown_pair_anchors = {
            anchor
            for pair in pair_anchor_names
            for anchor in pair
            if anchor not in anchors
        }
        if unknown_pair_anchors:
            issues.append("The two-Approver matrix references unknown Approvers")
        else:
            actual_pairs = {
                frozenset((anchors[first], anchors[second]))
                for first, second in pair_anchor_names
            }
            expected_pairs = {
                frozenset(pair) for pair in itertools.combinations(maintainers, 2)
            }
            if (
                not matrix_structure_valid
                or actual_pairs != expected_pairs
                or len(pair_anchor_names) != len(expected_pairs)
            ):
                issues.append(
                    "The two-Approver matrix is not the complete set of distinct "
                    "maintainer pairs"
                )
    except ValueError as error:
        issues.append(str(error))

    try:
        governance_review_rule = configuration_rule(
            merge_protections,
            "Review / governance enforcement",
        )
        if if_condition_lines(governance_review_rule) != (
            "      - *governance_area",
        ):
            issues.append("Governance enforcement changes do not require two Approvers")
        if not has_exact_rule_scalar(
            governance_review_rule,
            "success_conditions",
            "*TWO_APPROVER_SUCCESS_CONDITIONS",
        ):
            issues.append(
                "Governance enforcement changes do not reuse the two-Approver gate"
            )
    except ValueError as error:
        issues.append(str(error))

    try:
        governance_policy_rule = configuration_rule(
            merge_protections,
            "Docs / repository governance policy",
        )
        if if_condition_lines(governance_policy_rule) != (
            "      - *governance_area",
        ):
            issues.append("Governance changes do not require the trusted policy check")
        if success_condition_lines(
            governance_policy_rule
        ) != POLICY_CHECK_SUCCESS_LINES:
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
                *[
                    f"- {html.escape(issue)}" for issue in governance_issues
                ],
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
) -> str:
    governance_issues = governance_issues or []
    lines = ["## Design Doc policy"]
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
        pull_request = event["pull_request"]
        pull_number = int(pull_request["number"])
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
        current_state = client.get_pull_request_state(repository, pull_number)
        if current_state != initial_state:
            complete_stale_policy_check(client, repository, check_run_id)
            print(
                "The pull request changed while this evaluation was running; "
                "skipping stale comment update."
            )
            return 0

        body = (
            build_comment(issues, feature_requirement_issue, governance_issues)
            if issues or feature_requirement_issue is not None or governance_issues
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
        if final_state != initial_state:
            complete_stale_policy_check(client, repository, check_run_id)
            print(
                "The pull request changed while the policy comment was being "
                "updated; skipping stale check completion."
            )
            return 0

        failed = feature_requirement_issue is not None or bool(governance_issues)
        if failed:
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
                issues, feature_requirement_issue, governance_issues
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

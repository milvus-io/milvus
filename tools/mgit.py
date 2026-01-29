#!/usr/bin/env python3
"""
mgit - Intelligent Git Workflow Tool for Milvus

A smart Git workflow automation tool that:
- Generates AI-powered commit messages following Milvus conventions
- Automates fork → issue → PR → cherry-pick workflow
- Syncs with master and squashes commits for clean history
- Ensures DCO compliance
- Cherry-picks PRs to release branches with proper formatting

Usage:
    mgit.py --commit       # Smart commit workflow
    mgit.py --rebase       # Rebase onto master and squash commits
    mgit.py --pr           # PR creation workflow
    mgit.py --search       # Search GitHub issues and PRs by keyword
    mgit.py --cherry-pick  # Cherry-pick a merged PR to release branch
    mgit.py --backport     # Backport current branch to release branch with auto-rebase
    mgit.py --all          # Complete workflow (rebase + commit + PR)
    mgit.py                # Same as --all
"""

import subprocess
import sys
import os
import json
import argparse
import re
import time
import shutil
from typing import Dict, List, Tuple, Optional
from dataclasses import dataclass
import urllib.request
import urllib.error
import uuid


def create_local_temp_file(content: str, suffix: str = ".txt") -> str:
    """Create a temporary file in the current directory.

    Args:
        content: Content to write to the file
        suffix: File suffix (e.g., '.txt', '.md')

    Returns:
        Path to the created temp file
    """
    filename = f".mgit_temp_{uuid.uuid4().hex[:8]}{suffix}"
    with open(filename, "w", encoding="utf-8") as f:
        f.write(content)
    return filename


def remove_local_temp_file(filepath: str):
    """Remove a local temp file if it exists."""
    if os.path.exists(filepath):
        os.remove(filepath)


# ANSI colors for terminal output
class Colors:
    GREEN = "\033[92m"
    YELLOW = "\033[93m"
    RED = "\033[91m"
    BLUE = "\033[94m"
    RESET = "\033[0m"
    BOLD = "\033[1m"


def print_success(msg: str):
    print(f"{Colors.GREEN}✓ {msg}{Colors.RESET}")


def print_error(msg: str):
    print(f"{Colors.RED}✗ {msg}{Colors.RESET}")


def print_warning(msg: str):
    print(f"{Colors.YELLOW}⚠ {msg}{Colors.RESET}")


def print_info(msg: str):
    print(f"{Colors.BLUE}ℹ {msg}{Colors.RESET}")


def print_header(msg: str):
    print(f"\n{Colors.BOLD}{msg}{Colors.RESET}")


# ============================================================================
# Git Module - Git operations
# ============================================================================


class GitOperations:
    """Handles all git command operations"""

    @staticmethod
    def run_command(
        command: List[str], capture_output: bool = True, check: bool = True
    ) -> str:
        """Execute a git command and return output"""
        try:
            result = subprocess.run(
                command,
                check=check,
                text=True,
                stdout=subprocess.PIPE if capture_output else None,
                stderr=subprocess.PIPE if capture_output else None,
            )
            return result.stdout.rstrip() if capture_output else ""
        except subprocess.CalledProcessError as e:
            if check:
                raise Exception(f"Command failed: {' '.join(command)}\n{e.stderr}")
            return ""

    @staticmethod
    def get_status() -> Tuple[List[str], List[str]]:
        """
        Get git status
        Returns: (staged_files, unstaged_files)
        """
        output = GitOperations.run_command(["git", "status", "--porcelain"])
        staged = []
        unstaged = []

        for line in output.splitlines():
            if not line:
                continue
            status = line[:2]
            filepath = line[3:]

            # Handle rename/copy: "R  old -> new" or "C  old -> new"
            # We want the new path for git operations
            if " -> " in filepath:
                filepath = filepath.split(" -> ", 1)[1]

            # Staged changes (first character is not space)
            if status[0] != " " and status[0] != "?":
                staged.append(filepath)
            # Unstaged changes
            if status[1] != " " or status[0] == "?":
                unstaged.append(filepath)

        return staged, unstaged

    @staticmethod
    def get_staged_diff() -> str:
        """Get diff of staged changes"""
        return GitOperations.run_command(["git", "diff", "--staged"])

    @staticmethod
    def get_staged_diff_stat() -> str:
        """Get diff statistics"""
        return GitOperations.run_command(["git", "diff", "--staged", "--stat"])

    @staticmethod
    def stage_files(files: List[str]):
        """Stage specified files"""
        GitOperations.run_command(["git", "add"] + files)

    @staticmethod
    def stage_all():
        """Stage all changes"""
        GitOperations.run_command(["git", "add", "-A"])

    @staticmethod
    def commit(message: str):
        """Create commit with DCO signature"""
        # Write message to local temporary file
        msg_file = create_local_temp_file(message, ".txt")

        try:
            # Commit with -s flag (auto-adds DCO)
            GitOperations.run_command(["git", "commit", "-s", "-F", msg_file])
        finally:
            # Clean up
            remove_local_temp_file(msg_file)

    @staticmethod
    def get_commit_hash(ref: str = "HEAD") -> str:
        """Get commit hash"""
        return GitOperations.run_command(["git", "rev-parse", "--short", ref])

    @staticmethod
    def get_current_branch() -> str:
        """Get current branch name"""
        return GitOperations.run_command(["git", "rev-parse", "--abbrev-ref", "HEAD"])

    @staticmethod
    def get_last_commit_message() -> str:
        """Get last commit message"""
        return GitOperations.run_command(["git", "log", "-1", "--pretty=%B"])

    @staticmethod
    def get_commit_count(base_branch: str = "master") -> int:
        """Get number of commits ahead of base branch (uses upstream remote)"""
        upstream_master = GitOperations.get_upstream_master()
        return GitOperations.get_commit_count_from_ref(upstream_master)

    @staticmethod
    def get_commit_count_from_ref(ref: str) -> int:
        """Get number of commits ahead of a specific ref (e.g., 'upstream/master')"""
        try:
            count = GitOperations.run_command(
                ["git", "rev-list", "--count", f"{ref}..HEAD"], check=False
            )
            return int(count) if count else 0
        except Exception:
            return 0

    @staticmethod
    def push(branch: str, force: bool = False, remote: str = None):
        """Push branch to remote (auto-detects fork if not specified)"""
        if remote is None:
            remote = GitOperations.get_fork_remote()
        cmd = ["git", "push", remote, branch]
        if force:
            cmd.append("-f")
        GitOperations.run_command(cmd)

    @staticmethod
    def get_fork_remote() -> str:
        """
        Auto-detect the remote that points to user's fork.
        Returns remote name (e.g., 'origin', 'fork')
        """
        # Get current GitHub username
        try:
            username = GitOperations.run_command(["gh", "api", "user", "-q", ".login"])
        except Exception:
            username = None

        # Get all remotes
        remotes_output = GitOperations.run_command(["git", "remote", "-v"])
        remotes = {}
        for line in remotes_output.splitlines():
            parts = line.split()
            if len(parts) >= 2 and "(push)" in line:
                name = parts[0]
                url = parts[1]
                remotes[name] = url

        # Find remote that matches user's fork
        for name, url in remotes.items():
            # Check if URL contains username (github.com/username/ or github.com:username/)
            if username and username.lower() in url.lower():
                return name
            # Check for common fork remote names
            if name in ["fork", "myfork", "personal"]:
                return name

        # If origin points to milvus-io/milvus, look for another remote
        origin_url = remotes.get("origin", "")
        if "milvus-io/milvus" in origin_url:
            # Origin is upstream, find fork
            for name, url in remotes.items():
                if name != "origin" and "milvus" in url.lower():
                    return name

        # Default to origin
        return "origin"

    @staticmethod
    def get_fork_info() -> Tuple[str, str]:
        """
        Get fork remote name and owner
        Returns: (remote_name, owner_username)
        """
        remote = GitOperations.get_fork_remote()
        remote_url = GitOperations.run_command(["git", "remote", "get-url", remote])

        # Parse username from URL
        # SSH: git@github.com:username/repo.git
        # HTTPS: https://github.com/username/repo.git
        if "github.com:" in remote_url:
            # SSH format
            owner = remote_url.split("github.com:")[1].split("/")[0]
        elif "github.com/" in remote_url:
            # HTTPS format
            owner = remote_url.split("github.com/")[1].split("/")[0]
        else:
            # Fallback: try gh api
            owner = GitOperations.run_command(["gh", "api", "user", "-q", ".login"])

        return remote, owner

    @staticmethod
    def get_user_info() -> Tuple[str, str]:
        """Get git user name and email"""
        name = GitOperations.run_command(["git", "config", "user.name"])
        email = GitOperations.run_command(["git", "config", "user.email"])
        return name, email

    @staticmethod
    def create_branch(branch_name: str):
        """Create and checkout a new branch"""
        GitOperations.run_command(["git", "checkout", "-b", branch_name])

    @staticmethod
    def branch_exists(branch_name: str) -> bool:
        """Check if a branch exists locally"""
        result = GitOperations.run_command(
            ["git", "rev-parse", "--verify", branch_name], check=False
        )
        return bool(result)

    @staticmethod
    def fetch(remote: str = "origin", branch: str = None):
        """Fetch from remote"""
        cmd = ["git", "fetch", remote]
        if branch:
            cmd.append(branch)
        GitOperations.run_command(cmd)

    @staticmethod
    def get_upstream_remote() -> str:
        """
        Find the remote that points to milvus-io/milvus (upstream).
        Returns remote name (e.g., 'upstream', 'origin').
        Always rebase against the official repo, not a potentially stale fork.
        """
        remotes_output = GitOperations.run_command(["git", "remote", "-v"])

        # First, look for a remote pointing to milvus-io/milvus
        for line in remotes_output.splitlines():
            if "milvus-io/milvus" in line and "(fetch)" in line:
                return line.split()[0]

        # If not found, check common upstream remote names
        remotes = [
            line.split()[0] for line in remotes_output.splitlines() if "(fetch)" in line
        ]
        for name in ["upstream", "milvus", "official"]:
            if name in remotes:
                return name

        # Fallback to origin (user should configure upstream properly)
        print_warning("No upstream remote found pointing to milvus-io/milvus")
        print_info(
            "Consider adding: git remote add upstream git@github.com:milvus-io/milvus.git"
        )
        return "origin"

    @staticmethod
    def get_upstream_master() -> str:
        """Get the upstream master reference (e.g., 'upstream/master')"""
        upstream = GitOperations.get_upstream_remote()
        return f"{upstream}/master"

    @staticmethod
    def rebase(target: str) -> Tuple[bool, str]:
        """
        Rebase current branch onto target
        Returns: (success, error_message)
        """
        try:
            GitOperations.run_command(["git", "rebase", target])
            return True, ""
        except Exception as e:
            return False, str(e)

    @staticmethod
    def rebase_abort():
        """Abort an in-progress rebase"""
        GitOperations.run_command(["git", "rebase", "--abort"], check=False)

    @staticmethod
    def is_rebase_in_progress() -> bool:
        """Check if a rebase is in progress"""
        git_dir = GitOperations.run_command(["git", "rev-parse", "--git-dir"])
        return os.path.exists(os.path.join(git_dir, "rebase-merge")) or os.path.exists(
            os.path.join(git_dir, "rebase-apply")
        )

    @staticmethod
    def reset_soft(target: str):
        """Soft reset to target (keeps changes staged)"""
        GitOperations.run_command(["git", "reset", "--soft", target])

    @staticmethod
    def get_all_changes_diff(base: str = None) -> str:
        """Get diff of all changes from base (defaults to upstream master)"""
        if base is None:
            base = GitOperations.get_upstream_master()
        return GitOperations.run_command(["git", "diff", base])

    @staticmethod
    def get_all_changes_stat(base: str = None) -> str:
        """Get diff statistics from base (defaults to upstream master)"""
        if base is None:
            base = GitOperations.get_upstream_master()
        return GitOperations.run_command(["git", "diff", "--stat", base])

    @staticmethod
    def get_commit_messages(base: str = None) -> List[str]:
        """Get all commit messages from base to HEAD (defaults to upstream master)"""
        if base is None:
            base = GitOperations.get_upstream_master()
        output = GitOperations.run_command(
            ["git", "log", f"{base}..HEAD", "--pretty=%B", "--reverse"]
        )
        return [msg.strip() for msg in output.split("\n\n") if msg.strip()]

    @staticmethod
    def checkout_branch(branch: str, create: bool = False):
        """Checkout a branch"""
        cmd = ["git", "checkout"]
        if create:
            cmd.append("-b")
        cmd.append(branch)
        GitOperations.run_command(cmd)

    @staticmethod
    def checkout_remote_branch(remote: str, branch: str, local_name: str = None):
        """Checkout a remote branch to a local branch"""
        if local_name is None:
            local_name = branch
        GitOperations.run_command(
            ["git", "checkout", "-b", local_name, f"{remote}/{branch}"]
        )

    @staticmethod
    def cherry_pick(commit_sha: str) -> Tuple[bool, str, List[str]]:
        """
        Cherry-pick a commit

        Returns: (success, error_message, conflict_files)
        """
        try:
            GitOperations.run_command(["git", "cherry-pick", commit_sha])
            return True, "", []
        except Exception as e:
            error_msg = str(e)
            # Get list of conflicting files
            conflict_files = []
            try:
                status_output = GitOperations.run_command(
                    ["git", "status", "--porcelain"]
                )
                for line in status_output.splitlines():
                    if (
                        line.startswith("UU ")
                        or line.startswith("AA ")
                        or line.startswith("DD ")
                    ):
                        conflict_files.append(line[3:])
            except Exception:
                pass
            return False, error_msg, conflict_files

    @staticmethod
    def cherry_pick_abort():
        """Abort an in-progress cherry-pick"""
        GitOperations.run_command(["git", "cherry-pick", "--abort"], check=False)

    @staticmethod
    def cherry_pick_continue():
        """Continue cherry-pick after resolving conflicts"""
        GitOperations.run_command(["git", "cherry-pick", "--continue"])

    @staticmethod
    def is_cherry_pick_in_progress() -> bool:
        """Check if a cherry-pick is in progress"""
        git_dir = GitOperations.run_command(["git", "rev-parse", "--git-dir"])
        return os.path.exists(os.path.join(git_dir, "CHERRY_PICK_HEAD"))

    @staticmethod
    def get_conflict_diff(file_path: str) -> str:
        """Get the conflict markers content for a file"""
        try:
            with open(file_path, "r") as f:
                return f.read()
        except Exception:
            return ""

    @staticmethod
    def remote_branch_exists(remote: str, branch: str) -> bool:
        """Check if a remote branch exists"""
        try:
            output = GitOperations.run_command(
                ["git", "ls-remote", "--heads", remote, branch]
            )
            return bool(output.strip())
        except Exception:
            return False

    @staticmethod
    def delete_branch(branch: str, force: bool = False):
        """Delete a local branch"""
        cmd = ["git", "branch", "-D" if force else "-d", branch]
        GitOperations.run_command(cmd, check=False)


# ============================================================================
# AI Module - Claude and OpenAI API integration
# ============================================================================


class AIService:
    """Handles AI API calls for commit message and PR generation"""

    COMMIT_TYPES = ["fix", "enhance", "feat", "refactor", "test", "docs", "chore"]

    def __init__(self):
        # Check for local claude CLI first
        self.has_claude_cli = self._check_claude_cli()

        self.gemini_key = os.getenv("GEMINI_API_KEY")
        self.anthropic_key = os.getenv("ANTHROPIC_API_KEY")
        self.openai_key = os.getenv("OPENAI_API_KEY")

        # Has API key if any key is set OR local claude is available
        self.has_api_key = bool(
            self.has_claude_cli
            or self.gemini_key
            or self.anthropic_key
            or self.openai_key
        )

    @staticmethod
    def _check_claude_cli() -> bool:
        """Check if local claude CLI is available"""
        # Use shutil.which() for cross-platform compatibility (works on Windows too)
        return shutil.which("claude") is not None

    def generate_commit_message(
        self, diff: str, files: List[str], stats: str
    ) -> Dict[str, str]:
        """
        Generate commit message using AI

        Returns:
            {
                'type': 'fix|enhance|feat|...',
                'title': 'Short summary (≤80 chars)',
                'body': 'Optional detailed explanation'
            }
        """
        # Limit diff size to avoid API limits
        max_diff_lines = 10000
        diff_lines = diff.splitlines()
        if len(diff_lines) > max_diff_lines:
            print_warning(
                f"Diff has {len(diff_lines)} lines (>{max_diff_lines}). Consider splitting into smaller PRs."
            )
            truncated_diff = "\n".join(diff_lines[:max_diff_lines])
            truncated_diff += (
                f"\n\n... (truncated {len(diff_lines) - max_diff_lines} lines)"
            )
        else:
            truncated_diff = diff

        # Check if any AI source is available
        if not self.has_api_key:
            raise Exception(
                "No AI available. Please either:\n"
                "  - Install Claude Code CLI (https://claude.com/code), or\n"
                "  - Set one of: GEMINI_API_KEY, ANTHROPIC_API_KEY, OPENAI_API_KEY"
            )

        prompt = self._build_commit_prompt(truncated_diff, files, stats)

        # Try local Claude CLI first if available
        if self.has_claude_cli:
            try:
                return self._call_claude_cli(prompt)
            except Exception as e:
                print_warning(f"Claude CLI failed: {e}")
                if self.gemini_key or self.anthropic_key or self.openai_key:
                    print_info("Falling back to API providers...")
                else:
                    raise

        # Try Gemini API if available
        if self.gemini_key:
            try:
                return self._call_gemini(prompt)
            except Exception as e:
                print_warning(f"Gemini API failed: {e}")
                if self.anthropic_key or self.openai_key:
                    print_info("Falling back to other AI providers...")
                else:
                    raise

        # Fall back to Claude
        if self.anthropic_key:
            try:
                return self._call_claude(prompt)
            except Exception as e:
                print_warning(f"Claude API failed: {e}")
                if self.openai_key:
                    print_info("Falling back to OpenAI...")
                else:
                    raise

        # Fall back to OpenAI
        if self.openai_key:
            return self._call_openai(prompt)

        raise Exception("All AI API calls failed")

    def _build_commit_prompt(self, diff: str, files: List[str], stats: str) -> str:
        """Build prompt for commit message generation"""
        examples = """
Examples of good Milvus commits:
- fix: Fix missing handling of FlushAllMsg in recovery storage
- enhance: optimize jieba and lindera analyzer clone
- feat: Add semantic highlight
- refactor: Simplify go unit tests
- test: Add planparserv2 benchmarks
"""
        files_list = "\n".join(f"  - {f}" for f in files)

        return f"""You are a commit message generator for the Milvus vector database project.

Generate a commit message following these guidelines:

1. Format: <type>: <summary>
   Types: fix, enhance, feat, refactor, test, docs, chore
2. Title MUST be ≤80 characters
3. Title should describe WHAT changed and WHY
4. Use imperative mood (e.g., "Fix bug" not "Fixed bug")
5. Optional body for complex changes (multiple paragraphs OK)

{examples}

Changed Files:
{files_list}

Statistics: {stats}

Diff:
{diff}

Return ONLY a JSON object with this exact format:
{{"type": "fix", "title": "your title here", "body": "optional detailed explanation"}}

If the change is simple, set body to empty string.
Ensure title is concise and ≤80 characters.
"""

    def _call_claude_cli(self, prompt: str) -> Dict[str, str]:
        """Call local Claude Code CLI"""
        try:
            result = subprocess.run(
                ["claude", "-p", prompt], capture_output=True, text=True, timeout=60
            )

            if result.returncode != 0:
                raise Exception(f"Claude CLI returned error: {result.stderr}")

            content = result.stdout.strip()
            return self._parse_ai_response(content)

        except subprocess.TimeoutExpired:
            raise Exception("Claude CLI timed out (>60s)")
        except Exception as e:
            raise Exception(f"Claude CLI error: {e}")

    def _call_claude(self, prompt: str) -> Dict[str, str]:
        """Call Claude API"""
        url = "https://api.anthropic.com/v1/messages"

        data = {
            "model": "claude-3-5-sonnet-20241022",
            "max_tokens": 1024,
            "messages": [{"role": "user", "content": prompt}],
        }

        req = urllib.request.Request(
            url,
            data=json.dumps(data).encode("utf-8"),
            headers={
                "x-api-key": self.anthropic_key,
                "anthropic-version": "2023-06-01",
                "content-type": "application/json",
            },
        )

        try:
            with urllib.request.urlopen(req, timeout=30) as response:
                result = json.loads(response.read().decode("utf-8"))
                content = result["content"][0]["text"]
                return self._parse_ai_response(content)
        except urllib.error.HTTPError as e:
            error_body = e.read().decode("utf-8")
            raise Exception(f"Claude API HTTP {e.code}: {error_body}")
        except Exception as e:
            raise Exception(f"Claude API error: {e}")

    def _call_openai(self, prompt: str) -> Dict[str, str]:
        """Call OpenAI API"""
        url = "https://api.openai.com/v1/chat/completions"

        data = {
            "model": "gpt-4",
            "messages": [{"role": "user", "content": prompt}],
            "temperature": 0.7,
        }

        req = urllib.request.Request(
            url,
            data=json.dumps(data).encode("utf-8"),
            headers={
                "Authorization": f"Bearer {self.openai_key}",
                "Content-Type": "application/json",
            },
        )

        try:
            with urllib.request.urlopen(req, timeout=30) as response:
                result = json.loads(response.read().decode("utf-8"))
                content = result["choices"][0]["message"]["content"]
                return self._parse_ai_response(content)
        except urllib.error.HTTPError as e:
            error_body = e.read().decode("utf-8")
            raise Exception(f"OpenAI API HTTP {e.code}: {error_body}")
        except Exception as e:
            raise Exception(f"OpenAI API error: {e}")

    def _call_gemini(self, prompt: str) -> Dict[str, str]:
        """Call Google Gemini API"""
        url = f"https://generativelanguage.googleapis.com/v1beta/models/gemini-2.0-flash:generateContent?key={self.gemini_key}"

        data = {
            "contents": [{"parts": [{"text": prompt}]}],
            "generationConfig": {"temperature": 0.7, "maxOutputTokens": 2048},
        }

        req = urllib.request.Request(
            url,
            data=json.dumps(data).encode("utf-8"),
            headers={"Content-Type": "application/json"},
        )

        try:
            with urllib.request.urlopen(req, timeout=30) as response:
                result = json.loads(response.read().decode("utf-8"))
                content = result["candidates"][0]["content"]["parts"][0]["text"]
                return self._parse_ai_response(content)
        except urllib.error.HTTPError as e:
            error_body = e.read().decode("utf-8")
            raise Exception(f"Gemini API HTTP {e.code}: {error_body}")
        except Exception as e:
            raise Exception(f"Gemini API error: {e}")

    def _extract_json(self, content: str) -> str:
        """Extract JSON object from content that may contain extra text"""
        # Remove markdown code blocks
        content = content.replace("```json", "").replace("```", "").strip()

        # Try direct parse first
        try:
            json.loads(content)
            return content
        except Exception:
            pass

        # Find the first { and last } to extract JSON
        start = content.find("{")
        if start == -1:
            return content

        # Find matching closing brace
        depth = 0
        end = -1
        for i in range(start, len(content)):
            if content[i] == "{":
                depth += 1
            elif content[i] == "}":
                depth -= 1
                if depth == 0:
                    end = i
                    break

        if end != -1:
            return content[start : end + 1]

        # Fallback: use last }
        end = content.rfind("}")
        if end > start:
            return content[start : end + 1]

        return content

    def _parse_ai_response(self, content: str) -> Dict[str, str]:
        """Parse AI response to extract commit message"""
        content = self._extract_json(content)

        try:
            data = json.loads(content)

            # Validate required fields
            if "type" not in data or "title" not in data:
                raise ValueError("Missing required fields")

            # Validate type
            if data["type"] not in self.COMMIT_TYPES:
                print_warning(f"Unknown commit type: {data['type']}, using 'chore'")
                data["type"] = "chore"

            # Ensure title is ≤80 chars
            if len(data["title"]) > 80:
                print_warning(
                    f"Title too long ({len(data['title'])} chars), truncating..."
                )
                data["title"] = data["title"][:77] + "..."

            # Ensure body exists
            if "body" not in data:
                data["body"] = ""

            return data

        except json.JSONDecodeError as e:
            raise Exception(
                f"Failed to parse AI response as JSON: {e}\nContent: {content}"
            )

    def generate_issue_content(
        self, diff: str, files: List[str], stats: str, issue_type: str
    ) -> Dict[str, str]:
        """
        Generate issue title and body using AI

        Returns:
            {
                'title': 'Issue title',
                'body': 'Issue description'
            }
        """
        # Limit diff size
        max_diff_lines = 10000
        diff_lines = diff.splitlines()
        if len(diff_lines) > max_diff_lines:
            print_warning(
                f"Diff has {len(diff_lines)} lines (>{max_diff_lines}). Consider splitting into smaller PRs."
            )
            truncated_diff = "\n".join(diff_lines[:max_diff_lines])
            truncated_diff += (
                f"\n\n... (truncated {len(diff_lines) - max_diff_lines} lines)"
            )
        else:
            truncated_diff = diff

        if not self.has_api_key:
            raise Exception("No AI available for issue generation")

        prompt = self._build_issue_prompt(truncated_diff, files, stats, issue_type)

        # Try AI providers in order
        if self.has_claude_cli:
            try:
                return self._call_ai_for_issue(prompt)
            except Exception as e:
                print_warning(f"Claude CLI failed: {e}")
                if not (self.gemini_key or self.anthropic_key or self.openai_key):
                    raise

        if self.gemini_key:
            try:
                return self._call_ai_for_issue(prompt, "gemini")
            except Exception as e:
                print_warning(f"Gemini API failed: {e}")
                if not (self.anthropic_key or self.openai_key):
                    raise

        if self.anthropic_key:
            try:
                return self._call_ai_for_issue(prompt, "claude")
            except Exception as e:
                print_warning(f"Claude API failed: {e}")
                if not self.openai_key:
                    raise

        if self.openai_key:
            return self._call_ai_for_issue(prompt, "openai")

        raise Exception("All AI API calls failed")

    def _build_issue_prompt(
        self, diff: str, files: List[str], stats: str, issue_type: str
    ) -> str:
        """Build prompt for issue content generation"""
        # Map issue type to prefix and description
        type_config = {
            "bug": {"prefix": "[Bug]", "desc": "a bug fix"},
            "feature": {"prefix": "[Feature]", "desc": "a new feature"},
            "enhancement": {
                "prefix": "[Enhancement]",
                "desc": "an enhancement to existing functionality",
            },
            "benchmark": {
                "prefix": "[Benchmark]",
                "desc": "a benchmark or performance improvement",
            },
        }
        config = type_config.get(
            issue_type, {"prefix": "[Feature]", "desc": "a change"}
        )
        prefix = config["prefix"]
        type_desc = config["desc"]
        files_list = "\n".join(f"  - {f}" for f in files)

        return f"""You are generating a GitHub issue for the Milvus vector database project.
This issue is for {type_desc}.

Based on the code changes below, generate:
1. A clear, concise issue title following Milvus convention:
   - MUST start with "{prefix}" prefix
   - Format: "{prefix} Brief description of the issue"
   - Example: "{prefix} Add support for sparse vector search"
   - Total length ≤80 characters (including prefix)
2. A detailed issue description explaining:
   - What problem this solves or what feature this adds
   - Why this change is needed
   - Brief technical approach (if relevant)

Changed Files:
{files_list}

Statistics: {stats}

Diff:
{diff}

Return ONLY a JSON object with this exact format:
{{"title": "{prefix} Your title here", "body": "Detailed issue description here"}}

IMPORTANT: The title MUST start with "{prefix}".
Make the description professional and informative.
Use markdown formatting in the body where appropriate.
"""

    def _call_ai_for_issue(self, prompt: str, provider: str = "cli") -> Dict[str, str]:
        """Call AI and parse issue response"""
        if provider == "cli":
            result = subprocess.run(
                ["claude", "-p", prompt], capture_output=True, text=True, timeout=60
            )
            if result.returncode != 0:
                raise Exception(f"Claude CLI error: {result.stderr}")
            content = result.stdout.strip()
        elif provider == "gemini":
            url = f"https://generativelanguage.googleapis.com/v1beta/models/gemini-2.0-flash:generateContent?key={self.gemini_key}"
            data = {
                "contents": [{"parts": [{"text": prompt}]}],
                "generationConfig": {"temperature": 0.7, "maxOutputTokens": 2048},
            }
            req = urllib.request.Request(
                url,
                data=json.dumps(data).encode("utf-8"),
                headers={"Content-Type": "application/json"},
            )
            with urllib.request.urlopen(req, timeout=30) as response:
                result = json.loads(response.read().decode("utf-8"))
                content = result["candidates"][0]["content"]["parts"][0]["text"]
        elif provider == "claude":
            url = "https://api.anthropic.com/v1/messages"
            data = {
                "model": "claude-3-5-sonnet-20241022",
                "max_tokens": 1024,
                "messages": [{"role": "user", "content": prompt}],
            }
            req = urllib.request.Request(
                url,
                data=json.dumps(data).encode("utf-8"),
                headers={
                    "x-api-key": self.anthropic_key,
                    "anthropic-version": "2023-06-01",
                    "content-type": "application/json",
                },
            )
            with urllib.request.urlopen(req, timeout=30) as response:
                result = json.loads(response.read().decode("utf-8"))
                content = result["content"][0]["text"]
        elif provider == "openai":
            url = "https://api.openai.com/v1/chat/completions"
            data = {
                "model": "gpt-4",
                "messages": [{"role": "user", "content": prompt}],
                "temperature": 0.7,
            }
            req = urllib.request.Request(
                url,
                data=json.dumps(data).encode("utf-8"),
                headers={
                    "Authorization": f"Bearer {self.openai_key}",
                    "Content-Type": "application/json",
                },
            )
            with urllib.request.urlopen(req, timeout=30) as response:
                result = json.loads(response.read().decode("utf-8"))
                content = result["choices"][0]["message"]["content"]
        else:
            raise Exception(f"Unknown provider: {provider}")

        # Parse response using shared extraction method
        content = self._extract_json(content)

        try:
            data = json.loads(content)
            if "title" not in data or "body" not in data:
                raise ValueError("Missing required fields")
            return data
        except json.JSONDecodeError as e:
            raise Exception(f"Failed to parse AI response: {e}")

    def analyze_conflict(
        self, conflict_files: List[str], original_pr_title: str
    ) -> str:
        """
        Analyze cherry-pick conflicts using AI

        Args:
            conflict_files: List of file paths with conflicts
            original_pr_title: Title of the original PR being cherry-picked

        Returns: AI analysis and suggestions
        """
        if not self.has_api_key:
            return "No AI available for conflict analysis."

        # Read conflict content from files
        conflict_contents = []
        for file_path in conflict_files[:5]:  # Limit to 5 files
            try:
                content = GitOperations.get_conflict_diff(file_path)
                if content:
                    # Limit content size
                    if len(content) > 2000:
                        content = content[:2000] + "\n... (truncated)"
                    conflict_contents.append(f"=== {file_path} ===\n{content}")
            except Exception:
                pass

        if not conflict_contents:
            return "Could not read conflict content from files."

        conflicts_text = "\n".join(conflict_contents)
        prompt = f"""You are analyzing a git cherry-pick conflict for the Milvus vector database project.

Original PR being cherry-picked: {original_pr_title}

The following files have merge conflicts:

{conflicts_text}

Please analyze:
1. What is causing the conflict (e.g., code structure changes, variable renames, etc.)
2. Which version should likely be kept (HEAD or the cherry-picked commit)
3. Specific suggestions for resolving each conflict

Keep the response concise and actionable. Focus on the most important conflicts first.
"""

        # Try available AI providers
        try:
            if self.has_claude_cli:
                result = subprocess.run(
                    ["claude", "-p", prompt], capture_output=True, text=True, timeout=60
                )
                if result.returncode == 0:
                    return result.stdout.strip()

            if self.gemini_key:
                url = f"https://generativelanguage.googleapis.com/v1beta/models/gemini-2.0-flash:generateContent?key={self.gemini_key}"
                data = {
                    "contents": [{"parts": [{"text": prompt}]}],
                    "generationConfig": {"temperature": 0.3, "maxOutputTokens": 2048},
                }
                req = urllib.request.Request(
                    url,
                    data=json.dumps(data).encode("utf-8"),
                    headers={"Content-Type": "application/json"},
                )
                with urllib.request.urlopen(req, timeout=30) as response:
                    result = json.loads(response.read().decode("utf-8"))
                    return result["candidates"][0]["content"]["parts"][0]["text"]

            if self.anthropic_key:
                url = "https://api.anthropic.com/v1/messages"
                data = {
                    "model": "claude-3-5-sonnet-20241022",
                    "max_tokens": 2048,
                    "messages": [{"role": "user", "content": prompt}],
                }
                req = urllib.request.Request(
                    url,
                    data=json.dumps(data).encode("utf-8"),
                    headers={
                        "x-api-key": self.anthropic_key,
                        "anthropic-version": "2023-06-01",
                        "content-type": "application/json",
                    },
                )
                with urllib.request.urlopen(req, timeout=30) as response:
                    result = json.loads(response.read().decode("utf-8"))
                    return result["content"][0]["text"]

            if self.openai_key:
                url = "https://api.openai.com/v1/chat/completions"
                data = {
                    "model": "gpt-4",
                    "messages": [{"role": "user", "content": prompt}],
                    "temperature": 0.3,
                }
                req = urllib.request.Request(
                    url,
                    data=json.dumps(data).encode("utf-8"),
                    headers={
                        "Authorization": f"Bearer {self.openai_key}",
                        "Content-Type": "application/json",
                    },
                )
                with urllib.request.urlopen(req, timeout=30) as response:
                    result = json.loads(response.read().decode("utf-8"))
                    return result["choices"][0]["message"]["content"]

        except Exception as e:
            return f"AI analysis failed: {e}"

        return "No AI provider available for conflict analysis."


# ============================================================================
# GitHub Module - GitHub CLI operations
# ============================================================================


class GitHubOperations:
    """Handles GitHub CLI operations"""

    @staticmethod
    def check_gh_cli():
        """Check if gh CLI is installed and authenticated"""
        try:
            subprocess.run(["gh", "--version"], check=True, capture_output=True)
        except (subprocess.CalledProcessError, FileNotFoundError):
            raise Exception(
                "GitHub CLI (gh) is not installed. Install from: https://cli.github.com/"
            )

        # Check authentication
        try:
            subprocess.run(["gh", "auth", "status"], check=True, capture_output=True)
        except subprocess.CalledProcessError:
            raise Exception("GitHub CLI not authenticated. Run: gh auth login")

    @staticmethod
    def create_issue(title: str, body: str, issue_type: str) -> str:
        """
        Create issue in milvus-io/milvus repo
        Returns: issue number
        """
        label = f"kind/{issue_type}"

        cmd = [
            "gh",
            "issue",
            "create",
            "--repo",
            "milvus-io/milvus",
            "--title",
            title,
            "--body",
            body,
            "--label",
            label,
        ]

        output = GitOperations.run_command(cmd)
        # Extract issue number from URL
        issue_number = output.split("/")[-1]
        return issue_number

    @staticmethod
    def create_pr(title: str, body: str, branch: str, issue_type: str) -> str:
        """
        Create PR from fork to milvus-io/milvus
        Returns: PR URL
        """
        # Get fork owner from remote URL (more reliable than gh api user)
        _, fork_owner = GitOperations.get_fork_info()

        label = f"kind/{issue_type}"

        cmd = [
            "gh",
            "pr",
            "create",
            "--repo",
            "milvus-io/milvus",
            "--head",
            f"{fork_owner}:{branch}",
            "--base",
            "master",
            "--title",
            title,
            "--body",
            body,
            "--label",
            label,
        ]

        return GitOperations.run_command(cmd)

    @staticmethod
    def add_cherry_pick_comment(pr_url: str, target_branch: str):
        """Add cherry-pick comment to PR"""
        cmd = ["gh", "pr", "comment", pr_url, "--body", f"/cherry-pick {target_branch}"]
        GitOperations.run_command(cmd)

    @staticmethod
    def get_existing_pr_for_branch(branch: str) -> Optional[Dict]:
        """
        Check if a PR already exists for the given branch.

        Returns: Dict with PR info (url, body, number) or None if no PR exists
        """
        try:
            _, fork_owner = GitOperations.get_fork_info()
            cmd = [
                "gh",
                "pr",
                "view",
                "--repo",
                "milvus-io/milvus",
                "--head",
                f"{fork_owner}:{branch}",
                "--json",
                "url,body,number,labels",
            ]
            result = subprocess.run(cmd, capture_output=True, text=True)
            if result.returncode == 0 and result.stdout.strip():
                return json.loads(result.stdout)
            return None
        except Exception:
            return None

    @staticmethod
    def get_release_branches() -> List[str]:
        """Get list of release branches (2.x)"""
        try:
            output = GitOperations.run_command(["git", "branch", "-r"], check=False)
            branches = []
            for line in output.splitlines():
                branch = line.strip().replace("origin/", "")
                if branch.startswith("2."):
                    branches.append(branch)
            return sorted(branches, reverse=True)
        except Exception:
            return []

    @staticmethod
    def search_merged_prs(query: str, limit: int = 20, days: int = 60) -> List[Dict]:
        """
        Search for merged PRs in milvus-io/milvus master branch

        Args:
            query: Search keyword or issue number (e.g., "TTL" or "#46716")
            limit: Maximum number of results
            days: Search PRs merged within the last N days (default 60)

        Returns: List of PR info dicts sorted by relevance
        """
        try:
            # Check if query is an issue number (must be purely numeric, optionally with #)
            issue_match = re.match(r"^#?(\d+)$", query.strip())

            if issue_match:
                issue_num = issue_match.group(1)
                # Search for PRs that reference this issue
                cmd = [
                    "gh",
                    "pr",
                    "list",
                    "--repo",
                    "milvus-io/milvus",
                    "--base",
                    "master",
                    "--state",
                    "merged",
                    "--search",
                    f"issue: #{issue_num}",
                    "--limit",
                    str(limit),
                    "--json",
                    "number,title,body,mergeCommit,mergedAt,headRefName",
                ]
                output = GitOperations.run_command(cmd)
                if not output:
                    return []
                return json.loads(output)
            else:
                # Keyword search using gh pr list with --search flag
                # GitHub search is case-insensitive by default
                from datetime import datetime, timedelta

                since_date = (datetime.now() - timedelta(days=days)).strftime(
                    "%Y-%m-%d"
                )

                # Build search query with date filter
                search_query = f"{query} merged:>={since_date}"

                cmd = [
                    "gh",
                    "pr",
                    "list",
                    "--repo",
                    "milvus-io/milvus",
                    "--base",
                    "master",
                    "--state",
                    "merged",
                    "--search",
                    search_query,
                    "--limit",
                    str(limit * 2),  # Get more results for ranking
                    "--json",
                    "number,title,body,mergeCommit,mergedAt,headRefName",
                ]

                output = GitOperations.run_command(cmd)
                if not output:
                    return []

                prs = json.loads(output)

                # Rank results by relevance (title match > body match)
                query_lower = query.strip().lower()
                keywords = query_lower.split()

                def relevance_score(pr: Dict) -> int:
                    """Calculate relevance score: higher = better match"""
                    title = (pr.get("title") or "").lower()
                    body = (pr.get("body") or "").lower()
                    score = 0

                    for kw in keywords:
                        # Exact keyword in title: +10 per match
                        if kw in title:
                            score += 10
                        # Partial keyword in title (e.g., 'mac' in 'macos'): +5
                        elif any(kw in word or word in kw for word in title.split()):
                            score += 5
                        # Keyword in body: +2
                        if kw in body:
                            score += 2

                    return score

                # Sort by relevance (highest first), then by PR number (newest first)
                prs_with_score = [(pr, relevance_score(pr)) for pr in prs]
                prs_with_score.sort(key=lambda x: (-x[1], -x[0].get("number", 0)))

                # Return all results sorted by relevance, let user choose
                # (removed score > 0 filter to allow partial matches like 'mac' finding 'mac14')
                sorted_prs = [pr for pr, score in prs_with_score]

                return sorted_prs[:limit]

        except Exception as e:
            print_warning(f"Search failed: {e}")
            return []

    @staticmethod
    def get_pr_details(pr_number: int) -> Optional[Dict]:
        """Get detailed PR information"""
        try:
            cmd = [
                "gh",
                "pr",
                "view",
                str(pr_number),
                "--repo",
                "milvus-io/milvus",
                "--json",
                "number,title,body,mergeCommit,mergedAt,headRefName,baseRefName",
            ]
            output = GitOperations.run_command(cmd)
            return json.loads(output) if output else None
        except Exception as e:
            print_warning(f"Failed to get PR details: {e}")
            return None

    @staticmethod
    def search_issues(query: str, limit: int = 100, days: int = 30) -> List[Dict]:
        """
        Search GitHub issues by keyword using gh CLI

        Args:
            query: Search keyword
            limit: Maximum number of results
            days: Search issues created within the last N days

        Returns: List of issue info dicts
        """
        try:
            from datetime import datetime, timedelta

            since_date = (datetime.now() - timedelta(days=days)).strftime("%Y-%m-%d")

            # Use gh search issues for better search capability
            cmd = [
                "gh",
                "search",
                "issues",
                "--repo",
                "milvus-io/milvus",
                "--limit",
                str(limit),
                "--json",
                "number,title,state,createdAt,url",
                "--",
                f"{query} created:>={since_date}",
            ]

            output = GitOperations.run_command(cmd, check=False)
            if not output:
                return []
            return json.loads(output)
        except Exception as e:
            print_warning(f"Issue search failed: {e}")
            return []

    @staticmethod
    def search_prs(
        query: str, limit: int = 100, days: int = 30, state: str = "all"
    ) -> List[Dict]:
        """
        Search GitHub PRs by keyword using gh CLI

        Args:
            query: Search keyword
            limit: Maximum number of results
            days: Search PRs created within the last N days
            state: PR state filter ('all', 'open', 'closed', 'merged')

        Returns: List of PR info dicts
        """
        try:
            from datetime import datetime, timedelta

            since_date = (datetime.now() - timedelta(days=days)).strftime("%Y-%m-%d")

            # Use gh search prs for better search capability
            cmd = [
                "gh",
                "search",
                "prs",
                "--repo",
                "milvus-io/milvus",
                "--limit",
                str(limit),
                "--json",
                "number,title,state,createdAt,url",
            ]

            # Add state filter if not 'all'
            if state != "all":
                cmd.extend(["--state", state])

            cmd.extend(["--", f"{query} created:>={since_date}"])

            output = GitOperations.run_command(cmd, check=False)
            if not output:
                return []
            return json.loads(output)
        except Exception as e:
            print_warning(f"PR search failed: {e}")
            return []

    @staticmethod
    def extract_related_issue(pr_body: str) -> Optional[str]:
        """Extract related issue number from PR body"""
        if not pr_body:
            return None

        # Match #123 format (GitHub issue/PR reference)
        # Also match full GitHub URL: https://github.com/.../issues/123
        patterns = [
            r"#(\d+)",
            r"github\.com/[^/]+/[^/]+/issues/(\d+)",
        ]

        for pattern in patterns:
            match = re.search(pattern, pr_body)
            if match:
                return match.group(1)

        return None

    @staticmethod
    def create_cherry_pick_pr(
        title: str, body: str, branch: str, target_branch: str
    ) -> str:
        """
        Create cherry-pick PR from fork to milvus-io/milvus release branch

        Args:
            title: PR title
            body: PR body
            branch: Source branch name (e.g., cp26/46997)
            target_branch: Target branch (e.g., 2.6)

        Returns: PR URL
        """
        _, fork_owner = GitOperations.get_fork_info()

        # Milvus uses plain version numbers as branch names (e.g., "2.6", not "branch-2.6")
        # Strip any "branch-" prefix if present
        if target_branch.startswith("branch-"):
            base = target_branch.replace("branch-", "")
        else:
            base = target_branch

        cmd = [
            "gh",
            "pr",
            "create",
            "--repo",
            "milvus-io/milvus",
            "--head",
            f"{fork_owner}:{branch}",
            "--base",
            base,
            "--title",
            title,
            "--body",
            body,
        ]

        return GitOperations.run_command(cmd)

    @staticmethod
    def get_milestones(version_prefix: str = None) -> List[Dict]:
        """
        Get milestones from milvus-io/milvus repo, optionally filtered by version prefix.

        Args:
            version_prefix: Version prefix to filter (e.g., "2.6" will match "2.6.9", "2.6.10")

        Returns: List of milestone dicts with 'number' and 'title'
        """
        try:
            output = GitOperations.run_command(
                [
                    "gh",
                    "api",
                    "repos/milvus-io/milvus/milestones",
                    "--jq",
                    "[.[] | {number: .number, title: .title}]",
                ]
            )

            milestones = json.loads(output) if output.strip() else []

            # Filter by version prefix if provided
            if version_prefix:
                filtered = []
                for m in milestones:
                    title = m.get("title", "")
                    # Match milestones that start with the version prefix
                    # e.g., "2.6" matches "2.6.9", "2.6.10", etc.
                    if title.startswith(version_prefix):
                        filtered.append(m)
                return sorted(filtered, key=lambda x: x.get("title", ""))

            return sorted(milestones, key=lambda x: x.get("title", ""))

        except Exception as e:
            print_warning(f"Failed to fetch milestones: {e}")
            return []

    @staticmethod
    def set_pr_milestone(pr_number: str, milestone_number: int):
        """
        Set milestone on a PR.

        Args:
            pr_number: PR number (extracted from URL or direct number)
            milestone_number: Milestone number from GitHub
        """
        # Extract PR number if URL is provided
        if "/" in str(pr_number):
            pr_number = pr_number.rstrip("/").split("/")[-1]

        cmd = [
            "gh",
            "pr",
            "edit",
            pr_number,
            "--repo",
            "milvus-io/milvus",
            "--milestone",
            str(milestone_number),
        ]
        GitOperations.run_command(cmd)


# ============================================================================
# Interaction Module - User prompts and input
# ============================================================================


class UserInteraction:
    """Handles user interaction and prompts"""

    @staticmethod
    def confirm(prompt: str, default: bool = False) -> bool:
        """Ask yes/no question"""
        suffix = " [Y/n]: " if default else " [y/N]: "
        response = input(prompt + suffix).strip().lower()

        if not response:
            return default
        return response in ["y", "yes"]

    @staticmethod
    def prompt(question: str) -> str:
        """Ask for single-line input"""
        return input(question + " ").strip()

    @staticmethod
    def prompt_multiline(prompt: str) -> str:
        """Ask for multi-line input"""
        print(prompt)
        print("(Enter empty line to finish)")
        lines = []
        while True:
            line = input()
            if not line:
                break
            lines.append(line)
        return "\n".join(lines)

    @staticmethod
    def choose_files(files: List[str]) -> List[str]:
        """Interactive file selection"""
        print("\nFiles to stage:")
        for i, f in enumerate(files):
            print(f"  {i}: {f}")

        print("\nEnter numbers (comma-separated) or 'all':")
        choice = input("> ").strip()

        if choice == "all":
            return files

        try:
            indices = [int(x.strip()) for x in choice.split(",")]
            return [files[i] for i in indices if 0 <= i < len(files)]
        except Exception:
            print_error("Invalid selection")
            return []

    @staticmethod
    def select_option(prompt: str, options: List[Tuple[str, str]]) -> str:
        """
        Present multiple choice options

        Args:
            prompt: Question to ask
            options: List of (key, description) tuples

        Returns: Selected key
        """
        print(f"\n{prompt}")
        for key, desc in options:
            print(f"  [{key}] {desc}")

        while True:
            choice = input("\nChoice: ").strip().lower()
            valid_keys = [k for k, _ in options]
            if choice in valid_keys:
                return choice
            print_error(f"Invalid choice. Please select from: {', '.join(valid_keys)}")


# ============================================================================
# Workflow Functions
# ============================================================================


def generate_branch_name(branch_type: str, description: str) -> str:
    """Generate a branch name from type and description"""
    # Clean up description: remove special chars, convert to lowercase, limit length
    clean_desc = re.sub(r"[^a-zA-Z0-9\s-]", "", description)
    clean_desc = re.sub(r"\s+", "-", clean_desc.strip().lower())
    clean_desc = clean_desc[:30]  # Limit length

    # Add timestamp suffix to ensure uniqueness
    timestamp = int(time.time()) % 10000

    return f"{branch_type}/{clean_desc}-{timestamp}"


def ensure_feature_branch():
    """Ensure we're not on master branch, create new branch if needed"""
    current_branch = GitOperations.get_current_branch()

    if current_branch != "master":
        print_success(f"Working on branch: {current_branch}")
        return current_branch

    # On master branch - need to create a new branch
    print_warning("You are on the 'master' branch!")
    print_info("Creating a new feature branch is required.")

    choice = UserInteraction.select_option(
        "Choose branch type:",
        [
            ("fix", "Bug fix"),
            ("feat", "New feature"),
            ("enhance", "Enhancement"),
            ("refactor", "Refactoring"),
            ("test", "Test changes"),
            ("docs", "Documentation"),
            ("chore", "Chore/maintenance"),
        ],
    )

    description = UserInteraction.prompt("Brief description (will be sanitized):")
    if not description:
        description = "update"

    branch_name = generate_branch_name(choice, description)

    # Check if branch already exists
    if GitOperations.branch_exists(branch_name):
        print_warning(f"Branch {branch_name} already exists")
        branch_name = generate_branch_name(choice, description + "-alt")

    print_info(f"Creating branch: {branch_name}")
    try:
        GitOperations.create_branch(branch_name)
        print_success(f"Created and switched to branch: {branch_name}")
        return branch_name
    except Exception as e:
        print_error(f"Failed to create branch: {e}")
        sys.exit(1)


def workflow_rebase():
    """Rebase and squash workflow"""
    print_header("🔄 Rebase & Squash Workflow")

    # 1. Check current branch
    branch = GitOperations.get_current_branch()
    if branch == "master":
        print_error("Cannot rebase on master branch")
        return False

    print_success(f"Current branch: {branch}")

    # 2. Check for uncommitted changes
    staged, unstaged = GitOperations.get_status()
    if staged or unstaged:
        print_error("You have uncommitted changes. Please commit or stash them first.")
        return False

    # 3. Check if rebase is already in progress
    if GitOperations.is_rebase_in_progress():
        print_warning("A rebase is already in progress!")
        choice = UserInteraction.select_option(
            "What do you want to do?",
            [
                ("c", "Continue rebase (after resolving conflicts)"),
                ("a", "Abort rebase"),
            ],
        )
        if choice == "a":
            GitOperations.rebase_abort()
            print_success("Rebase aborted")
            return False
        else:
            print_info("Please resolve conflicts and run mgit --rebase again")
            return False

    # 4. Fetch latest master from upstream (milvus-io/milvus)
    upstream_remote = GitOperations.get_upstream_remote()
    upstream_master = GitOperations.get_upstream_master()
    print_info(f"Fetching latest master from {upstream_remote}...")
    try:
        GitOperations.fetch(upstream_remote, "master")
        print_success(f"Fetched {upstream_master}")
    except Exception as e:
        print_error(f"Failed to fetch: {e}")
        return False

    # 5. Check commit count
    commit_count = GitOperations.get_commit_count_from_ref(upstream_master)
    if commit_count == 0:
        print_warning("No commits ahead of master. Nothing to do.")
        return True

    print_info(f"Found {commit_count} commit(s) ahead of master")

    # Show existing commits
    print("\nExisting commits:")
    commit_log = GitOperations.run_command(
        ["git", "log", "--oneline", f"{upstream_master}..HEAD"]
    )
    for line in commit_log.splitlines():
        print(f"  {line}")

    # 6. Rebase onto upstream master (milvus-io/milvus)
    print_info(f"\nRebasing onto {upstream_master}...")
    success, error = GitOperations.rebase(upstream_master)

    if not success:
        print_error("Rebase failed - conflicts detected!")
        print_info("\nTo resolve:")
        print_info("  1. Fix conflicts in the listed files")
        print_info("  2. Run: git add <fixed-files>")
        print_info("  3. Run: git rebase --continue")
        print_info("  4. Run: mgit --rebase again")
        print_info("\nOr run: git rebase --abort to cancel")
        return False

    print_success("Rebase completed successfully")

    # 7. Check if squash is needed
    new_commit_count = GitOperations.get_commit_count("master")

    if new_commit_count <= 1:
        print_info("Only one commit - no squash needed")
        if new_commit_count == 1:
            # Ask about force push
            fork_remote, fork_owner = GitOperations.get_fork_info()
            if UserInteraction.confirm(f"\nForce push to {fork_remote}?", default=True):
                try:
                    GitOperations.push(branch, force=True, remote=fork_remote)
                    print_success(f"Force pushed to {fork_remote}/{branch}")
                except Exception as e:
                    print_error(f"Push failed: {e}")
        return True

    # 8. Squash commits
    print_header(f"\n📦 Squashing {new_commit_count} commits into one")

    if not UserInteraction.confirm("Proceed with squash?", default=True):
        print_warning("Squash cancelled")
        return True

    # Get all changes for AI analysis
    print_info("Analyzing all changes...")
    diff = GitOperations.get_all_changes_diff(upstream_master)
    stats = GitOperations.get_all_changes_stat(upstream_master)

    # Get list of changed files
    changed_files_output = GitOperations.run_command(
        ["git", "diff", "--name-only", upstream_master]
    )
    changed_files = [f for f in changed_files_output.splitlines() if f]

    print(f"\n{Colors.BLUE}Changes to be squashed:{Colors.RESET}")
    print(stats)

    # 9. Generate new commit message with AI
    ai_service = AIService()
    full_message = None

    if not ai_service.has_api_key:
        print_warning("No AI API key found.")
        full_message = UserInteraction.prompt_multiline(
            "Enter commit message for squashed commit:"
        )
    else:
        print_info("\nGenerating commit message with AI...")
        try:
            msg_data = ai_service.generate_commit_message(diff, changed_files, stats)
            full_message = f"{msg_data['type']}: {msg_data['title']}"
            if msg_data["body"]:
                full_message += f"\n\n{msg_data['body']}"
        except Exception as e:
            print_error(f"AI generation failed: {e}")
            full_message = UserInteraction.prompt_multiline(
                "Enter commit message manually:"
            )

    # 10. Show and confirm message
    print(f"\n{Colors.BOLD}{'=' * 60}{Colors.RESET}")
    print(f"{Colors.BOLD}Squashed Commit Message:{Colors.RESET}\n")
    print(full_message)
    print(f"\n{Colors.BOLD}{'=' * 60}{Colors.RESET}")

    choice = UserInteraction.select_option(
        "Options:",
        [
            ("y", "Accept and squash"),
            ("e", "Edit message"),
            ("r", "Regenerate with AI"),
            ("m", "Enter manually"),
            ("n", "Cancel (keep multiple commits)"),
        ],
    )

    if choice == "n":
        print_warning("Squash cancelled - keeping multiple commits")
        return True
    elif choice == "e":
        temp_file = create_local_temp_file(full_message, ".txt")
        editor = os.getenv("EDITOR", "vi")
        subprocess.run([editor, temp_file])
        with open(temp_file, "r") as f:
            full_message = f.read().strip()
        remove_local_temp_file(temp_file)
    elif choice == "r":
        return workflow_rebase()  # Restart
    elif choice == "m":
        full_message = UserInteraction.prompt_multiline("Enter commit message:")

    # 11. Perform squash (soft reset + recommit)
    print_info("Squashing commits...")
    try:
        GitOperations.reset_soft(upstream_master)
        GitOperations.commit(full_message)
        commit_hash = GitOperations.get_commit_hash()
        print_success(f"Squashed into single commit: {commit_hash}")
    except Exception as e:
        print_error(f"Squash failed: {e}")
        return False

    # 12. Force push
    fork_remote, fork_owner = GitOperations.get_fork_info()
    if UserInteraction.confirm(f"\nForce push to {fork_remote}?", default=True):
        try:
            GitOperations.push(branch, force=True, remote=fork_remote)
            print_success(f"Force pushed to {fork_remote}/{branch}")
        except Exception as e:
            print_error(f"Push failed: {e}")

    print_success("\n✅ Rebase and squash completed!")
    return True


def workflow_commit():
    """Smart commit workflow"""
    print_header("📝 Smart Commit Workflow")

    # 0. Ensure we're on a feature branch (not master)
    ensure_feature_branch()

    # 1. Check git status
    print_info("Checking git status...")
    staged, unstaged = GitOperations.get_status()

    if not staged and not unstaged:
        print_warning("No changes to commit")
        return None

    # 2. Handle unstaged files
    if unstaged:
        print(f"\n{Colors.YELLOW}Unstaged files ({len(unstaged)}):{Colors.RESET}")
        for f in unstaged[:10]:  # Show first 10
            print(f"  {f}")
        if len(unstaged) > 10:
            print(f"  ... and {len(unstaged) - 10} more")

        choice = UserInteraction.select_option(
            "Stage files?",
            [
                ("a", "Stage all files"),
                ("s", "Select files"),
                ("c", "Cancel (only commit staged files)"),
            ],
        )

        if choice == "a":
            GitOperations.stage_all()
            print_success("All files staged")
        elif choice == "s":
            selected = UserInteraction.choose_files(unstaged)
            if selected:
                GitOperations.stage_files(selected)
                print_success(f"Staged {len(selected)} file(s)")
            else:
                print_warning("No files selected for staging")
        # choice == 'c': continue with only staged files

    # Re-check staged files
    staged, _ = GitOperations.get_status()
    if not staged:
        print_warning("No staged changes to commit")
        return None

    # 3. Run format tools (optional)
    print_header("\n🛠️  Code Formatting")

    # Detect which languages are being modified
    staged_files = staged
    has_go_files = any(f.endswith(".go") for f in staged_files)
    # Include .c extension for C code in internal/core
    has_cpp_files = any(
        f.endswith((".cpp", ".cc", ".c", ".h", ".hpp")) for f in staged_files
    )
    has_python_files = any(f.endswith(".py") for f in staged_files)
    has_shell_files = any(f.endswith(".sh") for f in staged_files)

    # Get repository root directory
    repo_root = GitOperations.run_command(["git", "rev-parse", "--show-toplevel"])

    # Track if any formatting was performed
    formatting_ran = False

    # Go formatting
    if has_go_files:
        if UserInteraction.confirm("Run Go formatting (make fmt)?", default=True):
            print_info("Running make fmt...")
            formatting_ran = True
            try:
                result = subprocess.run(
                    ["make", "fmt"],
                    cwd=repo_root,
                    capture_output=True,
                    text=True,
                    timeout=120,
                )
                if result.returncode == 0:
                    print_success("Go formatting completed")
                else:
                    print_warning(
                        f"Go formatting completed with warnings:\n{result.stderr}"
                    )
            except subprocess.TimeoutExpired:
                print_error("Go formatting timed out (>120s)")
                if not UserInteraction.confirm("Continue without Go formatting?"):
                    return None
            except Exception as e:
                print_warning(f"Go formatting failed: {e}")
                if not UserInteraction.confirm("Continue without Go formatting?"):
                    return None

    # C++ formatting
    if has_cpp_files:
        if UserInteraction.confirm("Run C++ formatting (clang-format)?", default=True):
            print_info("Running C++ formatting...")
            formatting_ran = True
            try:
                # Run clang-format via the existing script
                cpp_core_dir = os.path.join(repo_root, "internal", "core")
                result = subprocess.run(
                    ["./run_clang_format.sh", "."],
                    cwd=cpp_core_dir,
                    capture_output=True,
                    text=True,
                    timeout=120,
                )
                if result.returncode == 0:
                    print_success("C++ formatting completed")
                else:
                    print_warning(
                        f"C++ formatting completed with warnings:\n{result.stderr}"
                    )
            except subprocess.TimeoutExpired:
                print_error("C++ formatting timed out (>120s)")
                if not UserInteraction.confirm("Continue without C++ formatting?"):
                    return None
            except FileNotFoundError:
                print_warning(
                    "C++ formatting script not found, trying clang-format directly..."
                )
                try:
                    # Fallback: run clang-format directly on staged C++ files
                    cpp_files = [
                        f
                        for f in staged_files
                        if f.endswith((".cpp", ".cc", ".c", ".h", ".hpp"))
                    ]
                    for cpp_file in cpp_files:
                        full_path = os.path.join(repo_root, cpp_file)
                        if os.path.exists(full_path):
                            subprocess.run(
                                ["clang-format", "-i", full_path],
                                capture_output=True,
                                timeout=30,
                                check=True,
                            )
                    print_success("C++ formatting completed (direct)")
                except FileNotFoundError:
                    print_warning(
                        "clang-format is not installed. Install with: brew install clang-format"
                    )
                    if not UserInteraction.confirm("Continue without C++ formatting?"):
                        return None
                except subprocess.CalledProcessError as e:
                    print_warning(f"clang-format failed on a file: {e}")
                    if not UserInteraction.confirm("Continue without C++ formatting?"):
                        return None
                except Exception as e:
                    print_warning(f"C++ formatting failed: {e}")
                    if not UserInteraction.confirm("Continue without C++ formatting?"):
                        return None
            except Exception as e:
                print_warning(f"C++ formatting failed: {e}")
                if not UserInteraction.confirm("Continue without C++ formatting?"):
                    return None

    # Python formatting with ruff
    if has_python_files:
        if UserInteraction.confirm("Run Python formatting (ruff)?", default=True):
            print_info("Running Python formatting...")
            formatting_ran = True
            py_files = [f for f in staged_files if f.endswith(".py")]
            try:
                # First run ruff format
                for py_file in py_files:
                    full_path = os.path.join(repo_root, py_file)
                    if os.path.exists(full_path):
                        subprocess.run(
                            ["ruff", "format", full_path],
                            capture_output=True,
                            timeout=30,
                            check=True,
                        )
                # Then run ruff check with auto-fix
                for py_file in py_files:
                    full_path = os.path.join(repo_root, py_file)
                    if os.path.exists(full_path):
                        subprocess.run(
                            ["ruff", "check", "--fix", full_path],
                            capture_output=True,
                            timeout=30,
                            check=False,  # ruff check may return non-zero for unfixable issues
                        )
                print_success("Python formatting completed")
            except FileNotFoundError:
                print_warning("ruff is not installed. Install with: pip install ruff")
                if not UserInteraction.confirm("Continue without Python formatting?"):
                    return None
            except subprocess.TimeoutExpired:
                print_error("Python formatting timed out")
                if not UserInteraction.confirm("Continue without Python formatting?"):
                    return None
            except Exception as e:
                print_warning(f"Python formatting failed: {e}")
                if not UserInteraction.confirm("Continue without Python formatting?"):
                    return None

    # Shell script linting with shellcheck
    if has_shell_files:
        if UserInteraction.confirm("Run Shell linting (shellcheck)?", default=True):
            print_info("Running shellcheck...")
            sh_files = [f for f in staged_files if f.endswith(".sh")]
            all_passed = True
            issues_found = []
            try:
                for sh_file in sh_files:
                    full_path = os.path.join(repo_root, sh_file)
                    if os.path.exists(full_path):
                        result = subprocess.run(
                            ["shellcheck", "-f", "gcc", full_path],
                            capture_output=True,
                            text=True,
                            timeout=30,
                        )
                        if result.returncode != 0:
                            all_passed = False
                            issues_found.append(f"{sh_file}:\n{result.stdout}")
                if all_passed:
                    print_success("Shell linting passed")
                else:
                    print_warning("Shellcheck found issues:")
                    for issue in issues_found[:3]:  # Show first 3 files with issues
                        print(issue)
                    if len(issues_found) > 3:
                        print(f"  ... and {len(issues_found) - 3} more file(s)")
                    if not UserInteraction.confirm(
                        "Continue with shellcheck warnings?", default=True
                    ):
                        return None
            except FileNotFoundError:
                print_warning(
                    "shellcheck is not installed. Install with: brew install shellcheck"
                )
                if not UserInteraction.confirm("Continue without shell linting?"):
                    return None
            except subprocess.TimeoutExpired:
                print_error("Shell linting timed out")
                if not UserInteraction.confirm("Continue without shell linting?"):
                    return None
            except Exception as e:
                print_warning(f"Shell linting failed: {e}")
                if not UserInteraction.confirm("Continue without shell linting?"):
                    return None

    # Check if formatting made changes and stage them (only if formatting was run)
    if formatting_ran:
        # Get files that were modified by formatting (intersection of staged files and unstaged changes)
        _, unstaged_after_fmt = GitOperations.get_status()
        # Only consider files that we originally staged (to avoid picking up unrelated changes)
        formatting_changes = [f for f in unstaged_after_fmt if f in staged_files]
        if formatting_changes:
            print_warning(f"Formatting modified {len(formatting_changes)} file(s)")
            if UserInteraction.confirm("Stage formatting changes?", default=True):
                GitOperations.stage_files(formatting_changes)
                print_success("Formatting changes staged")

    # 4. Collect diff information
    print_info("Analyzing changes...")
    diff = GitOperations.get_staged_diff()
    stats = GitOperations.get_staged_diff_stat()

    print(f"\n{Colors.BLUE}Changes to be committed:{Colors.RESET}")
    print(stats)

    # 4. Generate commit message with AI
    full_message = None
    ai_service = AIService()

    if not ai_service.has_api_key:
        print_warning("No AI API key found.")
        print_info("To enable AI-powered commit messages, set one of:")
        print_info("  export GEMINI_API_KEY='your-key'  (recommended)")
        print_info("  export ANTHROPIC_API_KEY='your-key'")
        print_info("  export OPENAI_API_KEY='your-key'")
        print("")
        full_message = UserInteraction.prompt_multiline(
            "Enter commit message manually:"
        )
    else:
        print_info("\nGenerating commit message with AI...")
        try:
            msg_data = ai_service.generate_commit_message(diff, staged, stats)

            # Build full commit message
            full_message = f"{msg_data['type']}: {msg_data['title']}"
            if msg_data["body"]:
                full_message += f"\n\n{msg_data['body']}"

        except Exception as e:
            print_error(f"AI generation failed: {e}")
            print_info("Falling back to manual input")
            full_message = UserInteraction.prompt_multiline("Enter commit message:")

    # 5. Review and confirm
    print(f"\n{Colors.BOLD}{'=' * 60}{Colors.RESET}")
    message_title = (
        "Commit Message:" if not ai_service.has_api_key else "Generated Commit Message:"
    )
    print(f"{Colors.BOLD}{message_title}{Colors.RESET}\n")
    print(full_message)
    print(f"\n{Colors.BOLD}{'=' * 60}{Colors.RESET}")

    choice = UserInteraction.select_option(
        "Options:",
        [
            ("y", "Accept and commit"),
            ("e", "Edit message"),
            ("r", "Regenerate with AI"),
            ("m", "Enter manually"),
            ("n", "Cancel"),
        ],
    )

    if choice == "n":
        print_warning("Commit cancelled")
        return None
    elif choice == "e":
        print_info("Opening editor...")
        # Create local temp file with message
        temp_file = create_local_temp_file(full_message, ".txt")

        # Open in editor
        editor = os.getenv("EDITOR", "vi")
        subprocess.run([editor, temp_file])

        # Read edited message
        with open(temp_file, "r") as f:
            full_message = f.read().strip()
        remove_local_temp_file(temp_file)
    elif choice == "r":
        print_info("Regenerating...")
        return workflow_commit()  # Recursive call
    elif choice == "m":
        full_message = UserInteraction.prompt_multiline("Enter commit message:")

    # 6. Create commit
    # Final verification that files are staged before attempting commit
    staged_final, _ = GitOperations.get_status()
    if not staged_final:
        print_error("No files staged for commit. Please stage files first.")
        if UserInteraction.confirm("Stage all changes now?", default=True):
            GitOperations.stage_all()
            print_success("All files staged")
        else:
            print_warning("Commit cancelled - no files staged")
            return None

    try:
        GitOperations.commit(full_message)
        commit_hash = GitOperations.get_commit_hash()
        print_success(f"Commit created: {commit_hash}")

        # 7. Optional code review
        print_header("\n🔍 Code Review")
        if UserInteraction.confirm("Run code review with Claude Code?", default=False):
            # Check if claude CLI is available (cross-platform)
            if shutil.which("claude") is None:
                print_warning(
                    "Claude Code CLI not found. Install from: https://claude.com/code"
                )
            else:
                try:
                    print_info("Running code review with Claude Code...")

                    # Get the commit patch for context
                    patch = GitOperations.run_command(["git", "show", "-1", "--patch"])

                    # Limit patch size to avoid token limits
                    max_patch_lines = 10000
                    patch_lines = patch.splitlines()
                    if len(patch_lines) > max_patch_lines:
                        print_warning(
                            f"Patch has {len(patch_lines)} lines (>{max_patch_lines}). Consider splitting into smaller PRs."
                        )
                        truncated_patch = "\n".join(patch_lines[:max_patch_lines])
                        truncated_patch += f"\n\n... (truncated {len(patch_lines) - max_patch_lines} lines)"
                    else:
                        truncated_patch = patch

                    # Build review prompt with the actual diff
                    review_prompt = f"""Review the following commit for potential issues, bugs, or improvements.
Focus on code quality, security, and best practices.

Commit diff:
{truncated_patch}
"""

                    result = subprocess.run(
                        ["claude", "-p", review_prompt],
                        capture_output=True,
                        text=True,
                        timeout=120,  # Increase timeout since we're sending more data
                    )

                    if result.returncode == 0:
                        print_success("Review completed")
                        print(f"\n{Colors.BLUE}Review Results:{Colors.RESET}")
                        print(result.stdout)
                    else:
                        print_warning(
                            f"Review completed with warnings:\n{result.stderr}"
                        )

                except subprocess.TimeoutExpired:
                    print_error("Review timed out (>60s)")
                except Exception as e:
                    print_warning(f"Review failed: {e}")

        return commit_hash
    except Exception as e:
        print_error(f"Commit failed: {e}")
        return None


def workflow_pr():
    """PR creation workflow"""
    print_header("📤 PR Creation Workflow")

    # 1. Pre-flight checks
    print_info("Running pre-flight checks...")

    try:
        GitHubOperations.check_gh_cli()
    except Exception as e:
        print_error(str(e))
        sys.exit(1)

    branch = GitOperations.get_current_branch()
    if branch == "master":
        print_error("Cannot create PR from master branch")
        sys.exit(1)

    print_success(f"Current branch: {branch}")

    # Check for commits
    commit_count = GitOperations.get_commit_count()
    if commit_count == 0:
        print_error("No commits to push")
        sys.exit(1)

    if commit_count > 1:
        upstream_master = GitOperations.get_upstream_master()
        print_warning(f"You have {commit_count} commits on this branch")
        print_info("Milvus typically requires a single squashed commit")
        print_info(f"Consider running: git rebase -i {upstream_master}")
        if not UserInteraction.confirm("Continue anyway?"):
            sys.exit(1)

    # Check DCO
    last_msg = GitOperations.get_last_commit_message()
    if "Signed-off-by:" not in last_msg:
        print_error("Last commit missing DCO signature")
        print_info("Run: git commit --amend -s")
        if not UserInteraction.confirm("Continue without DCO?"):
            sys.exit(1)

    # 2. Push to fork
    fork_remote, fork_owner = GitOperations.get_fork_info()
    print_info(f"Pushing {branch} to {fork_remote} ({fork_owner})...")
    try:
        GitOperations.push(branch, remote=fork_remote)
        print_success(f"Pushed to {fork_remote} ({fork_owner}/{branch})")
    except Exception as e:
        print_warning(f"Push failed: {e}")
        if UserInteraction.confirm("Force push?"):
            GitOperations.push(branch, force=True, remote=fork_remote)
            print_success(f"Force pushed to {fork_remote}")
        else:
            sys.exit(1)

    # 3. Handle issue
    print_header("\n📋 GitHub Issue")

    issue_number = None
    issue_type = "feature"
    choice = None

    # Check if there's an existing PR with a linked issue
    existing_pr = GitHubOperations.get_existing_pr_for_branch(branch)
    if existing_pr:
        pr_body = existing_pr.get("body", "")
        linked_issue = GitHubOperations.extract_related_issue(pr_body)
        if linked_issue:
            issue_number = linked_issue
            # Try to extract issue type from PR labels
            labels = existing_pr.get("labels", [])
            for label in labels:
                label_name = label.get("name", "") if isinstance(label, dict) else label
                if label_name.startswith("kind/"):
                    issue_type = label_name.replace("kind/", "")
                    break
            print_success(f"PR already linked to issue #{issue_number}, skipping issue creation")

    if not issue_number:
        print_warning("Milvus requires all PRs to reference an issue!")

        choice = UserInteraction.select_option(
            "Issue management:",
            [
                ("c", "Create new issue"),
                ("e", "Use existing issue number"),
            ],
        )

    if not issue_number and choice == "c":
        # Create new issue - select type first
        issue_type = UserInteraction.select_option(
            "Issue type:",
            [
                ("bug", "[Bug] Bug fix"),
                ("feature", "[Feature] New feature"),
                ("enhancement", "[Enhancement] Enhancement"),
                ("benchmark", "[Benchmark] Performance/Benchmark"),
            ],
        )

        # Get diff for AI analysis (compare against upstream milvus-io/milvus)
        upstream_master = GitOperations.get_upstream_master()
        diff = GitOperations.get_all_changes_diff(upstream_master)
        stats = GitOperations.get_all_changes_stat(upstream_master)
        changed_files = GitOperations.run_command(
            ["git", "diff", "--name-only", upstream_master]
        ).splitlines()

        # Generate issue content with AI
        ai_service = AIService()
        issue_title = None
        issue_body = None

        if ai_service.has_api_key:
            print_info("Generating issue content with AI...")
            try:
                issue_data = ai_service.generate_issue_content(
                    diff, changed_files, stats, issue_type
                )
                issue_title = issue_data["title"]
                issue_body = issue_data["body"]
            except Exception as e:
                print_warning(f"AI generation failed: {e}")

        # Show and confirm issue content
        if issue_title and issue_body:
            print(f"\n{Colors.BOLD}{'=' * 60}{Colors.RESET}")
            print(f"{Colors.BOLD}Generated Issue:{Colors.RESET}\n")
            print(f"{Colors.BLUE}Title:{Colors.RESET} {issue_title}")
            print(f"\n{Colors.BLUE}Description:{Colors.RESET}\n{issue_body}")
            print(f"\n{Colors.BOLD}{'=' * 60}{Colors.RESET}")

            issue_choice = UserInteraction.select_option(
                "Options:",
                [
                    ("y", "Accept and create issue"),
                    ("e", "Edit content"),
                    ("r", "Regenerate with AI"),
                    ("m", "Enter manually"),
                ],
            )

            if issue_choice == "e":
                content = f"Title: {issue_title}\n\n---\n\n{issue_body}"
                temp_file = create_local_temp_file(content, ".md")
                editor = os.getenv("EDITOR", "vi")
                subprocess.run([editor, temp_file])
                with open(temp_file, "r") as f:
                    edited = f.read()
                remove_local_temp_file(temp_file)
                # Parse edited content
                if "---" in edited:
                    parts = edited.split("---", 1)
                    issue_title = parts[0].replace("Title:", "").strip()
                    issue_body = parts[1].strip()
                else:
                    lines = edited.strip().split("\n", 1)
                    issue_title = lines[0].replace("Title:", "").strip()
                    issue_body = lines[1].strip() if len(lines) > 1 else ""
            elif issue_choice == "r":
                # Regenerate - recursive call will handle it
                print_info("Regenerating...")
                try:
                    issue_data = ai_service.generate_issue_content(
                        diff, changed_files, stats, issue_type
                    )
                    issue_title = issue_data["title"]
                    issue_body = issue_data["body"]
                except Exception as e:
                    print_error(f"Regeneration failed: {e}")
                    issue_title = UserInteraction.prompt("Issue title:")
                    issue_body = UserInteraction.prompt_multiline("Issue description:")
            elif issue_choice == "m":
                issue_title = UserInteraction.prompt("Issue title:")
                issue_body = UserInteraction.prompt_multiline("Issue description:")
        else:
            # No AI available or failed - manual input
            issue_title = UserInteraction.prompt("Issue title:")
            issue_body = UserInteraction.prompt_multiline("Issue description:")

        print_info("Creating issue in milvus-io/milvus...")
        try:
            issue_number = GitHubOperations.create_issue(
                issue_title, issue_body, issue_type
            )
            print_success(f"Issue created: #{issue_number}")
        except Exception as e:
            print_error(f"Failed to create issue: {e}")
            sys.exit(1)

    elif not issue_number and choice == "e":
        issue_number = UserInteraction.prompt("Enter issue number:")
        issue_type = UserInteraction.select_option(
            "Issue type:",
            [
                ("bug", "[Bug] Bug fix"),
                ("feature", "[Feature] New feature"),
                ("enhancement", "[Enhancement] Enhancement"),
                ("benchmark", "[Benchmark] Performance/Benchmark"),
            ],
        )
        print_success(f"Using issue #{issue_number}")

    # 4. Create PR
    print_header("\n🔀 Pull Request")

    # Get PR title from last commit
    default_title = last_msg.split("\n")[0]
    print(f"Default title: {default_title}")

    if UserInteraction.confirm("Use this title?", default=True):
        pr_title = default_title
    else:
        pr_title = UserInteraction.prompt("PR title:")

    # Build PR body
    pr_body = UserInteraction.prompt_multiline("PR description (optional):")

    # Add required issue reference (Milvus requirement)
    if not pr_body:
        pr_body = ""
    pr_body += f"\n\nissue: #{issue_number}"

    print_info("Creating PR in milvus-io/milvus...")
    try:
        pr_url = GitHubOperations.create_pr(pr_title, pr_body, branch, issue_type)
        print_success(f"PR created: {pr_url}")
    except Exception as e:
        print_error(f"Failed to create PR: {e}")
        sys.exit(1)

    # 5. Cherry-pick
    print_header("\n🍒 Cherry-pick")

    if UserInteraction.confirm("Cherry-pick to release branches?"):
        branches = GitHubOperations.get_release_branches()

        if branches:
            print("\nAvailable release branches:")
            for i, b in enumerate(branches):
                print(f"  {i}: {b}")

            selection = UserInteraction.prompt(
                "Enter numbers (comma-separated) or Enter to skip:"
            )

            if selection:
                try:
                    indices = [int(x.strip()) for x in selection.split(",")]
                    for idx in indices:
                        if 0 <= idx < len(branches):
                            target = branches[idx]
                            print_info(f"Requesting cherry-pick to {target}...")
                            GitHubOperations.add_cherry_pick_comment(pr_url, target)
                    print_success("Cherry-pick requests added")
                except Exception as e:
                    print_warning(f"Cherry-pick failed: {e}")
        else:
            print_warning("No release branches found")

    # Done
    print_header("\n✅ Complete!")
    if issue_number:
        print(f"  Issue: #{issue_number}")
    print(f"  PR:    {pr_url}")


def workflow_all():
    """Complete workflow: rebase + commit + PR"""
    # Check if there are changes to commit first
    staged, unstaged = GitOperations.get_status()

    if staged or unstaged:
        # Has uncommitted changes - do commit first
        commit_hash = workflow_commit()
        if not commit_hash:
            print_warning("Commit workflow cancelled")
            return
        print("")

    # Now check if we need to rebase (have commits ahead of master)
    branch = GitOperations.get_current_branch()
    if branch != "master":
        # Always compare against upstream (milvus-io/milvus), not origin fork
        upstream_remote = GitOperations.get_upstream_remote()
        upstream_master = GitOperations.get_upstream_master()
        commit_count = GitOperations.get_commit_count("master")
        if commit_count > 0:
            print_info(f"Found {commit_count} commit(s) - checking if rebase needed...")

            # Fetch from upstream to check if we're behind
            try:
                GitOperations.fetch(upstream_remote, "master")
            except Exception:
                pass

            # Check if there are upstream changes
            behind_count = GitOperations.run_command(
                ["git", "rev-list", "--count", f"HEAD..{upstream_master}"], check=False
            )

            if behind_count and int(behind_count) > 0:
                print_warning(
                    f"Branch is {behind_count} commit(s) behind {upstream_master}"
                )
                if UserInteraction.confirm("Run rebase workflow first?", default=True):
                    if not workflow_rebase():
                        print_warning("Rebase workflow failed or cancelled")
                        if not UserInteraction.confirm("Continue to PR anyway?"):
                            return
                    print("")
            elif commit_count > 1:
                print_warning(
                    f"You have {commit_count} commits (Milvus prefers single commit)"
                )
                if UserInteraction.confirm("Squash commits?", default=True):
                    if not workflow_rebase():
                        print_warning("Squash workflow failed or cancelled")
                        if not UserInteraction.confirm("Continue to PR anyway?"):
                            return
                    print("")

    # Proceed to PR
    workflow_pr()


def workflow_search():
    """Search GitHub issues and PRs by keyword"""
    print_header("🔍 GitHub Search")

    # Get search parameters
    query = UserInteraction.prompt("Enter search keyword:")
    if not query:
        print_error("Search keyword is required")
        return

    # Ask for search type
    search_type = UserInteraction.select_option(
        "What do you want to search?",
        [
            ("i", "Issues only"),
            ("p", "PRs only"),
            ("b", "Both issues and PRs"),
        ],
    )

    # Ask for time range
    days_input = UserInteraction.prompt("Search within last N days (default: 30):")
    try:
        days = int(days_input) if days_input else 30
    except ValueError:
        days = 30

    # Ask for limit
    limit_input = UserInteraction.prompt("Max results (default: 100):")
    try:
        limit = int(limit_input) if limit_input else 100
    except ValueError:
        limit = 100

    print_info(f"Searching for: '{query}' (last {days} days, limit {limit})")

    # Execute search
    if search_type in ["i", "b"]:
        print_header("\n📋 Issues")
        issues = GitHubOperations.search_issues(query, limit=limit, days=days)
        if issues:
            for issue in issues:
                created = issue.get("createdAt", "")[:10]
                state = issue.get("state", "UNKNOWN")
                state_color = Colors.GREEN if state == "OPEN" else Colors.RED
                print(
                    f"  #{issue['number']} [{state_color}{state}{Colors.RESET}] {issue['title']} ({created})"
                )
            print(f"\n  Total: {len(issues)} issues")
        else:
            print_warning("No issues found")

    if search_type in ["p", "b"]:
        print_header("\n🔀 Pull Requests")
        prs = GitHubOperations.search_prs(query, limit=limit, days=days)
        if prs:
            for pr in prs:
                created = pr.get("createdAt", "")[:10]
                state = pr.get("state", "UNKNOWN")
                if state == "MERGED":
                    state_color = Colors.BLUE
                elif state == "OPEN":
                    state_color = Colors.GREEN
                else:
                    state_color = Colors.RED
                print(
                    f"  #{pr['number']} [{state_color}{state}{Colors.RESET}] {pr['title']} ({created})"
                )
            print(f"\n  Total: {len(prs)} PRs")
        else:
            print_warning("No PRs found")

    print_success("\nSearch complete!")


# ============================================================================
# Cherry-Pick Workflow Helpers
# ============================================================================


@dataclass
class CherryPickContext:
    """Context object for cherry-pick workflow state"""

    original_branch: str = ""
    pr_details: Optional[Dict] = None
    commit_sha: str = ""
    related_issue: Optional[str] = None
    target_branch: str = ""
    version_num: str = ""
    cp_branch: str = ""
    upstream_remote: str = ""
    fork_remote: str = ""
    fork_owner: str = ""
    pr_url: str = ""


def _cp_preflight_checks() -> str:
    """Run pre-flight checks for cherry-pick workflow.

    Returns: Original branch name
    Raises: SystemExit on failure
    """
    print_info("Running pre-flight checks...")

    try:
        GitHubOperations.check_gh_cli()
    except Exception as e:
        print_error(str(e))
        sys.exit(1)

    staged, unstaged = GitOperations.get_status()
    if staged or unstaged:
        print_error("You have uncommitted changes. Please commit or stash them first.")
        sys.exit(1)

    original_branch = GitOperations.get_current_branch()
    print_success(f"Current branch: {original_branch}")
    return original_branch


def _cp_select_pr() -> Tuple[Dict, str, Optional[str]]:
    """Select PR to cherry-pick.

    Returns: (pr_details, commit_sha, related_issue)
    Raises: SystemExit on failure
    """
    print_header("\n🔍 Select PR to cherry-pick")

    pr_input = UserInteraction.prompt(
        "Enter PR number or issue number (e.g., '46716' or '#46716'):"
    )
    if not pr_input:
        print_error("PR or issue number is required")
        print_info("Tip: Use 'mgit --search' to find PRs by keyword first")
        sys.exit(1)

    num_match = re.match(r"^#?(\d+)$", pr_input.strip())
    if not num_match:
        print_error(
            "Invalid input. Please enter a valid PR or issue number (e.g., '46716' or '#46716')."
        )
        print_info("Tip: Use 'mgit --search' to find PRs by keyword first")
        sys.exit(1)

    input_num = int(num_match.group(1))

    print_info(f"Looking up #{input_num}...")
    pr_details = GitHubOperations.get_pr_details(input_num)

    if not pr_details:
        print_info(
            f"#{input_num} is not a PR, searching for PRs related to issue #{input_num}..."
        )
        prs = GitHubOperations.search_merged_prs(f"#{input_num}")
        if prs:
            print(f"\n{Colors.BOLD}PRs related to issue #{input_num}:{Colors.RESET}")
            for i, pr in enumerate(prs):
                merged_at = (
                    pr.get("mergedAt", "Unknown")[:10]
                    if pr.get("mergedAt")
                    else "Unknown"
                )
                print(f"  [{i}] #{pr['number']} {pr['title']} (Merged: {merged_at})")

            selection = UserInteraction.prompt("\nSelect PR index (0, 1, ...):")
            try:
                idx = int(selection)
                if idx < 0 or idx >= len(prs):
                    print_error("Invalid selection")
                    sys.exit(1)
                pr_details = GitHubOperations.get_pr_details(prs[idx]["number"])
            except ValueError:
                print_error("Invalid input")
                sys.exit(1)
        else:
            print_error(f"No merged PRs found related to issue #{input_num}")
            sys.exit(1)

    if not pr_details:
        print_error("Failed to get PR details")
        sys.exit(1)

    merge_commit = pr_details.get("mergeCommit", {})
    commit_sha = merge_commit.get("oid") if merge_commit else None

    if not commit_sha:
        print_error("Could not find merge commit SHA for this PR")
        sys.exit(1)

    print_success(f"Selected: #{pr_details['number']} - {pr_details['title']}")
    print_info(f"Merge commit: {commit_sha[:12]}")

    related_issue = GitHubOperations.extract_related_issue(pr_details.get("body", ""))
    if related_issue:
        print_info(f"Related issue: #{related_issue}")

    return pr_details, commit_sha, related_issue


def _cp_get_target_branch() -> Tuple[str, str, str]:
    """Get and validate target branch.

    Returns: (target_branch, version_num, upstream_remote)
    Raises: SystemExit on failure
    """
    print_header("\n🎯 Target Branch")

    target_version = UserInteraction.prompt("Enter target version (e.g., '2.6'):")
    if not target_version:
        print_error("Target version is required")
        sys.exit(1)

    if target_version.startswith("branch-"):
        target_branch = target_version.replace("branch-", "")
        version_num = target_branch
    else:
        target_branch = target_version
        version_num = target_version

    upstream_remote = GitOperations.get_upstream_remote()
    print_info(f"Fetching {upstream_remote}/{target_branch}...")

    try:
        GitOperations.fetch(upstream_remote, target_branch)
    except Exception as e:
        print_error(f"Failed to fetch target branch: {e}")
        print_info(f"Make sure branch '{target_branch}' exists in milvus-io/milvus")
        sys.exit(1)

    try:
        GitOperations.fetch(upstream_remote, "master")
    except Exception:
        pass

    print_success(f"Target branch: {target_branch}")
    return target_branch, version_num, upstream_remote


def _cp_setup_branch(ctx: CherryPickContext) -> str:
    """Create or switch to cherry-pick branch.

    Returns: Cherry-pick branch name
    Raises: SystemExit on failure or cancel
    """
    cp_branch = f"cp{ctx.version_num.replace('.', '')}/{ctx.pr_details['number']}"
    print_info(f"Cherry-pick branch: {cp_branch}")

    if GitOperations.branch_exists(cp_branch):
        print_warning(f"Branch {cp_branch} already exists")
        choice = UserInteraction.select_option(
            "What do you want to do?",
            [
                ("d", "Delete and recreate"),
                ("u", "Use existing branch"),
                ("c", "Cancel"),
            ],
        )
        if choice == "c":
            print_warning("Cherry-pick cancelled")
            sys.exit(0)
        elif choice == "d":
            GitOperations.delete_branch(cp_branch, force=True)
            print_success(f"Deleted branch {cp_branch}")

    if not GitOperations.branch_exists(cp_branch):
        print_info(
            f"Creating branch {cp_branch} from {ctx.upstream_remote}/{ctx.target_branch}..."
        )
        try:
            GitOperations.checkout_remote_branch(
                ctx.upstream_remote, ctx.target_branch, cp_branch
            )
            print_success(f"Created and switched to branch: {cp_branch}")
        except Exception as e:
            print_error(f"Failed to create branch: {e}")
            sys.exit(1)
    else:
        GitOperations.checkout_branch(cp_branch)
        print_success(f"Switched to existing branch: {cp_branch}")

    return cp_branch


def _cp_execute_cherry_pick(ctx: CherryPickContext) -> bool:
    """Execute cherry-pick operation and handle conflicts.

    Returns: True if successful, exits on abort or wait
    """
    print_header("\n🍒 Executing Cherry-Pick")

    print_info(f"Cherry-picking commit {ctx.commit_sha[:12]}...")
    success, error_msg, conflict_files = GitOperations.cherry_pick(ctx.commit_sha)

    if not success:
        print_warning("Cherry-pick encountered conflicts!")
        print(f"\n{Colors.RED}Conflicting files:{Colors.RESET}")
        for f in conflict_files:
            print(f"  - {f}")

        print_header("\n🤖 AI Conflict Analysis")
        ai_service = AIService()
        if ai_service.has_api_key:
            print_info("Analyzing conflicts with AI...")
            analysis = ai_service.analyze_conflict(
                conflict_files, ctx.pr_details["title"]
            )
            print(f"\n{Colors.BLUE}Analysis:{Colors.RESET}")
            print(analysis)
        else:
            print_warning("No AI available for conflict analysis")

        print_header("\n⚠️ Conflict Resolution Required")
        print_info("Please resolve the conflicts manually:")
        print("  1. Edit the conflicting files to resolve conflicts")
        print("  2. Run: git add <resolved-files>")
        print("  3. Run: git cherry-pick --continue")
        print("")
        print("Or run: git cherry-pick --abort to cancel")

        choice = UserInteraction.select_option(
            "Options:",
            [
                ("w", "Wait - I will resolve conflicts and continue later"),
                ("a", "Abort cherry-pick and return to original branch"),
            ],
        )

        if choice == "a":
            GitOperations.cherry_pick_abort()
            GitOperations.checkout_branch(ctx.original_branch)
            GitOperations.delete_branch(ctx.cp_branch, force=True)
            print_warning("Cherry-pick aborted")
            sys.exit(1)
        else:
            print_info("\nAfter resolving conflicts, run:")
            print("  git add <files>")
            print("  git cherry-pick --continue")
            print(
                "  python3 tools/mgit.py --cherry-pick  # To continue with PR creation"
            )
            sys.exit(0)

    print_success("Cherry-pick successful!")

    diff_stat = GitOperations.run_command(["git", "diff", "--stat", "HEAD~1"])
    print(f"\n{Colors.BLUE}Changes:{Colors.RESET}")
    print(diff_stat)
    return True


def _cp_push_to_fork(ctx: CherryPickContext) -> Tuple[str, str]:
    """Push cherry-pick branch to fork.

    Returns: (fork_remote, fork_owner)
    Raises: SystemExit on failure or cancel
    """
    print_header("\n📤 Push to Fork")

    if not UserInteraction.confirm("Push to your fork?", default=True):
        print_warning("Push cancelled. You can push manually later.")
        print(f"  Branch: {ctx.cp_branch}")
        sys.exit(0)

    fork_remote, fork_owner = GitOperations.get_fork_info()
    print_info(f"Pushing {ctx.cp_branch} to {fork_remote}...")

    try:
        GitOperations.push(ctx.cp_branch, force=False, remote=fork_remote)
        print_success(f"Pushed to {fork_remote}/{ctx.cp_branch}")
    except Exception as e:
        print_warning(f"Push failed: {e}")
        if UserInteraction.confirm("Force push?"):
            GitOperations.push(ctx.cp_branch, force=True, remote=fork_remote)
            print_success(f"Force pushed to {fork_remote}/{ctx.cp_branch}")
        else:
            print_warning("Push cancelled")
            sys.exit(1)

    return fork_remote, fork_owner


def _cp_create_pr(ctx: CherryPickContext) -> str:
    """Create cherry-pick PR.

    Returns: PR URL
    Raises: SystemExit on failure or cancel
    """
    print_header("\n🔀 Create Cherry-Pick PR")

    original_title = ctx.pr_details["title"]
    type_match = re.match(r"^(\w+):\s*(.+)$", original_title)
    if type_match:
        pr_type = type_match.group(1)
        title_rest = type_match.group(2)
    else:
        pr_type = "fix"
        title_rest = original_title

    cp_pr_title = (
        f"{pr_type}: [{ctx.version_num}] {title_rest} (#{ctx.pr_details['number']})"
    )

    cp_pr_body = f"""Cherry-pick from master
pr: #{ctx.pr_details["number"]}"""

    if ctx.related_issue:
        cp_pr_body += f"\nRelated to #{ctx.related_issue}"

    original_body = ctx.pr_details.get("body", "")
    if original_body:
        cleaned_body = re.sub(r"[Ii]ssue:\s*#\d+\s*\n?", "", original_body)
        cleaned_body = cleaned_body.strip()
        if cleaned_body:
            if len(cleaned_body) > 1000:
                cleaned_body = cleaned_body[:1000] + "\n\n... (truncated)"
            cp_pr_body += f"\n\n{cleaned_body}"

    print(f"\n{Colors.BOLD}{'=' * 60}{Colors.RESET}")
    print(f"{Colors.BOLD}PR Title:{Colors.RESET} {cp_pr_title}")
    print(f"\n{Colors.BOLD}PR Body:{Colors.RESET}")
    print(cp_pr_body)
    print(f"\n{Colors.BOLD}{'=' * 60}{Colors.RESET}")

    choice = UserInteraction.select_option(
        "Options:",
        [
            ("y", "Create PR"),
            ("e", "Edit title/body"),
            ("n", "Cancel (branch already pushed)"),
        ],
    )

    if choice == "n":
        print_warning("PR creation cancelled")
        print(f"Branch {ctx.cp_branch} has been pushed. You can create PR manually.")
        sys.exit(0)
    elif choice == "e":
        cp_pr_title = (
            UserInteraction.prompt(f"PR title [{cp_pr_title}]:") or cp_pr_title
        )
        print("Enter PR body (Enter empty line to finish):")
        new_body = UserInteraction.prompt_multiline("PR body:")
        if new_body:
            cp_pr_body = new_body

    print_info("Creating cherry-pick PR...")
    try:
        pr_url = GitHubOperations.create_cherry_pick_pr(
            cp_pr_title, cp_pr_body, ctx.cp_branch, ctx.target_branch
        )
        print_success(f"PR created: {pr_url}")
        return pr_url
    except Exception as e:
        print_error(f"Failed to create PR: {e}")
        print_info(f"You can create PR manually from branch: {ctx.cp_branch}")
        sys.exit(1)


def _cp_set_milestone(ctx: CherryPickContext):
    """Set milestone for the cherry-pick PR."""
    print_header("\n🎯 Set Milestone (Roadmap)")

    milestones = GitHubOperations.get_milestones(ctx.version_num)

    if not milestones:
        print_warning(f"No milestones found for version {ctx.version_num}")
        return

    print("Available milestones:")
    for i, m in enumerate(milestones):
        print(f"  [{i}] {m['title']}")
    print("  [s] Skip - don't set milestone")

    selection = UserInteraction.prompt("\nSelect milestone index:")

    if selection.lower() == "s" or not selection.strip():
        print_info("Skipping milestone")
        return

    try:
        idx = int(selection)
        if 0 <= idx < len(milestones):
            selected_milestone = milestones[idx]
            print_info(f"Setting milestone to {selected_milestone['title']}...")
            try:
                GitHubOperations.set_pr_milestone(
                    ctx.pr_url, selected_milestone["number"]
                )
                print_success(f"Milestone set: {selected_milestone['title']}")
            except Exception as e:
                print_warning(f"Failed to set milestone: {e}")
        else:
            print_warning("Invalid selection, skipping milestone")
    except ValueError:
        print_warning("Invalid input, skipping milestone")


def workflow_backport():
    """Backport workflow: Submit current PR to a historical release branch with auto-rebase"""
    print_header("📦 Backport Workflow")

    # 1. Pre-flight checks
    print_info("Running pre-flight checks...")

    try:
        GitHubOperations.check_gh_cli()
    except Exception as e:
        print_error(str(e))
        sys.exit(1)

    current_branch = GitOperations.get_current_branch()
    if current_branch == "master":
        print_error(
            "Cannot backport from master branch. Please work on a feature branch first."
        )
        sys.exit(1)

    print_success(f"Current branch: {current_branch}")

    # Check for commits
    upstream_master = GitOperations.get_upstream_master()
    commit_count = GitOperations.get_commit_count("master")
    if commit_count == 0:
        print_error("No commits to backport")
        sys.exit(1)

    print_info(f"Found {commit_count} commit(s) to backport")

    # 2. Get the commit(s) to backport
    # Get the commit range from upstream/master to HEAD
    commits = GitOperations.run_command(
        ["git", "log", f"{upstream_master}..HEAD", "--pretty=%H", "--reverse"]
    ).splitlines()

    if not commits:
        print_error("No commits found to backport")
        sys.exit(1)

    print_info("Commits to backport:")
    for commit in commits[:5]:  # Show first 5
        msg = GitOperations.run_command(["git", "log", "-1", "--pretty=%s", commit])
        print(f"  {commit[:8]}: {msg}")
    if len(commits) > 5:
        print(f"  ... and {len(commits) - 5} more")

    # 3. Select target release branch
    print_header("\n🎯 Select Target Branch")

    branches = GitHubOperations.get_release_branches()

    if not branches:
        print_error("No release branches found")
        sys.exit(1)

    print("\nAvailable release branches:")
    for i, b in enumerate(branches):
        # First branch is the most recent (sorted in reverse order)
        label = " (latest)" if i == 0 else ""
        print(f"  {i}: {b}{label}")

    selection = UserInteraction.prompt("Enter branch number (default: 0 for latest):")
    if not selection.strip():
        selection = "0"
    try:
        idx = int(selection)
        if 0 <= idx < len(branches):
            target_branch = branches[idx]
        else:
            print_error("Invalid selection")
            sys.exit(1)
    except ValueError:
        print_error("Invalid input")
        sys.exit(1)

    print_success(f"Target branch: {target_branch}")

    # 4. Create backport branch
    print_header("\n🌿 Creating Backport Branch")

    # Extract version number (e.g., "2.5" from "origin/2.5")
    version_num = target_branch.split("/")[-1]
    backport_branch = f"backport-{version_num}-{current_branch}"

    # Check if branch already exists
    if GitOperations.branch_exists(backport_branch):
        if UserInteraction.confirm(
            f"Branch {backport_branch} already exists. Delete and recreate?"
        ):
            GitOperations.delete_branch(backport_branch, force=True)
        else:
            print_error("Cannot continue with existing branch")
            sys.exit(1)

    # Fetch and create branch from release branch
    upstream_remote = GitOperations.get_upstream_remote()
    print_info(f"Fetching {target_branch}...")
    try:
        GitOperations.fetch(upstream_remote, version_num)
    except Exception as e:
        print_warning(f"Fetch warning: {e}")

    # Create branch from release branch
    print_info(f"Creating branch {backport_branch} from {target_branch}...")
    try:
        GitOperations.run_command(
            ["git", "checkout", "-b", backport_branch, target_branch]
        )
        print_success(f"Created branch: {backport_branch}")
    except Exception as e:
        print_error(f"Failed to create branch: {e}")
        GitOperations.checkout_branch(current_branch)
        sys.exit(1)

    # 5. Cherry-pick commits
    print_header("\n🍒 Cherry-picking Commits")

    cherry_pick_failed = False
    commits_applied = 0
    commits_skipped = 0
    for i, commit in enumerate(commits):
        msg = GitOperations.run_command(["git", "log", "-1", "--pretty=%s", commit])
        print_info(f"Cherry-picking [{i + 1}/{len(commits)}]: {msg[:60]}...")

        success, error_msg, conflict_files = GitOperations.cherry_pick(commit)

        if not success:
            print_error(f"Cherry-pick failed: {error_msg}")
            cherry_pick_failed = True

            if conflict_files:
                print_warning(f"Conflicts in: {', '.join(conflict_files)}")

                # Use AI to analyze conflicts if available
                ai_service = AIService()
                if ai_service.has_api_key:
                    print_info("Analyzing conflicts with AI...")
                    try:
                        analysis = ai_service.analyze_conflict(conflict_files, msg)
                        print(f"\n{Colors.BLUE}AI Analysis:{Colors.RESET}")
                        print(analysis)
                    except Exception:
                        pass

            choice = UserInteraction.select_option(
                "Options:",
                [
                    ("r", "Resolve conflicts manually, then continue"),
                    ("s", "Skip this commit"),
                    ("a", "Abort backport"),
                ],
            )

            if choice == "a":
                print_info("Aborting backport...")
                GitOperations.cherry_pick_abort()
                GitOperations.checkout_branch(current_branch)
                GitOperations.delete_branch(backport_branch, force=True)
                print_warning("Backport aborted, returned to original branch")
                sys.exit(1)
            elif choice == "s":
                print_info("Skipping this commit...")
                GitOperations.cherry_pick_abort()
                commits_skipped += 1
                continue
            elif choice == "r":
                print_info(
                    "Please resolve conflicts, stage changes, then press Enter..."
                )
                input()
                try:
                    GitOperations.cherry_pick_continue()
                    print_success("Cherry-pick continued")
                    commits_applied += 1
                except Exception as e:
                    print_error(f"Failed to continue: {e}")
                    sys.exit(1)
        else:
            print_success(f"Cherry-picked: {commit[:8]}")
            commits_applied += 1

    # Report cherry-pick results
    if commits_applied == len(commits):
        print_success("All commits cherry-picked successfully!")
    elif commits_applied > 0:
        print_warning(
            f"Cherry-picked {commits_applied}/{len(commits)} commits ({commits_skipped} skipped)"
        )
    else:
        print_error("No commits were cherry-picked")
        if UserInteraction.confirm("Abort backport?", default=True):
            GitOperations.checkout_branch(current_branch)
            GitOperations.delete_branch(backport_branch, force=True)
            print_warning("Backport aborted, returned to original branch")
            sys.exit(1)

    # 6. Push to fork
    print_header("\n📤 Pushing to Fork")

    fork_remote, fork_owner = GitOperations.get_fork_info()
    print_info(f"Pushing {backport_branch} to {fork_remote} ({fork_owner})...")

    try:
        GitOperations.push(backport_branch, remote=fork_remote)
        print_success(f"Pushed to {fork_remote}")
    except Exception as e:
        print_warning(f"Push failed: {e}")
        if UserInteraction.confirm("Force push?"):
            GitOperations.push(backport_branch, force=True, remote=fork_remote)
            print_success("Force pushed")
        else:
            sys.exit(1)

    # 7. Create PR
    print_header("\n📝 Creating Backport PR")

    # Get original PR title if exists, or use commit message
    original_title = GitOperations.run_command(
        ["git", "log", "-1", "--pretty=%s", commits[-1]]
    )

    # Create backport PR title
    pr_title = f"[{version_num}] {original_title}"
    if len(pr_title) > 80:
        pr_title = pr_title[:77] + "..."

    print_info(f"PR Title: {pr_title}")

    # Build PR body
    pr_body = f"""## Backport to {version_num}

This PR backports the following commits from `{current_branch}` to `{version_num}`:

"""
    for commit in commits:
        msg = GitOperations.run_command(["git", "log", "-1", "--pretty=%s", commit])
        pr_body += f"- {commit[:8]}: {msg}\n"

    # Allow editing
    if UserInteraction.confirm("Edit PR title/description?"):
        new_title = UserInteraction.prompt(f"PR title [{pr_title}]:")
        if new_title:
            pr_title = new_title
        new_body = UserInteraction.prompt_multiline(
            "PR description (or Enter to keep default):"
        )
        if new_body:
            pr_body = new_body

    print_info("Creating PR...")
    try:
        pr_url = GitHubOperations.create_cherry_pick_pr(
            pr_title, pr_body, backport_branch, version_num
        )
        print_success(f"PR created: {pr_url}")
    except Exception as e:
        print_error(f"Failed to create PR: {e}")
        print_info(
            f"You can create the PR manually from: {fork_owner}/{backport_branch} → milvus-io/milvus:{version_num}"
        )
        sys.exit(1)

    # 8. Done
    print_header("\n✅ Backport Complete!")
    print(f"  Source:  {current_branch}")
    print(f"  Target:  {version_num}")
    print(f"  Branch:  {backport_branch}")
    print(f"  PR:      {pr_url}")

    # Return to original branch
    if UserInteraction.confirm(
        f"\nReturn to original branch ({current_branch})?", default=True
    ):
        GitOperations.checkout_branch(current_branch)
        print_success(f"Switched back to {current_branch}")


def workflow_cherry_pick():
    """Cherry-pick workflow: PR/issue number → cherry-pick → create PR"""
    print_header("🍒 Cherry-Pick Workflow")

    # Initialize context
    ctx = CherryPickContext()

    # Step 1: Pre-flight checks
    ctx.original_branch = _cp_preflight_checks()

    # Step 2: Select PR to cherry-pick
    ctx.pr_details, ctx.commit_sha, ctx.related_issue = _cp_select_pr()

    # Step 3: Get target branch
    ctx.target_branch, ctx.version_num, ctx.upstream_remote = _cp_get_target_branch()

    # Step 4: Setup cherry-pick branch
    ctx.cp_branch = _cp_setup_branch(ctx)

    # Step 5: Execute cherry-pick
    _cp_execute_cherry_pick(ctx)

    # Step 6: Push to fork
    ctx.fork_remote, ctx.fork_owner = _cp_push_to_fork(ctx)

    # Step 7: Create PR
    ctx.pr_url = _cp_create_pr(ctx)

    # Step 8: Set milestone
    _cp_set_milestone(ctx)

    # Done!
    print_header("\n✅ Cherry-Pick Complete!")
    print(f"  Original PR: #{ctx.pr_details['number']}")
    print(f"  Target:      {ctx.target_branch}")
    print(f"  Branch:      {ctx.cp_branch}")
    print(f"  PR:          {ctx.pr_url}")

    # Optionally return to original branch
    if UserInteraction.confirm(
        f"\nReturn to original branch ({ctx.original_branch})?", default=True
    ):
        GitOperations.checkout_branch(ctx.original_branch)
        print_success(f"Switched back to {ctx.original_branch}")


# ============================================================================
# Main Entry Point
# ============================================================================


def main():
    parser = argparse.ArgumentParser(
        description="mgit - Intelligent Git Workflow Tool for Milvus",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  mgit.py --commit       Smart commit workflow
  mgit.py --rebase       Rebase onto master and squash commits
  mgit.py --pr           PR creation workflow
  mgit.py --search       Search GitHub issues and PRs by keyword
  mgit.py --cherry-pick  Cherry-pick PR to release branch (by PR/issue number)
  mgit.py --backport     Backport current branch to release branch
  mgit.py --all          Complete workflow (default)
        """,
    )

    parser.add_argument(
        "--commit", action="store_true", help="Run smart commit workflow"
    )

    parser.add_argument("--pr", action="store_true", help="Run PR creation workflow")

    parser.add_argument(
        "--all",
        action="store_true",
        help="Run complete workflow (rebase + commit + PR)",
    )

    parser.add_argument(
        "--rebase",
        action="store_true",
        help="Run rebase and squash workflow (sync with master, squash commits)",
    )

    parser.add_argument(
        "--cherry-pick",
        action="store_true",
        dest="cherry_pick",
        help="Cherry-pick a merged PR to a release branch",
    )

    parser.add_argument(
        "--search", action="store_true", help="Search GitHub issues and PRs by keyword"
    )

    parser.add_argument(
        "--backport",
        action="store_true",
        help="Backport current branch to a release branch with auto-rebase",
    )

    args = parser.parse_args()

    # Default to --all if no arguments (except cherry-pick, search, backport which are standalone)
    if not (
        args.commit
        or args.pr
        or args.all
        or args.rebase
        or args.cherry_pick
        or args.search
        or args.backport
    ):
        args.all = True

    try:
        print(f"{Colors.BOLD}{Colors.BLUE}")
        print("╔═══════════════════════════════════════╗")
        print("║   mgit - Milvus Git Workflow Tool    ║")
        print("╚═══════════════════════════════════════╝")
        print(f"{Colors.RESET}")

        if args.search:
            workflow_search()
        elif args.cherry_pick:
            workflow_cherry_pick()
        elif args.backport:
            workflow_backport()
        elif args.rebase:
            workflow_rebase()
        elif args.commit:
            workflow_commit()
        elif args.pr:
            workflow_pr()
        elif args.all:
            workflow_all()

    except KeyboardInterrupt:
        print(f"\n\n{Colors.YELLOW}Operation cancelled by user{Colors.RESET}")
        sys.exit(1)
    except Exception as e:
        print_error(f"Unexpected error: {e}")
        import traceback

        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()

#!/usr/bin/env python3
"""
mgit - Intelligent Git Workflow Tool for Milvus

A smart Git workflow automation tool that:
- Generates AI-powered commit messages following Milvus conventions
- Automates fork → issue → PR → cherry-pick workflow
- Syncs with master and squashes commits for clean history
- Ensures DCO compliance

Usage:
    mgit.py --commit     # Smart commit workflow
    mgit.py --rebase     # Rebase onto master and squash commits
    mgit.py --pr         # PR creation workflow
    mgit.py --all        # Complete workflow (rebase + commit + PR)
    mgit.py              # Same as --all
"""

import subprocess
import sys
import os
import json
import argparse
import re
import time
import shutil
import tempfile
from typing import Dict, List, Tuple, Optional
import urllib.request
import urllib.error

# ANSI colors for terminal output
class Colors:
    GREEN = '\033[92m'
    YELLOW = '\033[93m'
    RED = '\033[91m'
    BLUE = '\033[94m'
    RESET = '\033[0m'
    BOLD = '\033[1m'

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
    def run_command(command: List[str], capture_output: bool = True, check: bool = True) -> str:
        """Execute a git command and return output"""
        try:
            result = subprocess.run(
                command,
                check=check,
                text=True,
                stdout=subprocess.PIPE if capture_output else None,
                stderr=subprocess.PIPE if capture_output else None
            )
            return result.stdout.strip() if capture_output else ""
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
        output = GitOperations.run_command(['git', 'status', '--porcelain'])
        staged = []
        unstaged = []

        for line in output.splitlines():
            if not line:
                continue
            status = line[:2]
            filepath = line[3:]

            # Staged changes (first character is not space)
            if status[0] != ' ' and status[0] != '?':
                staged.append(filepath)
            # Unstaged changes
            if status[1] != ' ' or status[0] == '?':
                unstaged.append(filepath)

        return staged, unstaged

    @staticmethod
    def get_staged_diff() -> str:
        """Get diff of staged changes"""
        return GitOperations.run_command(['git', 'diff', '--staged'])

    @staticmethod
    def get_staged_diff_stat() -> str:
        """Get diff statistics"""
        return GitOperations.run_command(['git', 'diff', '--staged', '--stat'])

    @staticmethod
    def stage_files(files: List[str]):
        """Stage specified files"""
        GitOperations.run_command(['git', 'add'] + files)

    @staticmethod
    def stage_all():
        """Stage all changes"""
        GitOperations.run_command(['git', 'add', '-A'])

    @staticmethod
    def commit(message: str):
        """Create commit with DCO signature"""
        # Write message to temporary file using tempfile for security and portability
        with tempfile.NamedTemporaryFile(mode='w', suffix='.txt', delete=False, encoding='utf-8') as f:
            f.write(message)
            msg_file = f.name

        try:
            # Commit with -s flag (auto-adds DCO)
            GitOperations.run_command(['git', 'commit', '-s', '-F', msg_file])
        finally:
            # Clean up
            if os.path.exists(msg_file):
                os.remove(msg_file)

    @staticmethod
    def get_commit_hash(ref: str = 'HEAD') -> str:
        """Get commit hash"""
        return GitOperations.run_command(['git', 'rev-parse', '--short', ref])

    @staticmethod
    def get_current_branch() -> str:
        """Get current branch name"""
        return GitOperations.run_command(['git', 'rev-parse', '--abbrev-ref', 'HEAD'])

    @staticmethod
    def get_last_commit_message() -> str:
        """Get last commit message"""
        return GitOperations.run_command(['git', 'log', '-1', '--pretty=%B'])

    @staticmethod
    def get_commit_count(base_branch: str = 'master') -> int:
        """Get number of commits ahead of base branch (uses upstream remote)"""
        upstream_master = GitOperations.get_upstream_master()
        return GitOperations.get_commit_count_from_ref(upstream_master)

    @staticmethod
    def get_commit_count_from_ref(ref: str) -> int:
        """Get number of commits ahead of a specific ref (e.g., 'upstream/master')"""
        try:
            count = GitOperations.run_command(
                ['git', 'rev-list', '--count', f'{ref}..HEAD'],
                check=False
            )
            return int(count) if count else 0
        except Exception:
            return 0

    @staticmethod
    def push(branch: str, force: bool = False, remote: str = None):
        """Push branch to remote (auto-detects fork if not specified)"""
        if remote is None:
            remote = GitOperations.get_fork_remote()
        cmd = ['git', 'push', remote, branch]
        if force:
            cmd.append('-f')
        GitOperations.run_command(cmd)

    @staticmethod
    def get_fork_remote() -> str:
        """
        Auto-detect the remote that points to user's fork.
        Returns remote name (e.g., 'origin', 'fork')
        """
        # Get current GitHub username
        try:
            username = GitOperations.run_command(['gh', 'api', 'user', '-q', '.login'])
        except Exception:
            username = None

        # Get all remotes
        remotes_output = GitOperations.run_command(['git', 'remote', '-v'])
        remotes = {}
        for line in remotes_output.splitlines():
            parts = line.split()
            if len(parts) >= 2 and '(push)' in line:
                name = parts[0]
                url = parts[1]
                remotes[name] = url

        # Find remote that matches user's fork
        for name, url in remotes.items():
            # Check if URL contains username (github.com/username/ or github.com:username/)
            if username and username.lower() in url.lower():
                return name
            # Check for common fork remote names
            if name in ['fork', 'myfork', 'personal']:
                return name

        # If origin points to milvus-io/milvus, look for another remote
        origin_url = remotes.get('origin', '')
        if 'milvus-io/milvus' in origin_url:
            # Origin is upstream, find fork
            for name, url in remotes.items():
                if name != 'origin' and 'milvus' in url.lower():
                    return name

        # Default to origin
        return 'origin'

    @staticmethod
    def get_fork_info() -> Tuple[str, str]:
        """
        Get fork remote name and owner
        Returns: (remote_name, owner_username)
        """
        remote = GitOperations.get_fork_remote()
        remote_url = GitOperations.run_command(['git', 'remote', 'get-url', remote])

        # Parse username from URL
        # SSH: git@github.com:username/repo.git
        # HTTPS: https://github.com/username/repo.git
        if 'github.com:' in remote_url:
            # SSH format
            owner = remote_url.split('github.com:')[1].split('/')[0]
        elif 'github.com/' in remote_url:
            # HTTPS format
            owner = remote_url.split('github.com/')[1].split('/')[0]
        else:
            # Fallback: try gh api
            owner = GitOperations.run_command(['gh', 'api', 'user', '-q', '.login'])

        return remote, owner

    @staticmethod
    def get_user_info() -> Tuple[str, str]:
        """Get git user name and email"""
        name = GitOperations.run_command(['git', 'config', 'user.name'])
        email = GitOperations.run_command(['git', 'config', 'user.email'])
        return name, email

    @staticmethod
    def create_branch(branch_name: str):
        """Create and checkout a new branch"""
        GitOperations.run_command(['git', 'checkout', '-b', branch_name])

    @staticmethod
    def branch_exists(branch_name: str) -> bool:
        """Check if a branch exists locally"""
        result = GitOperations.run_command(
            ['git', 'rev-parse', '--verify', branch_name],
            check=False
        )
        return bool(result)

    @staticmethod
    def fetch(remote: str = 'origin', branch: str = None):
        """Fetch from remote"""
        cmd = ['git', 'fetch', remote]
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
        remotes_output = GitOperations.run_command(['git', 'remote', '-v'])

        # First, look for a remote pointing to milvus-io/milvus
        for line in remotes_output.splitlines():
            if 'milvus-io/milvus' in line and '(fetch)' in line:
                return line.split()[0]

        # If not found, check common upstream remote names
        remotes = [line.split()[0] for line in remotes_output.splitlines() if '(fetch)' in line]
        for name in ['upstream', 'milvus', 'official']:
            if name in remotes:
                return name

        # Fallback to origin (user should configure upstream properly)
        print_warning("No upstream remote found pointing to milvus-io/milvus")
        print_info("Consider adding: git remote add upstream git@github.com:milvus-io/milvus.git")
        return 'origin'

    @staticmethod
    def get_upstream_master() -> str:
        """Get the upstream master reference (e.g., 'upstream/master')"""
        upstream = GitOperations.get_upstream_remote()
        return f'{upstream}/master'

    @staticmethod
    def rebase(target: str) -> Tuple[bool, str]:
        """
        Rebase current branch onto target
        Returns: (success, error_message)
        """
        try:
            GitOperations.run_command(['git', 'rebase', target])
            return True, ""
        except Exception as e:
            return False, str(e)

    @staticmethod
    def rebase_abort():
        """Abort an in-progress rebase"""
        GitOperations.run_command(['git', 'rebase', '--abort'], check=False)

    @staticmethod
    def is_rebase_in_progress() -> bool:
        """Check if a rebase is in progress"""
        git_dir = GitOperations.run_command(['git', 'rev-parse', '--git-dir'])
        return os.path.exists(os.path.join(git_dir, 'rebase-merge')) or \
               os.path.exists(os.path.join(git_dir, 'rebase-apply'))

    @staticmethod
    def reset_soft(target: str):
        """Soft reset to target (keeps changes staged)"""
        GitOperations.run_command(['git', 'reset', '--soft', target])

    @staticmethod
    def get_all_changes_diff(base: str = None) -> str:
        """Get diff of all changes from base (defaults to upstream master)"""
        if base is None:
            base = GitOperations.get_upstream_master()
        return GitOperations.run_command(['git', 'diff', base])

    @staticmethod
    def get_all_changes_stat(base: str = None) -> str:
        """Get diff statistics from base (defaults to upstream master)"""
        if base is None:
            base = GitOperations.get_upstream_master()
        return GitOperations.run_command(['git', 'diff', '--stat', base])

    @staticmethod
    def get_commit_messages(base: str = None) -> List[str]:
        """Get all commit messages from base to HEAD (defaults to upstream master)"""
        if base is None:
            base = GitOperations.get_upstream_master()
        output = GitOperations.run_command(
            ['git', 'log', f'{base}..HEAD', '--pretty=%B', '--reverse']
        )
        return [msg.strip() for msg in output.split('\n\n') if msg.strip()]


# ============================================================================
# AI Module - Claude and OpenAI API integration
# ============================================================================

class AIService:
    """Handles AI API calls for commit message and PR generation"""

    COMMIT_TYPES = ['fix', 'enhance', 'feat', 'refactor', 'test', 'docs', 'chore']

    def __init__(self):
        # Check for local claude CLI first
        self.has_claude_cli = self._check_claude_cli()

        self.gemini_key = os.getenv('GEMINI_API_KEY')
        self.anthropic_key = os.getenv('ANTHROPIC_API_KEY')
        self.openai_key = os.getenv('OPENAI_API_KEY')

        # Has API key if any key is set OR local claude is available
        self.has_api_key = bool(self.has_claude_cli or self.gemini_key or self.anthropic_key or self.openai_key)

    @staticmethod
    def _check_claude_cli() -> bool:
        """Check if local claude CLI is available"""
        # Use shutil.which() for cross-platform compatibility (works on Windows too)
        return shutil.which('claude') is not None

    def generate_commit_message(self, diff: str, files: List[str], stats: str) -> Dict[str, str]:
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
        max_diff_lines = 500
        diff_lines = diff.splitlines()
        if len(diff_lines) > max_diff_lines:
            truncated_diff = '\n'.join(diff_lines[:max_diff_lines])
            truncated_diff += f"\n\n... (truncated {len(diff_lines) - max_diff_lines} lines)"
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
{chr(10).join(f'  - {f}' for f in files)}

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
                ['claude', '-p', prompt],
                capture_output=True,
                text=True,
                timeout=60
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
            "messages": [
                {"role": "user", "content": prompt}
            ]
        }

        req = urllib.request.Request(
            url,
            data=json.dumps(data).encode('utf-8'),
            headers={
                "x-api-key": self.anthropic_key,
                "anthropic-version": "2023-06-01",
                "content-type": "application/json"
            }
        )

        try:
            with urllib.request.urlopen(req, timeout=30) as response:
                result = json.loads(response.read().decode('utf-8'))
                content = result['content'][0]['text']
                return self._parse_ai_response(content)
        except urllib.error.HTTPError as e:
            error_body = e.read().decode('utf-8')
            raise Exception(f"Claude API HTTP {e.code}: {error_body}")
        except Exception as e:
            raise Exception(f"Claude API error: {e}")

    def _call_openai(self, prompt: str) -> Dict[str, str]:
        """Call OpenAI API"""
        url = "https://api.openai.com/v1/chat/completions"

        data = {
            "model": "gpt-4",
            "messages": [{"role": "user", "content": prompt}],
            "temperature": 0.7
        }

        req = urllib.request.Request(
            url,
            data=json.dumps(data).encode('utf-8'),
            headers={
                "Authorization": f"Bearer {self.openai_key}",
                "Content-Type": "application/json"
            }
        )

        try:
            with urllib.request.urlopen(req, timeout=30) as response:
                result = json.loads(response.read().decode('utf-8'))
                content = result['choices'][0]['message']['content']
                return self._parse_ai_response(content)
        except urllib.error.HTTPError as e:
            error_body = e.read().decode('utf-8')
            raise Exception(f"OpenAI API HTTP {e.code}: {error_body}")
        except Exception as e:
            raise Exception(f"OpenAI API error: {e}")

    def _call_gemini(self, prompt: str) -> Dict[str, str]:
        """Call Google Gemini API"""
        url = f"https://generativelanguage.googleapis.com/v1beta/models/gemini-2.0-flash:generateContent?key={self.gemini_key}"

        data = {
            "contents": [{
                "parts": [{"text": prompt}]
            }],
            "generationConfig": {
                "temperature": 0.7,
                "maxOutputTokens": 2048
            }
        }

        req = urllib.request.Request(
            url,
            data=json.dumps(data).encode('utf-8'),
            headers={
                "Content-Type": "application/json"
            }
        )

        try:
            with urllib.request.urlopen(req, timeout=30) as response:
                result = json.loads(response.read().decode('utf-8'))
                content = result['candidates'][0]['content']['parts'][0]['text']
                return self._parse_ai_response(content)
        except urllib.error.HTTPError as e:
            error_body = e.read().decode('utf-8')
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
        start = content.find('{')
        if start == -1:
            return content

        # Find matching closing brace
        depth = 0
        end = -1
        for i in range(start, len(content)):
            if content[i] == '{':
                depth += 1
            elif content[i] == '}':
                depth -= 1
                if depth == 0:
                    end = i
                    break

        if end != -1:
            return content[start:end+1]

        # Fallback: use last }
        end = content.rfind('}')
        if end > start:
            return content[start:end+1]

        return content

    def _parse_ai_response(self, content: str) -> Dict[str, str]:
        """Parse AI response to extract commit message"""
        content = self._extract_json(content)

        try:
            data = json.loads(content)

            # Validate required fields
            if 'type' not in data or 'title' not in data:
                raise ValueError("Missing required fields")

            # Validate type
            if data['type'] not in self.COMMIT_TYPES:
                print_warning(f"Unknown commit type: {data['type']}, using 'chore'")
                data['type'] = 'chore'

            # Ensure title is ≤80 chars
            if len(data['title']) > 80:
                print_warning(f"Title too long ({len(data['title'])} chars), truncating...")
                data['title'] = data['title'][:77] + '...'

            # Ensure body exists
            if 'body' not in data:
                data['body'] = ''

            return data

        except json.JSONDecodeError as e:
            raise Exception(f"Failed to parse AI response as JSON: {e}\nContent: {content}")

    def generate_issue_content(self, diff: str, files: List[str], stats: str, issue_type: str) -> Dict[str, str]:
        """
        Generate issue title and body using AI

        Returns:
            {
                'title': 'Issue title',
                'body': 'Issue description'
            }
        """
        # Limit diff size
        max_diff_lines = 500
        diff_lines = diff.splitlines()
        if len(diff_lines) > max_diff_lines:
            truncated_diff = '\n'.join(diff_lines[:max_diff_lines])
            truncated_diff += f"\n\n... (truncated {len(diff_lines) - max_diff_lines} lines)"
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
                return self._call_ai_for_issue(prompt, 'gemini')
            except Exception as e:
                print_warning(f"Gemini API failed: {e}")
                if not (self.anthropic_key or self.openai_key):
                    raise

        if self.anthropic_key:
            try:
                return self._call_ai_for_issue(prompt, 'claude')
            except Exception as e:
                print_warning(f"Claude API failed: {e}")
                if not self.openai_key:
                    raise

        if self.openai_key:
            return self._call_ai_for_issue(prompt, 'openai')

        raise Exception("All AI API calls failed")

    def _build_issue_prompt(self, diff: str, files: List[str], stats: str, issue_type: str) -> str:
        """Build prompt for issue content generation"""
        # Map issue type to prefix and description
        type_config = {
            'bug': {'prefix': '[Bug]', 'desc': 'a bug fix'},
            'feature': {'prefix': '[Feature]', 'desc': 'a new feature'},
            'enhancement': {'prefix': '[Enhancement]', 'desc': 'an enhancement to existing functionality'},
            'benchmark': {'prefix': '[Benchmark]', 'desc': 'a benchmark or performance improvement'}
        }
        config = type_config.get(issue_type, {'prefix': '[Feature]', 'desc': 'a change'})
        prefix = config['prefix']
        type_desc = config['desc']

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
{chr(10).join(f'  - {f}' for f in files)}

Statistics: {stats}

Diff:
{diff}

Return ONLY a JSON object with this exact format:
{{"title": "{prefix} Your title here", "body": "Detailed issue description here"}}

IMPORTANT: The title MUST start with "{prefix}".
Make the description professional and informative.
Use markdown formatting in the body where appropriate.
"""

    def _call_ai_for_issue(self, prompt: str, provider: str = 'cli') -> Dict[str, str]:
        """Call AI and parse issue response"""
        if provider == 'cli':
            result = subprocess.run(
                ['claude', '-p', prompt],
                capture_output=True,
                text=True,
                timeout=60
            )
            if result.returncode != 0:
                raise Exception(f"Claude CLI error: {result.stderr}")
            content = result.stdout.strip()
        elif provider == 'gemini':
            url = f"https://generativelanguage.googleapis.com/v1beta/models/gemini-2.0-flash:generateContent?key={self.gemini_key}"
            data = {
                "contents": [{"parts": [{"text": prompt}]}],
                "generationConfig": {"temperature": 0.7, "maxOutputTokens": 2048}
            }
            req = urllib.request.Request(url, data=json.dumps(data).encode('utf-8'),
                                         headers={"Content-Type": "application/json"})
            with urllib.request.urlopen(req, timeout=30) as response:
                result = json.loads(response.read().decode('utf-8'))
                content = result['candidates'][0]['content']['parts'][0]['text']
        elif provider == 'claude':
            url = "https://api.anthropic.com/v1/messages"
            data = {
                "model": "claude-3-5-sonnet-20241022",
                "max_tokens": 1024,
                "messages": [{"role": "user", "content": prompt}]
            }
            req = urllib.request.Request(url, data=json.dumps(data).encode('utf-8'),
                                         headers={
                                             "x-api-key": self.anthropic_key,
                                             "anthropic-version": "2023-06-01",
                                             "content-type": "application/json"
                                         })
            with urllib.request.urlopen(req, timeout=30) as response:
                result = json.loads(response.read().decode('utf-8'))
                content = result['content'][0]['text']
        elif provider == 'openai':
            url = "https://api.openai.com/v1/chat/completions"
            data = {
                "model": "gpt-4",
                "messages": [{"role": "user", "content": prompt}],
                "temperature": 0.7
            }
            req = urllib.request.Request(url, data=json.dumps(data).encode('utf-8'),
                                         headers={
                                             "Authorization": f"Bearer {self.openai_key}",
                                             "Content-Type": "application/json"
                                         })
            with urllib.request.urlopen(req, timeout=30) as response:
                result = json.loads(response.read().decode('utf-8'))
                content = result['choices'][0]['message']['content']
        else:
            raise Exception(f"Unknown provider: {provider}")

        # Parse response using shared extraction method
        content = self._extract_json(content)

        try:
            data = json.loads(content)
            if 'title' not in data or 'body' not in data:
                raise ValueError("Missing required fields")
            return data
        except json.JSONDecodeError as e:
            raise Exception(f"Failed to parse AI response: {e}")


# ============================================================================
# GitHub Module - GitHub CLI operations
# ============================================================================

class GitHubOperations:
    """Handles GitHub CLI operations"""

    @staticmethod
    def check_gh_cli():
        """Check if gh CLI is installed and authenticated"""
        try:
            subprocess.run(['gh', '--version'], check=True, capture_output=True)
        except (subprocess.CalledProcessError, FileNotFoundError):
            raise Exception("GitHub CLI (gh) is not installed. Install from: https://cli.github.com/")

        # Check authentication
        try:
            subprocess.run(['gh', 'auth', 'status'], check=True, capture_output=True)
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
            'gh', 'issue', 'create',
            '--repo', 'milvus-io/milvus',
            '--title', title,
            '--body', body,
            '--label', label
        ]

        output = GitOperations.run_command(cmd)
        # Extract issue number from URL
        issue_number = output.split('/')[-1]
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
            'gh', 'pr', 'create',
            '--repo', 'milvus-io/milvus',
            '--head', f'{fork_owner}:{branch}',
            '--base', 'master',
            '--title', title,
            '--body', body,
            '--label', label
        ]

        return GitOperations.run_command(cmd)

    @staticmethod
    def add_cherry_pick_comment(pr_url: str, target_branch: str):
        """Add cherry-pick comment to PR"""
        cmd = [
            'gh', 'pr', 'comment', pr_url,
            '--body', f'/cherry-pick {target_branch}'
        ]
        GitOperations.run_command(cmd)

    @staticmethod
    def get_release_branches() -> List[str]:
        """Get list of release branches (2.x)"""
        try:
            output = GitOperations.run_command(
                ['git', 'branch', '-r'],
                check=False
            )
            branches = []
            for line in output.splitlines():
                branch = line.strip().replace('origin/', '')
                if branch.startswith('2.'):
                    branches.append(branch)
            return sorted(branches, reverse=True)
        except Exception:
            return []


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
        return response in ['y', 'yes']

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
        return '\n'.join(lines)

    @staticmethod
    def choose_files(files: List[str]) -> List[str]:
        """Interactive file selection"""
        print("\nFiles to stage:")
        for i, f in enumerate(files):
            print(f"  {i}: {f}")

        print("\nEnter numbers (comma-separated) or 'all':")
        choice = input("> ").strip()

        if choice == 'all':
            return files

        try:
            indices = [int(x.strip()) for x in choice.split(',')]
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
    clean_desc = re.sub(r'[^a-zA-Z0-9\s-]', '', description)
    clean_desc = re.sub(r'\s+', '-', clean_desc.strip().lower())
    clean_desc = clean_desc[:30]  # Limit length

    # Add timestamp suffix to ensure uniqueness
    timestamp = int(time.time()) % 10000

    return f"{branch_type}/{clean_desc}-{timestamp}"


def ensure_feature_branch():
    """Ensure we're not on master branch, create new branch if needed"""
    current_branch = GitOperations.get_current_branch()

    if current_branch != 'master':
        print_success(f"Working on branch: {current_branch}")
        return current_branch

    # On master branch - need to create a new branch
    print_warning("You are on the 'master' branch!")
    print_info("Creating a new feature branch is required.")

    choice = UserInteraction.select_option(
        "Choose branch type:",
        [
            ('fix', 'Bug fix'),
            ('feat', 'New feature'),
            ('enhance', 'Enhancement'),
            ('refactor', 'Refactoring'),
            ('test', 'Test changes'),
            ('docs', 'Documentation'),
            ('chore', 'Chore/maintenance'),
        ]
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
    if branch == 'master':
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
                ('c', 'Continue rebase (after resolving conflicts)'),
                ('a', 'Abort rebase'),
            ]
        )
        if choice == 'a':
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
        GitOperations.fetch(upstream_remote, 'master')
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
        ['git', 'log', '--oneline', f'{upstream_master}..HEAD']
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
    new_commit_count = GitOperations.get_commit_count('master')

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
        ['git', 'diff', '--name-only', upstream_master]
    )
    changed_files = [f for f in changed_files_output.splitlines() if f]

    print(f"\n{Colors.BLUE}Changes to be squashed:{Colors.RESET}")
    print(stats)

    # 9. Generate new commit message with AI
    ai_service = AIService()
    full_message = None

    if not ai_service.has_api_key:
        print_warning("No AI API key found.")
        full_message = UserInteraction.prompt_multiline("Enter commit message for squashed commit:")
    else:
        print_info("\nGenerating commit message with AI...")
        try:
            msg_data = ai_service.generate_commit_message(diff, changed_files, stats)
            full_message = f"{msg_data['type']}: {msg_data['title']}"
            if msg_data['body']:
                full_message += f"\n\n{msg_data['body']}"
        except Exception as e:
            print_error(f"AI generation failed: {e}")
            full_message = UserInteraction.prompt_multiline("Enter commit message manually:")

    # 10. Show and confirm message
    print(f"\n{Colors.BOLD}{'='*60}{Colors.RESET}")
    print(f"{Colors.BOLD}Squashed Commit Message:{Colors.RESET}\n")
    print(full_message)
    print(f"\n{Colors.BOLD}{'='*60}{Colors.RESET}")

    choice = UserInteraction.select_option(
        "Options:",
        [
            ('y', 'Accept and squash'),
            ('e', 'Edit message'),
            ('r', 'Regenerate with AI'),
            ('m', 'Enter manually'),
            ('n', 'Cancel (keep multiple commits)'),
        ]
    )

    if choice == 'n':
        print_warning("Squash cancelled - keeping multiple commits")
        return True
    elif choice == 'e':
        with tempfile.NamedTemporaryFile(mode='w', suffix='.txt', delete=False) as f:
            f.write(full_message)
            temp_file = f.name
        editor = os.getenv('EDITOR', 'vi')
        subprocess.run([editor, temp_file])
        with open(temp_file, 'r') as f:
            full_message = f.read().strip()
        os.unlink(temp_file)
    elif choice == 'r':
        return workflow_rebase()  # Restart
    elif choice == 'm':
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
                ('a', 'Stage all files'),
                ('s', 'Select files'),
                ('c', 'Cancel (only commit staged files)'),
            ]
        )

        if choice == 'a':
            GitOperations.stage_all()
            print_success("All files staged")
        elif choice == 's':
            selected = UserInteraction.choose_files(unstaged)
            if selected:
                GitOperations.stage_files(selected)
                print_success(f"Staged {len(selected)} file(s)")
        # choice == 'c': continue with only staged files

    # Re-check staged files
    staged, _ = GitOperations.get_status()
    if not staged:
        print_warning("No staged changes to commit")
        return None

    # 3. Run format tools (optional)
    print_header("\n🛠️  Code Formatting")
    if UserInteraction.confirm("Run code formatting tools (make fmt)?", default=True):
        print_info("Running make fmt...")
        try:
            # Get repository root directory
            repo_root = GitOperations.run_command(['git', 'rev-parse', '--show-toplevel'])
            # Run make fmt
            result = subprocess.run(
                ['make', 'fmt'],
                cwd=repo_root,
                capture_output=True,
                text=True,
                timeout=120
            )
            if result.returncode == 0:
                print_success("Formatting completed")

                # Check if formatting made changes
                _, unstaged_after_fmt = GitOperations.get_status()
                if unstaged_after_fmt:
                    print_warning("Formatting made changes to files")
                    if UserInteraction.confirm("Stage formatting changes?", default=True):
                        GitOperations.stage_all()
                        print_success("Formatting changes staged")
                    else:
                        print_warning("Formatting changes not staged - commit will not include them")
            else:
                print_warning(f"Formatting completed with warnings:\n{result.stderr}")
        except subprocess.TimeoutExpired:
            print_error("Formatting timed out (>120s)")
            if not UserInteraction.confirm("Continue without formatting?"):
                return None
        except Exception as e:
            print_warning(f"Formatting failed: {e}")
            if not UserInteraction.confirm("Continue without formatting?"):
                return None

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
        full_message = UserInteraction.prompt_multiline("Enter commit message manually:")
    else:
        print_info("\nGenerating commit message with AI...")
        try:
            msg_data = ai_service.generate_commit_message(diff, staged, stats)

            # Build full commit message
            full_message = f"{msg_data['type']}: {msg_data['title']}"
            if msg_data['body']:
                full_message += f"\n\n{msg_data['body']}"

        except Exception as e:
            print_error(f"AI generation failed: {e}")
            print_info("Falling back to manual input")
            full_message = UserInteraction.prompt_multiline("Enter commit message:")

    # 5. Review and confirm
    print(f"\n{Colors.BOLD}{'='*60}{Colors.RESET}")
    message_title = "Commit Message:" if not ai_service.has_api_key else "Generated Commit Message:"
    print(f"{Colors.BOLD}{message_title}{Colors.RESET}\n")
    print(full_message)
    print(f"\n{Colors.BOLD}{'='*60}{Colors.RESET}")

    choice = UserInteraction.select_option(
        "Options:",
        [
            ('y', 'Accept and commit'),
            ('e', 'Edit message'),
            ('r', 'Regenerate with AI'),
            ('m', 'Enter manually'),
            ('n', 'Cancel'),
        ]
    )

    if choice == 'n':
        print_warning("Commit cancelled")
        return None
    elif choice == 'e':
        print_info("Opening editor...")
        # Create temp file with message
        with tempfile.NamedTemporaryFile(mode='w', suffix='.txt', delete=False) as f:
            f.write(full_message)
            temp_file = f.name

        # Open in editor
        editor = os.getenv('EDITOR', 'vi')
        subprocess.run([editor, temp_file])

        # Read edited message
        with open(temp_file, 'r') as f:
            full_message = f.read().strip()
        os.unlink(temp_file)
    elif choice == 'r':
        print_info("Regenerating...")
        return workflow_commit()  # Recursive call
    elif choice == 'm':
        full_message = UserInteraction.prompt_multiline("Enter commit message:")

    # 6. Create commit
    try:
        GitOperations.commit(full_message)
        commit_hash = GitOperations.get_commit_hash()
        print_success(f"Commit created: {commit_hash}")

        # 7. Optional code review
        print_header("\n🔍 Code Review")
        if UserInteraction.confirm("Run code review with Claude Code?", default=False):
            # Check if claude CLI is available (cross-platform)
            if shutil.which('claude') is None:
                print_warning("Claude Code CLI not found. Install from: https://claude.com/code")
            else:
                try:
                    print_info("Running code review with Claude Code...")

                    # Run claude with review prompt
                    review_prompt = "Review the most recent commit for potential issues, bugs, or improvements. Focus on code quality, security, and best practices."

                    result = subprocess.run(
                        ['claude', '-p', review_prompt],
                        capture_output=True,
                        text=True,
                        timeout=60
                    )

                    if result.returncode == 0:
                        print_success("Review completed")
                        print(f"\n{Colors.BLUE}Review Results:{Colors.RESET}")
                        print(result.stdout)
                    else:
                        print_warning(f"Review completed with warnings:\n{result.stderr}")

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
    if branch == 'master':
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

    print_warning("Milvus requires all PRs to reference an issue!")

    choice = UserInteraction.select_option(
        "Issue management:",
        [
            ('c', 'Create new issue'),
            ('e', 'Use existing issue number'),
        ]
    )

    issue_number = None
    issue_type = 'feature'

    if choice == 'c':
        # Create new issue - select type first
        issue_type = UserInteraction.select_option(
            "Issue type:",
            [
                ('bug', '[Bug] Bug fix'),
                ('feature', '[Feature] New feature'),
                ('enhancement', '[Enhancement] Enhancement'),
                ('benchmark', '[Benchmark] Performance/Benchmark'),
            ]
        )

        # Get diff for AI analysis (compare against upstream milvus-io/milvus)
        upstream_master = GitOperations.get_upstream_master()
        diff = GitOperations.get_all_changes_diff(upstream_master)
        stats = GitOperations.get_all_changes_stat(upstream_master)
        changed_files = GitOperations.run_command(
            ['git', 'diff', '--name-only', upstream_master]
        ).splitlines()

        # Generate issue content with AI
        ai_service = AIService()
        issue_title = None
        issue_body = None

        if ai_service.has_api_key:
            print_info("Generating issue content with AI...")
            try:
                issue_data = ai_service.generate_issue_content(diff, changed_files, stats, issue_type)
                issue_title = issue_data['title']
                issue_body = issue_data['body']
            except Exception as e:
                print_warning(f"AI generation failed: {e}")

        # Show and confirm issue content
        if issue_title and issue_body:
            print(f"\n{Colors.BOLD}{'='*60}{Colors.RESET}")
            print(f"{Colors.BOLD}Generated Issue:{Colors.RESET}\n")
            print(f"{Colors.BLUE}Title:{Colors.RESET} {issue_title}")
            print(f"\n{Colors.BLUE}Description:{Colors.RESET}\n{issue_body}")
            print(f"\n{Colors.BOLD}{'='*60}{Colors.RESET}")

            issue_choice = UserInteraction.select_option(
                "Options:",
                [
                    ('y', 'Accept and create issue'),
                    ('e', 'Edit content'),
                    ('r', 'Regenerate with AI'),
                    ('m', 'Enter manually'),
                ]
            )

            if issue_choice == 'e':
                content = f"Title: {issue_title}\n\n---\n\n{issue_body}"
                with tempfile.NamedTemporaryFile(mode='w', suffix='.md', delete=False) as f:
                    f.write(content)
                    temp_file = f.name
                editor = os.getenv('EDITOR', 'vi')
                subprocess.run([editor, temp_file])
                with open(temp_file, 'r') as f:
                    edited = f.read()
                os.unlink(temp_file)
                # Parse edited content
                if '---' in edited:
                    parts = edited.split('---', 1)
                    issue_title = parts[0].replace('Title:', '').strip()
                    issue_body = parts[1].strip()
                else:
                    lines = edited.strip().split('\n', 1)
                    issue_title = lines[0].replace('Title:', '').strip()
                    issue_body = lines[1].strip() if len(lines) > 1 else ''
            elif issue_choice == 'r':
                # Regenerate - recursive call will handle it
                print_info("Regenerating...")
                try:
                    issue_data = ai_service.generate_issue_content(diff, changed_files, stats, issue_type)
                    issue_title = issue_data['title']
                    issue_body = issue_data['body']
                except Exception as e:
                    print_error(f"Regeneration failed: {e}")
                    issue_title = UserInteraction.prompt("Issue title:")
                    issue_body = UserInteraction.prompt_multiline("Issue description:")
            elif issue_choice == 'm':
                issue_title = UserInteraction.prompt("Issue title:")
                issue_body = UserInteraction.prompt_multiline("Issue description:")
        else:
            # No AI available or failed - manual input
            issue_title = UserInteraction.prompt("Issue title:")
            issue_body = UserInteraction.prompt_multiline("Issue description:")

        print_info("Creating issue in milvus-io/milvus...")
        try:
            issue_number = GitHubOperations.create_issue(issue_title, issue_body, issue_type)
            print_success(f"Issue created: #{issue_number}")
        except Exception as e:
            print_error(f"Failed to create issue: {e}")
            sys.exit(1)

    elif choice == 'e':
        issue_number = UserInteraction.prompt("Enter issue number:")
        issue_type = UserInteraction.select_option(
            "Issue type:",
            [
                ('bug', '[Bug] Bug fix'),
                ('feature', '[Feature] New feature'),
                ('enhancement', '[Enhancement] Enhancement'),
                ('benchmark', '[Benchmark] Performance/Benchmark'),
            ]
        )
        print_success(f"Using issue #{issue_number}")

    # 4. Create PR
    print_header("\n🔀 Pull Request")

    # Get PR title from last commit
    default_title = last_msg.split('\n')[0]
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
    pr_body += "\n\nSigned-off-by: (Checked)"

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

            selection = UserInteraction.prompt("Enter numbers (comma-separated) or Enter to skip:")

            if selection:
                try:
                    indices = [int(x.strip()) for x in selection.split(',')]
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
    if branch != 'master':
        # Always compare against upstream (milvus-io/milvus), not origin fork
        upstream_remote = GitOperations.get_upstream_remote()
        upstream_master = GitOperations.get_upstream_master()
        commit_count = GitOperations.get_commit_count('master')
        if commit_count > 0:
            print_info(f"Found {commit_count} commit(s) - checking if rebase needed...")

            # Fetch from upstream to check if we're behind
            try:
                GitOperations.fetch(upstream_remote, 'master')
            except Exception:
                pass

            # Check if there are upstream changes
            behind_count = GitOperations.run_command(
                ['git', 'rev-list', '--count', f'HEAD..{upstream_master}'],
                check=False
            )

            if behind_count and int(behind_count) > 0:
                print_warning(f"Branch is {behind_count} commit(s) behind {upstream_master}")
                if UserInteraction.confirm("Run rebase workflow first?", default=True):
                    if not workflow_rebase():
                        print_warning("Rebase workflow failed or cancelled")
                        if not UserInteraction.confirm("Continue to PR anyway?"):
                            return
                    print("")
            elif commit_count > 1:
                print_warning(f"You have {commit_count} commits (Milvus prefers single commit)")
                if UserInteraction.confirm("Squash commits?", default=True):
                    if not workflow_rebase():
                        print_warning("Squash workflow failed or cancelled")
                        if not UserInteraction.confirm("Continue to PR anyway?"):
                            return
                    print("")

    # Proceed to PR
    workflow_pr()


# ============================================================================
# Main Entry Point
# ============================================================================

def main():
    parser = argparse.ArgumentParser(
        description='mgit - Intelligent Git Workflow Tool for Milvus',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  mgit.py --commit     Smart commit workflow
  mgit.py --rebase     Rebase onto master and squash commits
  mgit.py --pr         PR creation workflow
  mgit.py --all        Complete workflow (default)
        """
    )

    parser.add_argument(
        '--commit',
        action='store_true',
        help='Run smart commit workflow'
    )

    parser.add_argument(
        '--pr',
        action='store_true',
        help='Run PR creation workflow'
    )

    parser.add_argument(
        '--all',
        action='store_true',
        help='Run complete workflow (rebase + commit + PR)'
    )

    parser.add_argument(
        '--rebase',
        action='store_true',
        help='Run rebase and squash workflow (sync with master, squash commits)'
    )

    args = parser.parse_args()

    # Default to --all if no arguments
    if not (args.commit or args.pr or args.all or args.rebase):
        args.all = True

    try:
        print(f"{Colors.BOLD}{Colors.BLUE}")
        print("╔═══════════════════════════════════════╗")
        print("║   mgit - Milvus Git Workflow Tool    ║")
        print("╚═══════════════════════════════════════╝")
        print(f"{Colors.RESET}")

        if args.rebase:
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

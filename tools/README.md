# Milvus Development Tools

## mgit.py - Intelligent Git Workflow Tool

`mgit.py` is an intelligent Git workflow tool designed to streamline commit and PR processes for Milvus development.

### Features

- 🤖 **AI-Powered Commit Messages** - Automatically generates commit messages following Milvus conventions
- ✅ **Automatic DCO Signing** - Ensures compliance with Developer Certificate of Origin
- 🌿 **Auto Branch Creation** - Prevents commits to master, creates feature branches automatically
- 🔄 **Complete PR Workflow** - fork → branch → commit → issue → PR → cherry-pick
- 🛠️ **Code Formatting** - Runs local format tools before commit
- 📝 **Interactive Controls** - Flexible step-by-step workflow

### Prerequisites

1. **Install GitHub CLI**
   ```bash
   # macOS
   brew install gh

   # Linux
   # See: https://cli.github.com/
   ```

2. **Authenticate GitHub CLI**
   ```bash
   gh auth login
   ```

3. **Configure AI API Key** (Optional - choose one or none)
   ```bash
   # Gemini API (recommended)
   export GEMINI_API_KEY=AIzaSy...

   # Claude API
   export ANTHROPIC_API_KEY=sk-ant-...

   # OpenAI API
   export OPENAI_API_KEY=sk-...
   ```

   Add to `~/.bashrc` or `~/.zshrc` for persistence:
   ```bash
   echo 'export GEMINI_API_KEY=your-key-here' >> ~/.zshrc
   ```

   **Note:** If you have local `claude` CLI installed, the tool will use it instead of API calls.

### Usage

#### 1. Smart Commit (create commit only)

```bash
python3 tools/mgit.py --commit
```

**Workflow:**
1. Check current branch (auto-create feature branch if on master)
2. Detect unstaged files, prompt to stage
3. Run code formatting tools (optional)
4. Analyze code changes (git diff)
5. AI generates Milvus-compliant commit message
6. Review generated message with options:
   - `y` Accept and commit
   - `e` Edit in $EDITOR
   - `r` Regenerate with AI
   - `m` Manual input
   - `n` Cancel
7. Auto-add DCO signature and create commit
8. Optionally run code review

**Example Output:**
```
Generated Commit Message:
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
enhance: optimize planparserv2 grammar and use SLL prediction

1. Reordered 'expr' alternatives to prioritize common patterns
2. Implemented SLL-first parsing for better performance
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

Options:
  [y] Accept and commit
  [e] Edit in $EDITOR
  [r] Regenerate with AI
  [m] Enter manually
  [n] Cancel
```

#### 2. Create PR (assumes commits exist)

```bash
python3 tools/mgit.py --pr
```

**Workflow:**
1. Check current branch, commit count, DCO signature
2. Push to your fork (origin)
3. Create or link GitHub Issue:
   - Create new issue
   - Use existing issue number
   - Skip (not recommended - Milvus requires issue reference)
4. Create PR to `milvus-io/milvus` with required issue reference
5. Optional: Cherry-pick to release branches (2.6, 2.5, etc.)

**Important:** All Milvus PRs must include an issue reference in the format:
```
issue: #39157
```

For cherry-pick PRs, include the original PR number:
```
issue: #39157
pr: #39200
```

#### 3. Complete Workflow (Commit + PR)

```bash
python3 tools/mgit.py --all
# or simply
python3 tools/mgit.py
```

Automatically executes: create commit → create PR.

### Setup Alias (Recommended)

Create a shell alias for convenience:

```bash
# Add to ~/.bashrc or ~/.zshrc
# Option 1: Dynamic path (works from any directory within the repo)
alias mgit='python3 "$(git rev-parse --show-toplevel)/tools/mgit.py"'

# Option 2: Fixed path (replace with your actual Milvus repo location)
# alias mgit='python3 /path/to/your/milvus/tools/mgit.py'
```

After reloading, use directly:
```bash
mgit --commit
mgit --pr
mgit  # complete workflow
```

### AI-Generated Commit Message Format

Follows Milvus conventions:

**Format:** `<type>: <summary>`

**Types:**
- `fix`: Bug fixes
- `enhance`: Improvements to existing features
- `feat`: New features
- `refactor`: Code refactoring
- `test`: Add or modify tests
- `docs`: Documentation updates
- `chore`: Build/tool changes

**Requirements:**
- Title ≤ 80 characters
- Use imperative mood (e.g., "Fix bug" not "Fixed bug")
- Optional: Detailed body explanation

**Examples:**
```
fix: Fix missing handling of FlushAllMsg in recovery storage

enhance: optimize jieba and lindera analyzer clone

feat: Add semantic highlight

test: Add planparserv2 benchmarks
```

### Branch Naming Convention

When creating a new branch from master, the tool generates names in the format:
```
{type}/{description}-{timestamp}
```

**Examples:**
- `fix/memory-leak-1234`
- `feat/add-gemini-api-5678`
- `enhance/optimize-parser-9012`

### Code Formatting

Before committing, the tool offers to run Milvus format tools:
```bash
make fmt           # Format Go code
make static-check  # Run linters (optional)
```

You can choose to:
- Run formatting and continue
- Skip formatting
- Cancel commit

### Code Review

After committing, you can optionally run a local code review using Claude Code (if installed):
```bash
claude -p "Review the recent changes for potential issues"
```

### FAQ

**Q: What if AI API call fails?**
A: The script auto-falls back to manual input mode.

**Q: How to modify generated commit message?**
A: Choose `e` to open in $EDITOR, or `m` for manual input.

**Q: Multiple commits on branch?**
A: Milvus typically requires a single squashed commit. Tool will warn and suggest:
```bash
git rebase -i origin/master
```

**Q: Can I use without API keys?**
A: Yes, but AI generation won't be available. You'll enter messages manually.

**Q: How to test without actually committing?**
A: Choose `n` at the confirmation step.

**Q: What if I'm on master branch?**
A: The tool will prompt you to create a new feature branch automatically.

### Advanced Usage

**Stage specific files only:**
```bash
python3 tools/mgit.py --commit
# Select 's' at file selection, then enter: 0,2,5
```

**Use custom editor:**
```bash
export EDITOR=vim  # or nano, emacs, etc.
python3 tools/mgit.py --commit
# Choose 'e' to edit
```

**Cherry-pick to multiple branches:**
```bash
python3 tools/mgit.py --pr
# At cherry-pick step, enter: 0,1,2
```

**Use local Claude Code instead of API:**
```bash
# If 'claude' command is available, tool will use it automatically
# No API key needed
```

### Troubleshooting

**GitHub CLI not authenticated:**
```
✗ GitHub CLI not authenticated. Run: gh auth login
```
Solution: Run `gh auth login` and follow prompts

**Git user info not configured:**
```
✗ Git user name/email not configured
```
Solution:
```bash
git config --global user.name "Your Name"
git config --global user.email "your.email@example.com"
```

**API rate limit:**
Wait and retry, or switch to another AI provider (Gemini ↔ Claude ↔ OpenAI)

**Format tools not found:**
Ensure you're in the Milvus repository root, or skip formatting step

### AI Provider Priority

1. **Local Claude Code** (if `claude` CLI available) - no API needed
2. **Gemini API** (if `GEMINI_API_KEY` set)
3. **Claude API** (if `ANTHROPIC_API_KEY` set)
4. **OpenAI API** (if `OPENAI_API_KEY` set)
5. **Manual Input** (if no AI available)

### Contributing

For improvements to `mgit.py`, refer to the design document: `docs/plans/2026-01-10-mgit-design.md`

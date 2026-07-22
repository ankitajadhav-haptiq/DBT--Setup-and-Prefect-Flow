#!/usr/bin/env bash
# custom_agent/setup.sh — one-command setup
#
# Usage:
#   bash custom_agent/setup.sh            # install everything
#   bash custom_agent/setup.sh --ollama   # also pull llama3 model via Ollama
#   bash custom_agent/setup.sh --no-hooks # skip git hook installation

set -euo pipefail

REPO_ROOT="$(git rev-parse --show-toplevel 2>/dev/null || pwd)"
HOOKS_DIR="$REPO_ROOT/.git/hooks"
AGENT_DIR="$REPO_ROOT/custom_agent"
MEMORY_DIR="$AGENT_DIR/memory"

GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m'

info()  { echo -e "${GREEN}[setup]${NC} $*"; }
warn()  { echo -e "${YELLOW}[setup]${NC} $*"; }
error() { echo -e "${RED}[setup]${NC} $*"; }

# ── Parse flags ──────────────────────────────────────────────────────────────
PULL_OLLAMA=false
SKIP_HOOKS=false
for arg in "$@"; do
    case "$arg" in
        --ollama)    PULL_OLLAMA=true ;;
        --no-hooks)  SKIP_HOOKS=true  ;;
    esac
done

# ── Python check ─────────────────────────────────────────────────────────────
PYTHON="$(command -v python3 || command -v python || true)"
if [[ -z "$PYTHON" ]]; then
    error "Python 3 not found. Please install Python 3.9+."
    exit 1
fi
PYVER=$("$PYTHON" -c "import sys; print(f'{sys.version_info.major}.{sys.version_info.minor}')")
info "Python $PYVER found at $PYTHON"

# ── Install Python dependencies ───────────────────────────────────────────────
info "Installing custom_agent dependencies..."
"$PYTHON" -m pip install --quiet -r "$AGENT_DIR/requirements.txt"

info "Installing audit_agent dependencies..."
"$PYTHON" -m pip install --quiet -r "$REPO_ROOT/audit_agent/requirements.txt" 2>/dev/null || \
    warn "audit_agent/requirements.txt not found — skipping."

# ── Create memory directory ───────────────────────────────────────────────────
mkdir -p "$MEMORY_DIR"
info "Memory directory: $MEMORY_DIR"

# ── .gitignore entries ────────────────────────────────────────────────────────
GITIGNORE="$REPO_ROOT/.gitignore"
declare -a ENTRIES=(
    "# custom_agent runtime files"
    ".agent_pre_commit_report.md"
    ".agent_pre_commit_report.html"
    ".agent_pre_commit_report_suggestions.json"
    ".agent_pre_push_report.md"
    ".agent_pre_push_report.html"
    ".agent_pre_push_report_suggestions.json"
    ".agent_pre_commit.log"
    "agent_report.md"
    "agent_report.html"
    "agent_pr_report.md"
    "agent_pr_report.html"
    "agent_pr_report_suggestions.json"
    "custom_agent/memory/*.json"
    "audit_workspace/"
)

info "Updating .gitignore..."
for entry in "${ENTRIES[@]}"; do
    if ! grep -qxF "$entry" "$GITIGNORE" 2>/dev/null; then
        echo "$entry" >> "$GITIGNORE"
    fi
done

# ── Install git hooks ──────────────────────────────────────────────────────────
if [[ "$SKIP_HOOKS" == "true" ]]; then
    warn "Skipping git hook installation (--no-hooks)."
else
    info "Installing git hooks..."
    mkdir -p "$HOOKS_DIR"

    for hook in pre-commit pre-push commit-msg; do
        SRC="$AGENT_DIR/git_integration/$hook"
        DEST="$HOOKS_DIR/$hook"

        if [[ ! -f "$SRC" ]]; then
            warn "$SRC not found — skipping $hook hook."
            continue
        fi

        if [[ -f "$DEST" ]] && ! grep -q "custom_agent" "$DEST" 2>/dev/null; then
            warn "$hook hook already exists (not from custom_agent). Backing up to $DEST.bak"
            cp "$DEST" "$DEST.bak"
        fi

        cp "$SRC" "$DEST"
        chmod +x "$DEST"
        info "  ✓ $hook"
    done
fi

# ── Ollama setup ───────────────────────────────────────────────────────────────
if [[ "$PULL_OLLAMA" == "true" ]]; then
    if command -v ollama &>/dev/null; then
        info "Pulling llama3 model (this may take a few minutes)..."
        ollama pull llama3
    else
        warn "Ollama binary not found. Install from https://ollama.ai and re-run with --ollama"
    fi
fi

# ── Smoke test ─────────────────────────────────────────────────────────────────
info "Running smoke test..."
"$PYTHON" -c "
import sys
sys.path.insert(0, '$REPO_ROOT')
from custom_agent import CustomAgent, ALL_TOOLS, build_llm, AgentMemory
print('  imports OK')
llm = build_llm(prefer_ollama=False)
print(f'  LLM: {type(llm).__name__}')
print('  smoke test PASSED')
"

# ── Summary ────────────────────────────────────────────────────────────────────
echo ""
echo -e "${GREEN}╔══════════════════════════════════════════════════════════╗${NC}"
echo -e "${GREEN}║  custom_agent setup complete                              ║${NC}"
echo -e "${GREEN}╚══════════════════════════════════════════════════════════╝${NC}"
echo ""
echo "Quick start:"
echo "  python custom_agent/main.py                    # scan staged changes"
echo "  python custom_agent/main.py --mode react       # use Ollama LLM"
echo "  python custom_agent/main.py --trace            # print reasoning steps"
echo "  python custom_agent/main.py --dismiss SQL-Q002 # mark false positive"
echo ""
if [[ "$SKIP_HOOKS" != "true" ]]; then
    echo "Git hooks installed:"
    echo "  pre-commit  — blocks CRITICAL findings before every commit"
    echo "  pre-push    — full scan before push"
    echo "  commit-msg  — validates commit message format"
    echo ""
    echo "Emergency bypass:  SKIP_AGENT=1 git commit -m \"...\""
fi

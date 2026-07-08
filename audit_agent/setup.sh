#!/usr/bin/env bash
# ──────────────────────────────────────────────────────────────────────────────
# setup.sh — One-command setup for the audit agent.
# Run from the repo root:  bash audit_agent/setup.sh
# ──────────────────────────────────────────────────────────────────────────────

set -euo pipefail

REPO_ROOT="$(git rev-parse --show-toplevel)"
AGENT_DIR="$REPO_ROOT/audit_agent"
PYTHON="${PYTHON:-python3}"
PIP="${PIP:-pip3}"

echo ""
echo "┌─────────────────────────────────────────────────────┐"
echo "│  Audit Agent — Setup                                │"
echo "└─────────────────────────────────────────────────────┘"

# ── 1. Python dependencies ────────────────────────────────────────────────
echo ""
echo "[1/4] Installing Python dependencies..."
"$PIP" install --quiet -r "$AGENT_DIR/requirements.txt"
echo "      ✅ Python packages installed"

# ── 2. Optional system tools ──────────────────────────────────────────────
echo ""
echo "[2/4] Checking optional system tools..."

if command -v gitleaks &>/dev/null; then
    echo "      ✅ gitleaks $(gitleaks version) found"
else
    echo "      ℹ  gitleaks not found — install from https://github.com/gitleaks/gitleaks/releases"
    echo "         Or: brew install gitleaks"
fi

if command -v ollama &>/dev/null; then
    echo "      ✅ ollama found — pull llama3 for AI mode: ollama pull llama3"
else
    echo "      ℹ  ollama not found — AI mode (--mode full) will be skipped"
    echo "         Install: curl -fsSL https://ollama.com/install.sh | sh"
fi

# ── 3. Create audit_workspace directory ───────────────────────────────────
echo ""
echo "[3/4] Creating audit workspace..."
mkdir -p "$REPO_ROOT/audit_workspace"
# Add to gitignore if not already there
GITIGNORE="$REPO_ROOT/.gitignore"
if [ -f "$GITIGNORE" ]; then
    for pattern in "audit_workspace/" "audit_report.md" "*.p8" "*.pem" ".env"; do
        if ! grep -qF "$pattern" "$GITIGNORE"; then
            echo "$pattern" >> "$GITIGNORE"
            echo "      Added '$pattern' to .gitignore"
        fi
    done
fi
echo "      ✅ audit_workspace/ ready"

# ── 4. Install git pre-commit hook ────────────────────────────────────────
echo ""
echo "[4/4] Installing git pre-commit hook..."
HOOK_SOURCE="$AGENT_DIR/hooks/pre-commit"
HOOK_DEST="$REPO_ROOT/.git/hooks/pre-commit"

if [ -f "$HOOK_DEST" ]; then
    echo "      ⚠  pre-commit hook already exists — backing up to pre-commit.bak"
    cp "$HOOK_DEST" "$HOOK_DEST.bak"
fi

cp "$HOOK_SOURCE" "$HOOK_DEST"
chmod +x "$HOOK_DEST"
echo "      ✅ pre-commit hook installed at .git/hooks/pre-commit"

# ── Done ──────────────────────────────────────────────────────────────────
echo ""
echo "┌─────────────────────────────────────────────────────┐"
echo "│  Setup complete!                                     │"
echo "│                                                      │"
echo "│  Run a scan now:                                     │"
echo "│    python audit_agent/main.py .                      │"
echo "│                                                      │"
echo "│  Pre-commit hook will run automatically on:          │"
echo "│    git commit (blocks CRITICAL findings)             │"
echo "│                                                      │"
echo "│  GitHub Actions will run on every PR to master.      │"
echo "└─────────────────────────────────────────────────────┘"
echo ""

#!/bin/bash
set -e

REPO_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
SCRIPTS_DIR="$REPO_ROOT/scripts"
API_DIR="$REPO_ROOT/api"

echo "→ Copying common.py from scripts/ to api/..."
cp "$SCRIPTS_DIR/common.py" "$API_DIR/common.py"

echo "→ Staging and committing..."
git -C "$REPO_ROOT" add "$API_DIR/common.py"

if git -C "$REPO_ROOT" diff --cached --quiet; then
    echo "  (no changes to commit)"
else
    git -C "$REPO_ROOT" commit -m "sync common.py to api/"
fi

echo "→ Pushing to git (triggers Fly deploy)..."
git -C "$REPO_ROOT" push

echo "✅ Done — Fly will redeploy automatically"
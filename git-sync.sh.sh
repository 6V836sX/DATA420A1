#!/usr/bin/env bash
set -euo pipefail

# ========== CONFIG (edit if needed) ==========
REMOTE_DEFAULT="git@github.com:6V836sX/DATA420A1.git"
BRANCH_DEFAULT="main"
USER_NAME_DEFAULT="6V836sX"
USER_EMAIL_DEFAULT="yxi75@uclive.ac.nz"
# ============================================

REMOTE="${REMOTE:-$REMOTE_DEFAULT}"
BRANCH="${BRANCH:-$BRANCH_DEFAULT}"
MSG="${1:-}"   # commit message from first arg, optional

# 1) ensure we are in a git repo
if ! git rev-parse --is-inside-work-tree >/dev/null 2>&1; then
  echo "[init] not a git repo — initializing..."
  git init
fi

# 2) set branch name (main) if needed
current_branch="$(git symbolic-ref --short -q HEAD || true)"
if [[ -z "$current_branch" || "$current_branch" == "master" ]]; then
  echo "[branch] switching to $BRANCH"
  git branch -M "$BRANCH"
fi

# 3) set identity (local, to avoid affecting other repos)
if ! git config --local user.name >/dev/null; then
  git config user.name  "$USER_NAME_DEFAULT"
fi
if ! git config --local user.email >/dev/null; then
  git config user.email "$USER_EMAIL_DEFAULT"
fi

# 4) .gitignore (create minimal one if missing)
if [[ ! -f .gitignore ]]; then
  cat > .gitignore <<'EOF'
# jupyter
.ipynb_checkpoints/
.jupyter/
jupyter_files/
# big outputs / caches
*.parquet
*.csv
*.tsv
*.feather
*.orc
*.avro
*.zip
*.gz
*.xz
# spark / tmp / logs
spark-warehouse/
derby.log
*.crc
# core dumps
core.*
# OS/editor
.DS_Store
Thumbs.db
EOF
  echo "[gitignore] created .gitignore"
fi

# 5) add remote 'origin' if missing
if ! git remote get-url origin >/dev/null 2>&1; then
  echo "[remote] adding origin -> $REMOTE"
  git remote add origin "$REMOTE"
fi

# 6) pull latest (rebase) to avoid diverging histories
echo "[pull] pulling latest from $BRANCH"
git pull --rebase origin "$BRANCH" || true

# 7) stage only notebooks by default (safe); set ADD_ALL=1 to add everything
if [[ "${ADD_ALL:-0}" == "1" ]]; then
  echo "[stage] adding ALL changes"
  git add -A
else
  echo "[stage] adding notebooks (*.ipynb) only"
  # stage modified + untracked notebooks
  git ls-files -o -m --exclude-standard | grep -E '\.ipynb$' | xargs -r git add
fi

# 8) commit (allow empty to no-op quietly)
if [[ -z "$MSG" ]]; then
  MSG="sync from cluster: $(date -u +'%Y-%m-%d %H:%M:%S UTC')"
fi
git commit -m "$MSG" || echo "[commit] nothing to commit"

# 9) push
echo "[push] pushing to origin/$BRANCH"
git push -u origin "$BRANCH"

echo "[done] ✔ synced with $REMOTE on branch $BRANCH"
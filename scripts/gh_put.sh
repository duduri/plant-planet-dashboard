#!/bin/bash
# gh_put.sh — upload a local file to a GitHub repo path via Contents API
# usage: gh_put.sh <local_file> <repo_path> [commit_message]
set -euo pipefail

LOCAL_FILE="${1:?local file required}"
REPO_PATH="${2:?repo path required}"
MSG="${3:-Update $(basename "$REPO_PATH")}"

REPO="duduri/plant-planet-dashboard"
BRANCH="main"

# Find token — prefer real NFD mount, fallback to sandbox dashboard_sync
TOKEN=""
for cand in \
  /sessions/great-pensive-feynman/mnt/00_*/dashboard_sync/.gh_token \
  /sessions/great-pensive-feynman/work/.gh_token; do
  if [ -f "$cand" ]; then
    TOKEN=$(tr -d '[:space:]' < "$cand")
    break
  fi
done
[ -z "$TOKEN" ] && { echo "ERROR: no .gh_token found" >&2; exit 1; }
[ -f "$LOCAL_FILE" ] || { echo "ERROR: $LOCAL_FILE not found" >&2; exit 1; }

API="https://api.github.com/repos/$REPO/contents/$REPO_PATH"

# Get current SHA (required for updates; absent for creation)
SHA=$(curl -s -H "Authorization: token $TOKEN" "$API?ref=$BRANCH" \
      | python3 -c "import sys,json; d=json.load(sys.stdin); print(d.get('sha') or '')" 2>/dev/null || echo "")

# Base64-encode content (no wrapping)
B64=$(base64 -w0 "$LOCAL_FILE" 2>/dev/null || base64 "$LOCAL_FILE" | tr -d '\n')

# Build payload
PAYLOAD=$(python3 -c "
import json, sys
p = {'message': '''$MSG''', 'content': '''$B64''', 'branch': '$BRANCH'}
sha = '''$SHA'''
if sha: p['sha'] = sha
print(json.dumps(p))
")

RESP=$(curl -s -X PUT -H "Authorization: token $TOKEN" \
  -H "Accept: application/vnd.github+json" \
  -d "$PAYLOAD" "$API")

COMMIT=$(echo "$RESP" | python3 -c "import sys,json; d=json.load(sys.stdin); c=d.get('commit',{}); print(c.get('sha','')[:7] if c else 'ERR: '+str(d.get('message','')))")
echo "[gh_put] $REPO_PATH → $COMMIT"

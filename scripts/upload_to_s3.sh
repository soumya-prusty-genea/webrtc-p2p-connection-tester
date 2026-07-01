#!/usr/bin/env bash
#
# Upload the webrtc-connection-tester-v2 source tree to S3.
#
# It first CLEANS the destination folder, then uploads the project files
# (excluding build artifacts, local secrets, and state).
#
# Credentials: do NOT hardcode them here. Export them before running, e.g.
#   export AWS_ACCESS_KEY_ID=...
#   export AWS_SECRET_ACCESS_KEY=...
#   export AWS_DEFAULT_REGION=us-east-1
# (or configure an AWS profile and pass AWS_PROFILE=...).

set -euo pipefail

# Destination bucket folder (override with S3_DEST env var if needed).
S3_DEST="${S3_DEST:-s3://asteriskcallrec/webrtc-connection-tester-tool/}"

# Resolve the project root (this script lives in <root>/scripts).
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"

if ! command -v aws >/dev/null 2>&1; then
  echo "ERROR: aws CLI not found. Install it: https://docs.aws.amazon.com/cli/latest/userguide/getting-started-install.html" >&2
  exit 1
fi

echo "Project root : ${PROJECT_ROOT}"
echo "Destination  : ${S3_DEST}"

# Sanity-check credentials/permissions early.
aws sts get-caller-identity >/dev/null

echo
echo "==> Step 1/2: cleaning destination folder ..."
aws s3 rm "${S3_DEST}" --recursive || true

echo
echo "==> Step 2/2: uploading project files ..."
aws s3 sync "${PROJECT_ROOT}/" "${S3_DEST}" \
  --delete \
  --exclude ".git/*" \
  --exclude "target/*" \
  --exclude "**/target/*" \
  --exclude ".env.local" \
  --exclude "*.rs.bk" \
  --exclude "sfu_state.json" \
  --exclude "test-reports/*"

echo
echo "Done. Uploaded ${PROJECT_ROOT} -> ${S3_DEST}"

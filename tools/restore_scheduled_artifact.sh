#!/usr/bin/env bash
set -euo pipefail

artifact_name=$1
destination=$2

# Branch isolation matches cache access. The default branch supplies a fallback.
run_id=$(
  gh api --method GET --paginate --slurp \
    "repos/${GITHUB_REPOSITORY}/actions/artifacts" \
    -f "name=${artifact_name}" -F per_page=100 |
    jq -r \
      --arg name "$artifact_name" \
      --arg branch "$GITHUB_REF_NAME" \
      --arg default_branch "$DEFAULT_BRANCH" \
      --argjson repository_id "$GITHUB_REPOSITORY_ID" \
      --argjson current_run "$GITHUB_RUN_ID" '
        [.[].artifacts[]
          | select(.name == $name and .expired == false)
          | select(.workflow_run.repository_id == $repository_id)
          | select(.workflow_run.head_repository_id == $repository_id)
          | select(.workflow_run.id != $current_run)
          | select(.workflow_run.head_branch == $branch
              or .workflow_run.head_branch == $default_branch)]
        | sort_by([(.workflow_run.head_branch == $branch), .created_at, .id])
        | last | .workflow_run.id // empty
      '
)

if [[ -z "$run_id" ]]; then
  printf 'No retained artifact for %s. Use the available cache or seed data.\n' "$artifact_name"
  exit 0
fi

# A temporary directory prevents extraction conflicts with cached files and seed data.
restore_dir=$(mktemp -d)
trap 'rm -rf -- "$restore_dir"' EXIT
gh run download "$run_id" --repo "$GITHUB_REPOSITORY" \
  --name "$artifact_name" --dir "$restore_dir"
mkdir -p -- "$destination"
cp -a "$restore_dir"/. "$destination"/
printf 'Restored %s from run %s.\n' "$artifact_name" "$run_id"

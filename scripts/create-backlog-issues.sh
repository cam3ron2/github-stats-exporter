#!/usr/bin/env bash

set -euo pipefail

SCRIPT_NAME="$(basename "$0")"
REPO=""
BACKLOG_FILE="docs/ISSUE_BACKLOG.md"
APPLY_CHANGES=0
DEDUPE=1
LABEL_COLOR="1D76DB"

usage() {
	cat <<EOF
Usage: $SCRIPT_NAME [options]

Create GitHub issues from backlog entries in markdown headings like:
  ### CP-001 - Issue title

Options:
  -r, --repo <owner/name>    Target repository (default: current gh repo)
  -f, --file <path>          Backlog markdown file (default: $BACKLOG_FILE)
  -a, --apply                Create labels and issues (default: dry-run)
      --no-dedupe            Do not skip already existing issues
  -h, --help                 Show this help

Examples:
  $SCRIPT_NAME
  $SCRIPT_NAME --apply
  $SCRIPT_NAME --repo cam3ron2/github-stats-exporter --apply
EOF
}

require_cmd() {
	local cmd="$1"
	if ! command -v "$cmd" >/dev/null 2>&1; then
		echo "error: required command not found: $cmd" >&2
		exit 1
	fi
}

trim() {
	local value="$1"
	value="${value#"${value%%[![:space:]]*}"}"
	value="${value%"${value##*[![:space:]]}"}"
	printf '%s' "$value"
}

while [[ $# -gt 0 ]]; do
	case "$1" in
	-r | --repo)
		REPO="${2:-}"
		shift 2
		;;
	-f | --file)
		BACKLOG_FILE="${2:-}"
		shift 2
		;;
	-a | --apply)
		APPLY_CHANGES=1
		shift
		;;
	--no-dedupe)
		DEDUPE=0
		shift
		;;
	-h | --help)
		usage
		exit 0
		;;
	*)
		echo "error: unknown argument: $1" >&2
		usage
		exit 1
		;;
	esac
done

require_cmd gh
require_cmd python3

if [[ ! -f "$BACKLOG_FILE" ]]; then
	echo "error: backlog file not found: $BACKLOG_FILE" >&2
	exit 1
fi

if [[ -z "$REPO" ]]; then
	REPO="$(gh repo view --json nameWithOwner -q '.nameWithOwner')"
fi

if [[ -z "$REPO" ]]; then
	echo "error: unable to resolve target repository" >&2
	exit 1
fi

TMP_DIR="$(mktemp -d)"
cleanup() {
	rm -rf "$TMP_DIR"
}
trap cleanup EXIT

ISSUE_INDEX_FILE="$TMP_DIR/issues.tsv"

python3 - "$BACKLOG_FILE" "$TMP_DIR" >"$ISSUE_INDEX_FILE" <<'PY'
import pathlib
import re
import sys

backlog_path = pathlib.Path(sys.argv[1])
output_dir = pathlib.Path(sys.argv[2])
text = backlog_path.read_text(encoding="utf-8")

source_plan_match = re.search(r"(?m)^Source plan:\s*`([^`]+)`", text)
source_plan = source_plan_match.group(1) if source_plan_match else ""

sections = re.split(r"(?m)^###\s+", text)
for section in sections[1:]:
    lines = section.splitlines()
    if not lines:
        continue

    heading = lines[0].strip()
    heading_match = re.match(r"(?P<id>[A-Z]+-\d+)\s*-\s*(?P<title>.+)$", heading)
    if not heading_match:
        continue

    issue_id = heading_match.group("id")
    issue_title = f"{issue_id} - {heading_match.group('title').strip()}"
    priority = ""
    labels = []
    scope_items = []
    acceptance_items = []
    mode = ""

    for raw_line in lines[1:]:
        line = raw_line.rstrip()
        stripped = line.strip()

        if stripped.startswith("- [ ] Priority:"):
            priority_match = re.search(r"`([^`]+)`", stripped)
            if priority_match:
                priority = priority_match.group(1).strip()
            mode = ""
            continue

        if stripped.startswith("- [ ] Labels:"):
            labels = [value.strip() for value in re.findall(r"`([^`]+)`", stripped)]
            mode = ""
            continue

        if stripped == "- [ ] Scope:":
            mode = "scope"
            continue

        if stripped == "- [ ] Acceptance criteria:":
            mode = "acceptance"
            continue

        bullet_match = re.match(r"-\s+(.*)$", stripped)
        if bullet_match and mode == "scope":
            scope_items.append(bullet_match.group(1).strip())
            continue
        if bullet_match and mode == "acceptance":
            acceptance_items.append(bullet_match.group(1).strip())
            continue

    body_lines = [f"Backlog import for `{issue_id}` from `{backlog_path.as_posix()}`."]
    if source_plan:
        body_lines.append(f"Source plan: `{source_plan}`.")
    body_lines.extend(["", "### Priority"])
    if priority:
        body_lines.append(f"- `{priority}`")
    else:
        body_lines.append("- Not specified")

    body_lines.extend(["", "### Scope"])
    if scope_items:
        body_lines.extend(f"- {item}" for item in scope_items)
    else:
        body_lines.append("- Not specified")

    body_lines.extend(["", "### Acceptance criteria"])
    if acceptance_items:
        body_lines.extend(f"- {item}" for item in acceptance_items)
    else:
        body_lines.append("- Not specified")

    body_path = output_dir / f"{issue_id}.md"
    body_path.write_text("\n".join(body_lines).rstrip() + "\n", encoding="utf-8")

    print("\t".join([issue_id, issue_title, priority, ",".join(labels), str(body_path)]))
PY

if [[ ! -s "$ISSUE_INDEX_FILE" ]]; then
	echo "error: no issue blocks were found in $BACKLOG_FILE" >&2
	exit 1
fi

mapfile -t CURRENT_LABELS < <(gh label list --repo "$REPO" --limit 1000 --json name -q '.[].name')
declare -A LABEL_EXISTS=()
for label_name in "${CURRENT_LABELS[@]}"; do
	LABEL_EXISTS["$label_name"]=1
done

ensure_label() {
	local label="$1"
	if [[ -z "$label" ]]; then
		return 0
	fi
	if [[ -n "${LABEL_EXISTS[$label]:-}" ]]; then
		return 0
	fi

	if [[ "$APPLY_CHANGES" -eq 1 ]]; then
		gh label create "$label" --repo "$REPO" --color "$LABEL_COLOR" --description "Created from issue backlog import" >/dev/null
		echo "[create] label: $label"
	else
		echo "[dry-run] would create label: $label"
	fi
	LABEL_EXISTS["$label"]=1
}

issue_exists() {
	local title="$1"
	local existing_number
	existing_number="$(
		gh issue list --repo "$REPO" \
			--state all \
			--search "\"$title\" in:title" \
			--limit 1 \
			--json number \
			-q '.[0].number // ""'
	)"
	[[ -n "$existing_number" ]]
}

created_count=0
skipped_count=0

echo "[info] repository: $REPO"
echo "[info] backlog file: $BACKLOG_FILE"
if [[ "$APPLY_CHANGES" -eq 1 ]]; then
	echo "[mode] apply"
else
	echo "[mode] dry-run"
fi

while IFS=$'\t' read -r issue_id title priority labels_csv body_file; do
	declare -a labels=()
	if [[ -n "$labels_csv" ]]; then
		IFS=',' read -r -a raw_labels <<<"$labels_csv"
		for label in "${raw_labels[@]}"; do
			label="$(trim "$label")"
			[[ -n "$label" ]] && labels+=("$label")
		done
	fi

	for label in "${labels[@]}"; do
		ensure_label "$label"
	done

	if [[ "$DEDUPE" -eq 1 ]] && issue_exists "$title"; then
		echo "[skip] $issue_id already exists: $title"
		((skipped_count += 1))
		continue
	fi

	if [[ "$APPLY_CHANGES" -eq 1 ]]; then
		cmd=(gh issue create --repo "$REPO" --title "$title" --body-file "$body_file")
		for label in "${labels[@]}"; do
			cmd+=(--label "$label")
		done
		issue_url="$("${cmd[@]}")"
		echo "[create] $issue_id -> $issue_url"
	else
		if [[ ${#labels[@]} -gt 0 ]]; then
			echo "[dry-run] would create: $title (labels: ${labels[*]})"
		else
			echo "[dry-run] would create: $title (labels: none)"
		fi
	fi

	((created_count += 1))
done <"$ISSUE_INDEX_FILE"

echo "[done] created=$created_count skipped=$skipped_count dedupe=$DEDUPE"

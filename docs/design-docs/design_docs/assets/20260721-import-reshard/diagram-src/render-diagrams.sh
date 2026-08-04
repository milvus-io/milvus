#!/usr/bin/env bash
set -euo pipefail

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
output_dir="$(cd "$script_dir/.." && pwd)"
source_html="$script_dir/diagrams.html"

chrome_bin=""
for candidate in google-chrome google-chrome-stable chromium chromium-browser; do
  if command -v "$candidate" >/dev/null 2>&1; then
    chrome_bin="$(command -v "$candidate")"
    break
  fi
done

if [[ -z "$chrome_bin" ]]; then
  echo "No headless Chrome/Chromium found. The committed PNG files remain the renderer-independent fallback." >&2
  exit 1
fi

render() {
  local diagram_id="$1"
  local css_height="$2"
  local output_name="$3"

  "$chrome_bin" \
    --headless=new \
    --no-sandbox \
    --disable-gpu \
    --disable-dev-shm-usage \
    --disable-breakpad \
    --disable-crash-reporter \
    --disable-features=Crashpad \
    --noerrdialogs \
    --hide-scrollbars \
    --force-device-scale-factor=2 \
    --window-size="1184,$css_height" \
    --run-all-compositor-stages-before-draw \
    --virtual-time-budget=1000 \
    --screenshot="$output_dir/$output_name" \
    "file://$source_html?diagram=$diagram_id"

  if [[ ! -s "$output_dir/$output_name" ]]; then
    echo "Chrome did not produce $output_name. Keep using the committed PNG fallback." >&2
    return 1
  fi
}

render legacy-import-flow 680 legacy-import-flow.png
render current-fragmentation 440 current-fragmentation.png
render data-layout 430 data-layout.png
render spill-sort-upload 650 spill-sort-upload.png
render overall-flow 500 overall-flow.png
render reader-model 520 reader-model.png
render autoid-ranges 480 autoid-ranges.png
render storage-authority 520 storage-authority.png
render attempt-isolation 500 attempt-isolation.png
render verified-read 430 verified-read.png
render gc-lifecycle 400 gc-lifecycle.png

echo "Rendered diagrams into $output_dir"

#!/usr/bin/env bash
# Run from the Milvus repository root with the native build environment loaded.
set -euo pipefail
out=$(mkdir -p "${1:?provide an unused build directory}" && cd "$1" && pwd -P)
mkdir "$out/bin"
python3 - "$out" <<'PY'
import hashlib, json, pathlib, subprocess, sys
out = pathlib.Path(sys.argv[1])
source = pathlib.Path("pkg/streaming/util/message/update.go").resolve(strict=True)
original = source.read_bytes()
assert original.count(b"const PKUpdateEnabled = false") == 1
overlay = out / "update.go"
overlay.write_bytes(original.replace(b"const PKUpdateEnabled = false", b"const PKUpdateEnabled = true"))
mapping = out / "overlay.json"
mapping.write_text(json.dumps({"Replace": {str(source): str(overlay)}}))
manifest = {"commit": subprocess.check_output(["git", "rev-parse", "HEAD"], text=True).strip()}
for path in (source, overlay, mapping):
    manifest[str(path)] = hashlib.sha256(path.read_bytes()).hexdigest()
(out / "build-inputs.json").write_text(json.dumps(manifest, indent=2))
PY
go build -p=2 -mod=readonly -pgo=off -tags dynamic -overlay="$out/overlay.json" -o "$out/bin/milvus" ./cmd
go build -mod=readonly -o "$out/bin/update-rpc" ./tests/integration/pkupdate/service/rpc
go build -p=2 -mod=readonly -pgo=off -tags dynamic -o "$out/bin/inspect" ./tests/integration/pkupdate/service/inspect

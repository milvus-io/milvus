#!/usr/bin/env bash
# Run from the Milvus repository root with the native build environment loaded.
set -euo pipefail
out=$(mkdir -p "${1:?provide an unused build directory}" && cd "$1" && pwd -P)
mkdir "$out/bin"
mode=${2:-normal}
if [[ "$mode" == faults ]]; then
    version=$(go list -m -f '{{.Version}}' github.com/pingcap/failpoint)
    GOBIN="$out/bin" go install "github.com/pingcap/failpoint/failpoint-ctl@$version"
fi
python3 - "$out" "$mode" <<'PY'
import hashlib, json, pathlib, subprocess, sys
out = pathlib.Path(sys.argv[1])
source = pathlib.Path("pkg/streaming/util/message/update.go").resolve(strict=True)
original = source.read_bytes()
assert original.count(b"const PKUpdateEnabled = false") == 1
overlay = out / "update.go"
overlay.write_bytes(original.replace(b"const PKUpdateEnabled = false", b"const PKUpdateEnabled = true"))
replacements = {str(source): str(overlay)}
inputs = [source, overlay]
if sys.argv[2] == "faults":
    import shutil
    for rel in (
        "internal/flushcommon/syncmgr/meta_writer.go",
        "internal/streamingnode/server/wal/recovery/recovery_background_task.go",
    ):
        orig = pathlib.Path(rel).resolve(strict=True)
        private = out / "rewritten" / rel
        private.parent.mkdir(parents=True)
        shutil.copyfile(orig, private)
        subprocess.run([out / "bin/failpoint-ctl", "enable", private.parent], check=True)
        for generated in private.parent.glob("*.go"):
            replacements[str(orig.parent / generated.name)] = str(generated)
            inputs.append(generated)
        inputs.append(orig)
    inputs.append(pathlib.Path("internal/util/pkupdatepoint/receipt.go").resolve())
elif sys.argv[2] != "normal":
    raise ValueError("build mode must be normal or faults")
mapping = out / "overlay.json"
mapping.write_text(json.dumps({"Replace": replacements}))
manifest = {"commit": subprocess.check_output(["git", "rev-parse", "HEAD"], text=True).strip()}
for path in [*inputs, mapping]:
    manifest[str(path)] = hashlib.sha256(path.read_bytes()).hexdigest()
(out / "build-inputs.json").write_text(json.dumps(manifest, indent=2))
PY
go build -p=2 -mod=readonly -pgo=off -tags dynamic -overlay="$out/overlay.json" -o "$out/bin/milvus" ./cmd
go build -mod=readonly -o "$out/bin/update-rpc" ./tests/integration/pkupdate/service/rpc
go build -p=2 -mod=readonly -pgo=off -tags dynamic -o "$out/bin/inspect" ./tests/integration/pkupdate/service/inspect
if [[ "$mode" == faults ]]; then
    go build -mod=readonly -o "$out/bin/transform" ./tests/integration/pkupdate/service/transform
fi

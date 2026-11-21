#!/usr/bin/env bash
# Run from the Milvus repository root with the native build environment loaded.
set -euo pipefail
out=$(mkdir -p "${1:?provide an unused build directory}" && cd "$1" && pwd -P)
if [[ -n $(git status --porcelain --untracked-files=normal) ]]; then
    echo "Build from a clean Milvus worktree with the output directory outside it." >&2
    exit 1
fi
native_dir=${PKUPDATE_NATIVE_BUILD_DIR:?provide the CMake build directory used by pkg-config}
mkdir "$out/bin"
mode=${2:-normal}
if [[ "$mode" == faults ]]; then
    version=$(go list -m -f '{{.Version}}' github.com/pingcap/failpoint)
    GOBIN="$out/bin" go install "github.com/pingcap/failpoint/failpoint-ctl@$version"
fi
python3 - "$out" "$mode" "$native_dir" <<'PY'
import hashlib, json, pathlib, subprocess, sys
out = pathlib.Path(sys.argv[1])
native = pathlib.Path(sys.argv[3]).resolve(strict=True)
cache = native / "CMakeCache.txt"
if "MILVUS_ENABLE_PK_UPDATE_NATIVE_BENCHMARK:BOOL=OFF" not in cache.read_text().splitlines():
    raise ValueError("service acceptance requires benchmark observation OFF")
cores = [p for p in [native / "src/libmilvus_core.dylib", native / "src/libmilvus_core.so"] if p.exists()]
if len(cores) != 1:
    raise ValueError("cannot identify the native core image")
replacements = {}
inputs = [cache, cores[0]]
if sys.argv[2] == "faults":
    import shutil
    private_dirs = {}
    for rel in (
        "internal/querynodev2/segments/segment.go",
        "internal/querynodev2/transformlogbuffer/buffer.go",
        "internal/streamingnode/server/wal/vchannel/growingruntime/live.go",
        "internal/streamingnode/server/wal/vchannel/queryresource/runtime.go",
        "internal/querynodev2/qnview/segment_resource_handle.go",
        "internal/querynodev2/qnview/segment_resource_manager.go",
        "internal/util/segcore/segment.go",
        "internal/proxy/replicate/replicate_stream_server.go",
        "internal/streamingnode/server/wal/walsummary/dispatch_stream.go",
        "internal/flushcommon/syncmgr/meta_writer.go",
        "internal/streamingnode/server/wal/recovery/recovery_background_task.go",
    ):
        orig = pathlib.Path(rel).resolve(strict=True)
        private = out / "rewritten" / rel
        private.parent.mkdir(parents=True, exist_ok=True)
        shutil.copyfile(orig, private)
        private_dirs[private.parent] = orig.parent
        inputs.append(orig)
    for copied_dir, original_dir in private_dirs.items():
        subprocess.run([out / "bin/failpoint-ctl", "enable", copied_dir], check=True)
        for generated in copied_dir.glob("*.go"):
            replacements[str(original_dir / generated.name)] = str(generated)
            inputs.append(generated)
    inputs.append(pathlib.Path("internal/util/pkupdatepoint/receipt.go").resolve())
elif sys.argv[2] != "normal":
    raise ValueError("build mode must be normal or faults")
if replacements:
    mapping = out / "overlay.json"
    mapping.write_text(json.dumps({"Replace": replacements}))
    inputs.append(mapping)
manifest = {"commit": subprocess.check_output(["git", "rev-parse", "HEAD"], text=True).strip()}
for path in inputs:
    digest = hashlib.sha256()
    with path.open("rb") as binary:
        for chunk in iter(lambda: binary.read(1024 * 1024), b""):
            digest.update(chunk)
    manifest[str(path)] = digest.hexdigest()
(out / "build-inputs.json").write_text(json.dumps(manifest, indent=2))
PY
overlay_args=()
if [[ "$mode" == faults ]]; then
    overlay_args+=(-overlay="$out/overlay.json")
fi
go build -p=2 -mod=readonly -pgo=off -tags dynamic ${overlay_args[@]+"${overlay_args[@]}"} -o "$out/bin/milvus" ./cmd
go build -mod=readonly -o "$out/bin/update-rpc" ./tests/integration/pkupdate/service/rpc
go build -p=2 -mod=readonly -pgo=off -tags dynamic -o "$out/bin/inspect" ./tests/integration/pkupdate/service/inspect
if [[ "$mode" == faults ]]; then
    go build -mod=readonly -o "$out/bin/ownerread" ./tests/integration/pkupdate/service/ownerread
    go build -mod=readonly -o "$out/bin/transform" ./tests/integration/pkupdate/service/transform
fi
go version -m "$out/bin/milvus" > "$out/go-buildinfo.txt"
if [[ $(uname) == Darwin ]]; then
    otool -L "$out/bin/milvus" > "$out/linked-libraries.txt"
else
    ldd "$out/bin/milvus" > "$out/linked-libraries.txt"
fi
python3 - "$out" <<'HASHES'
import hashlib, json, pathlib, sys
out = pathlib.Path(sys.argv[1])
hashes = {}
for path in sorted((out / "bin").iterdir()):
    digest = hashlib.sha256()
    with path.open("rb") as binary:
        for chunk in iter(lambda: binary.read(1024 * 1024), b""):
            digest.update(chunk)
    hashes[path.name] = digest.hexdigest()
(out / "binary-hashes.json").write_text(json.dumps(hashes, indent=2))
HASHES

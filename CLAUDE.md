# Milvus

Vector database. Go + C++ (internal/core/) + Rust (tantivy).
pkg has its own go.mod (module: `github.com/milvus-io/milvus/pkg/v2`). Run `go get` from `pkg/` when adding dependencies there, not from root.

## Architecture

Coordinators manage metadata and scheduling; nodes execute work.
- Coordinators: rootcoord, datacoord, querycoordv2 (note the v2 suffix in directory names)
- Nodes: proxy (user-facing), querynodev2, datanode, streamingnode
- All component interfaces defined in `internal/types/types.go`

## Testing

Go tests MUST use `-tags dynamic,test` or they won't compile:

```bash
go test -tags dynamic,test -count=1 ./internal/querycoordv2/...
go test -tags dynamic,test -count=1 ./internal/proxy/... -run TestXxx
```

Per-module shortcuts: `make test-querycoord`, `make test-proxy`, etc.

## Run Milvus Locally

```bash
scripts/start_standalone.sh    # start standalone mode
scripts/start_cluster.sh       # start cluster mode
scripts/stop_graceful.sh       # stop
scripts/standalone_embed.sh    # embedded standalone (no external deps)
```

## Code Conventions

- Error handling: use `merr` package, not fmt.Errorf
- Logging: use `pkg/v2/log`, not standard `"log"` or fmt.Println
- Import order: standard → third-party → github.com/milvus-io (enforced by gci)
- Config params: paramtable (`pkg/v2/util/paramtable`), config in `configs/milvus.yaml`

## PR and Commit Conventions

PR title format: `{type}: {description}`. Valid types: `feat:`, `fix:`, `enhance:`, `test:`, `doc:`, `auto:`, `build(deps):`.
PR body must be non-empty. Issue/doc linking rules:
- `fix:` — must link issue (e.g. `issue: #123`)
- `feat:` — must link issue + design doc from milvus-io/milvus-design-docs
- `enhance:` — must link issue if size L/XL/XXL
- `doc:`, `test:` — no issue required
- 2.x branch PRs must link the corresponding master PR (e.g. `pr: #123`)

DCO check is required. Always use `-s` so the developer's Signed-off-by is appended last:

```
git commit -s -m "commit message

Co-Authored-By: Claude Opus 4.6 <noreply@anthropic.com>"
```

`-s` auto-appends `Signed-off-by: <developer>` at the end. The developer MUST be the final sign-off, not the AI.

## Generated Files — Do Not Hand-Edit

- Mock files (`internal/mocks/*`, `mock_*.go`): regenerate with `make generate-mockery-{module}`
- Proto files (`pkg/proto/*.pb.go`): regenerate with `make generated-proto-without-cpp`

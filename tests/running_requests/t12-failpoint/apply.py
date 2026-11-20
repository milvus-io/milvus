"""Apply the T12 test-image overlay to a fresh Milvus source copy."""

import shutil
import sys
from pathlib import Path

source = Path(sys.argv[1])
changes = {
    "dql/task_search.go": (
        "func (t *SearchTask) Execute(ctx context.Context) error {",
        """
	if err := reqregistry.T12TestStage(ctx, "retry", t.ID()); err != nil {
		return err
	}
""",
    ),
    "dql/task_query.go": (
        "func (t *QueryTask) Execute(ctx context.Context) error {",
        """
	path := "search_by_id"
	if t.GetMetricLabel() == metrics.ReQueryLabel {
		path = "requery"
	}
	if err := reqregistry.T12TestStage(ctx, path, t.ID()); err != nil {
		return err
	}
""",
    ),
    "search_auto_load.go": (
        "return node.retryDQLWithExecutionRetry(ctx, dbName, collectionName, func(ctx context.Context) (bool, error) {\n\t\tagain, err := execute(ctx)",
        """
		if errors.Is(err, reqregistry.ErrT12Retry) {
			return true, err
		}
""",
    ),
}
proxy = source / "internal/proxy"
import_line = '\t"github.com/milvus-io/milvus/internal/proxy/reqregistry"\n'
updates = {}
for name, (anchor, code) in changes.items():
    path = proxy / name
    text = path.read_text()
    assert text.count(anchor) == 1 and code not in text, path
    updated = text.replace(anchor, anchor + code, 1)
    if import_line not in updated:
        updated = updated.replace("import (\n", "import (\n" + import_line, 1)
    updates[path] = updated
for path, text in updates.items():
    path.write_text(text)
shutil.copyfile(Path(__file__).with_name("t12_stage.go.in"), proxy / "reqregistry/t12_stage.go")

package compaction

import (
	"path/filepath"
	"time"

	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

// Config is what the executor needs from the configuration.
type Config struct {
	// LocalRoot is the directory of the task directories. The executor
	// deletes it at start.
	LocalRoot string
	// TerminalTaskTTL is how long a finished or failed task waits for a
	// query before the executor deletes it.
	TerminalTaskTTL time.Duration
	// TaskSlot is what one running task takes from the node slots.
	TaskSlot int64
	// Parallelism is the number of tasks that run at once.
	Parallelism int
	// DiskMargin is the free disk a task needs on top of twice its inputs.
	DiskMargin int64
}

// ConfigFromParams reads the configuration from the param table.
func ConfigFromParams() Config {
	p := paramtable.Get()
	return Config{
		LocalRoot:       filepath.Join(p.LocalStorageCfg.Path.GetValue(), "pkindex_compaction"),
		TerminalTaskTTL: p.DataNodeCfg.PKIndexCompactionTerminalTaskTTL.GetAsDuration(time.Second),
		TaskSlot:        p.DataCoordCfg.PKIndexCompactionSlotUsage.GetAsInt64(),
		Parallelism:     p.DataNodeCfg.PKIndexCompactionParallelism.GetAsInt(),
		DiskMargin:      p.DataNodeCfg.PKIndexCompactionDiskMargin.GetAsInt64(),
	}
}

package compaction

import (
	"context"
	"fmt"
	"math"
	"strings"
	"sync"
	"time"

	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

// Config holds the trigger thresholds and the scheduling limits.
type Config struct {
	// L0Trigger is the number of flushed tables that triggers their merge.
	L0Trigger int
	// MinMergeRuns is the smallest number of consecutive runs that merge.
	MinMergeRuns int
	// SizeRatio bounds the largest run of a merge by the smallest.
	SizeRatio float64
	// SpaceAmpRatio triggers a merge of every table when the bytes outside
	// the oldest run reach this many times the oldest run.
	SpaceAmpRatio float64
	// MaxInputBytes bounds the input of a flushed-table or size-tiered merge.
	MaxInputBytes int64
	// MaxInputTables bounds the number of flushed tables in one merge.
	MaxInputTables int
	// MaxConcurrent bounds the tasks in flight across all vchannels.
	MaxConcurrent int
	// InspectInterval is how often the changed manifests are scored.
	InspectInterval time.Duration
	// FailureCooldown keeps a vchannel out of planning after a failure.
	FailureCooldown time.Duration
	// TargetFileSize is the size at which an output table is closed.
	TargetFileSize int64
	// TaskSlot is what one task takes from a node's slots.
	TaskSlot int64
}

// normalized returns a copy whose values the planner can use: a value that
// would make a rule meaningless is replaced by the nearest usable one.
func (c Config) normalized() Config {
	n, _ := c.normalize()
	return n
}

// normalize returns the normalized copy and the correction made to each field.
func (c Config) normalize() (Config, []correction) {
	var fixes []correction
	fix := func(field string, bad bool, from, to any) {
		if bad {
			fixes = append(fixes, correction{field: field, from: fmt.Sprint(from), to: fmt.Sprint(to)})
		}
	}
	n := c
	if n.L0Trigger < 2 {
		fix("L0Trigger", true, c.L0Trigger, 2)
		n.L0Trigger = 2
	}
	if n.MinMergeRuns < 2 {
		fix("MinMergeRuns", true, c.MinMergeRuns, 2)
		n.MinMergeRuns = 2
	}
	if n.MaxInputTables < 2 {
		fix("MaxInputTables", true, c.MaxInputTables, 2)
		n.MaxInputTables = 2
	}
	if n.SizeRatio < 1 {
		fix("SizeRatio", true, c.SizeRatio, 1)
		n.SizeRatio = 1
	}
	if n.SpaceAmpRatio <= 0 {
		fix("SpaceAmpRatio", true, c.SpaceAmpRatio, 1)
		n.SpaceAmpRatio = 1
	}
	if n.MaxInputBytes <= 0 {
		fix("MaxInputBytes", true, c.MaxInputBytes, int64(math.MaxInt64))
		n.MaxInputBytes = math.MaxInt64
	}
	if n.TargetFileSize <= 0 {
		fix("TargetFileSize", true, c.TargetFileSize, 128<<20)
		n.TargetFileSize = 128 << 20
	}
	if n.MaxConcurrent < 1 {
		fix("MaxConcurrent", true, c.MaxConcurrent, 1)
		n.MaxConcurrent = 1
	}
	if n.InspectInterval <= 0 {
		fix("InspectInterval", true, c.InspectInterval, 10*time.Second)
		n.InspectInterval = 10 * time.Second
	}
	if n.FailureCooldown < 0 {
		fix("FailureCooldown", true, c.FailureCooldown, 0)
		n.FailureCooldown = 0
	}
	if n.TaskSlot < 1 {
		fix("TaskSlot", true, c.TaskSlot, 1)
		n.TaskSlot = 1
	}
	return n, fixes
}

// correction is one value that normalize replaced.
type correction struct{ field, from, to string }

// ConfigFromParams reads the configuration from the param table. It logs each
// value it had to correct.
func ConfigFromParams(ctx context.Context) Config {
	p := &paramtable.Get().DataCoordCfg
	cfg, fixes := Config{
		L0Trigger:       p.PKIndexCompactionL0Trigger.GetAsInt(),
		MinMergeRuns:    p.PKIndexCompactionMinMergeRuns.GetAsInt(),
		SizeRatio:       p.PKIndexCompactionSizeRatio.GetAsFloat(),
		SpaceAmpRatio:   p.PKIndexCompactionSpaceAmpRatio.GetAsFloat(),
		MaxInputBytes:   p.PKIndexCompactionMaxInputBytes.GetAsInt64(),
		MaxInputTables:  p.PKIndexCompactionMaxInputTables.GetAsInt(),
		MaxConcurrent:   p.PKIndexCompactionMaxConcurrent.GetAsInt(),
		InspectInterval: p.PKIndexCompactionInspectInterval.GetAsDuration(time.Second),
		FailureCooldown: p.PKIndexCompactionFailureCooldown.GetAsDuration(time.Second),
		TargetFileSize:  p.PKIndexCompactionTargetFileSize.GetAsInt64(),
		TaskSlot:        p.PKIndexCompactionSlotUsage.GetAsInt64(),
	}.normalize()
	logCorrections(ctx, fixes)
	return cfg
}

var (
	lastCorrectionsMu sync.Mutex
	lastCorrections   string
)

// logCorrections logs each correction, but only when the set differs from
// the set it logged last: the configuration is read on every inspection, and
// the same bad value must not warn every time. It reports whether it logged.
func logCorrections(ctx context.Context, fixes []correction) bool {
	parts := make([]string, 0, len(fixes))
	for _, f := range fixes {
		parts = append(parts, f.field+":"+f.from+"->"+f.to)
	}
	joined := strings.Join(parts, ",")
	lastCorrectionsMu.Lock()
	same := joined == lastCorrections
	lastCorrections = joined
	lastCorrectionsMu.Unlock()
	if same {
		return false
	}
	for _, f := range fixes {
		mlog.Warn(ctx, "primary key index compaction config value corrected",
			mlog.String("field", f.field), mlog.String("configured", f.from), mlog.String("used", f.to))
	}
	return len(fixes) > 0
}

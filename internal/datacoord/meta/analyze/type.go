package analyze

import (
	"github.com/milvus-io/milvus/internal/proto/indexpb"
	"github.com/milvus-io/milvus/internal/proto/workerpb"
	"github.com/milvus-io/milvus/pkg/util/typeutil"
)

type AnalyzeMeta interface {
	GetAnalyzeTask(taskID int64) *indexpb.AnalyzeTask
	AddAnalyzeTask(task *indexpb.AnalyzeTask) error
	DropAnalyzeTask(taskID int64) error
	UpdateAnalyzeVersion(taskID int64) error
	BuildingAnalyzeTask(taskID, nodeID int64) error
	FinishAnalyzeTask(taskID int64, result *workerpb.AnalyzeResult) error
	GetAllTasks() map[int64]*indexpb.AnalyzeTask
	CheckCleanAnalyzeTask(taskID typeutil.UniqueID) (bool, *indexpb.AnalyzeTask)
}

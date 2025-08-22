package compactor

import (
	"github.com/milvus-io/milvus/pkg/v2/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v2/util/typeutil"
)

// namespaceClusteringCompactionTask is a compaction task that compacts records with same namespaces together.
// segments involved in the task must be sorted by namespace(alias by partition key).
// this task use merge sort to compact segments.
type namespaceClusteringCompactionTask struct {
	mix
}


var _ Compactor = (*clusteringCompactionTask)(nil)

func (t *namespaceClusteringCompactionTask) Complete() {
	panic("not implemented") // TODO: Implement
}

func (t *namespaceClusteringCompactionTask) Compact() (*datapb.CompactionPlanResult, error) {
	panic("not implemented") // TODO: Implement
}

func (t *namespaceClusteringCompactionTask) Stop() {
	panic("not implemented") // TODO: Implement
}

func (t *namespaceClusteringCompactionTask) GetPlanID() typeutil.UniqueID {
	panic("not implemented") // TODO: Implement
}

func (t *namespaceClusteringCompactionTask) GetCollection() typeutil.UniqueID {
	panic("not implemented") // TODO: Implement
}

func (t *namespaceClusteringCompactionTask) GetChannelName() string {
	panic("not implemented") // TODO: Implement
}

func (t *namespaceClusteringCompactionTask) GetCompactionType() datapb.CompactionType {
	panic("not implemented") // TODO: Implement
}

func (t *namespaceClusteringCompactionTask) GetSlotUsage() int64 {
	panic("not implemented") // TODO: Implement
}

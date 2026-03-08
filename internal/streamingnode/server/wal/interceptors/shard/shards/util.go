package shards

import "github.com/milvus-io/milvus/internal/streamingnode/server/wal/interceptors/shard/utils"

type (
	PartitionUniqueKey = utils.PartitionUniqueKey
	SegmentBelongs     = utils.SegmentBelongs
)

type TxnManager interface {
	RecoverDone() <-chan struct{}
}

// TxnSession is a session interface
type TxnSession interface {
	// should be called when the session is done.
	RegisterCleanup(cleanup func(), timetick uint64)
}

package syncmgr

import (
	"github.com/milvus-io/milvus-proto/go-api/v2/msgpb"
	"github.com/milvus-io/milvus/internal/allocator"
	"github.com/milvus-io/milvus/internal/flushcommon/metacache"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/pkg/proto/datapb"
	"github.com/milvus-io/milvus/pkg/util/retry"
	"github.com/milvus-io/milvus/pkg/util/typeutil"
)

func NewSyncTask() *SyncTask {
	return new(SyncTask)
}

func (t *SyncTask) WithSyncPack(pack *SyncPack) *SyncTask {
	t.pack = pack

	// legacy code, remove later
	t.bm25Binlogs = make(map[int64]*datapb.FieldBinlog)
	t.collectionID = t.pack.collectionID
	t.partitionID = t.pack.partitionID
	t.channelName = t.pack.channelName
	t.segmentID = t.pack.segmentID
	t.batchRows = t.pack.batchRows
	// t.metacache = t.pack.metacache
	// t.schema = t.metacache.Schema()
	t.startPosition = t.pack.startPosition
	t.checkpoint = t.pack.checkpoint
	t.level = t.pack.level
	t.dataSource = t.pack.dataSource
	t.tsFrom = t.pack.tsFrom
	t.tsTo = t.pack.tsTo
	t.failureCallback = t.pack.errHandler
	return t
}

func (t *SyncTask) WithChunkManager(cm storage.ChunkManager) *SyncTask {
	t.chunkManager = cm
	return t
}

func (t *SyncTask) WithAllocator(allocator allocator.Interface) *SyncTask {
	t.allocator = allocator
	return t
}

func (t *SyncTask) WithCheckpoint(cp *msgpb.MsgPosition) *SyncTask {
	t.checkpoint = cp
	return t
}

func (t *SyncTask) WithTimeRange(from, to typeutil.Timestamp) *SyncTask {
	t.tsFrom, t.tsTo = from, to
	return t
}

func (t *SyncTask) WithFlush() *SyncTask {
	t.pack.isFlush = true
	return t
}

func (t *SyncTask) WithDrop() *SyncTask {
	t.pack.isDrop = true
	return t
}

func (t *SyncTask) WithMetaCache(metacache metacache.MetaCache) *SyncTask {
	t.metacache = metacache
	return t
}

func (t *SyncTask) WithMetaWriter(metaWriter MetaWriter) *SyncTask {
	t.metaWriter = metaWriter
	return t
}

func (t *SyncTask) WithWriteRetryOptions(opts ...retry.Option) *SyncTask {
	t.writeRetryOpts = opts
	return t
}

func (t *SyncTask) WithFailureCallback(callback func(error)) *SyncTask {
	t.failureCallback = callback
	return t
}

func (t *SyncTask) WithBatchRows(batchRows int64) *SyncTask {
	t.batchRows = batchRows
	return t
}

func (t *SyncTask) WithLevel(level datapb.SegmentLevel) *SyncTask {
	t.level = level
	return t
}

func (t *SyncTask) WithDataSource(source string) *SyncTask {
	t.dataSource = source
	return t
}

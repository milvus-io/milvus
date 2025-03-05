package syncmgr

import (
	"github.com/milvus-io/milvus/internal/allocator"
	"github.com/milvus-io/milvus/internal/flushcommon/metacache"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/pkg/v2/util/retry"
)

func NewSyncTask() *SyncTask {
	return new(SyncTask)
}

func (t *SyncTask) WithSyncPack(pack *SyncPack) *SyncTask {
	t.pack = pack

	// legacy code, remove later
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

func (t *SyncTask) WithSyncBufferSize(size int64) *SyncTask {
	t.syncBufferSize = size
	return t
}

func (t *SyncTask) WithMultiPartUploadSize(size int64) *SyncTask {
	t.multiPartUploadSize = size
	return t
}

package syncmgr

import (
	"time"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/allocator"
	"github.com/milvus-io/milvus/internal/flushcommon/metacache"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/pkg/v3/proto/indexpb"
)

func NewSyncTask() *SyncTask {
	return &SyncTask{ioRetry: defaultIORetryPolicy()}
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

func (t *SyncTask) SetChunkManager(cm storage.ChunkManager) { t.chunkManager = cm }

func (t *SyncTask) SetDrop() { t.pack.isDrop = true }

func (t *SyncTask) WithStorageConfig(storageConfig *indexpb.StorageConfig) *SyncTask {
	t.storageConfig = storageConfig
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

func (t *SyncTask) WithSchema(schema *schemapb.CollectionSchema) *SyncTask {
	t.schema = schema
	return t
}

func (t *SyncTask) WithMetaWriter(metaWriter MetaWriter) *SyncTask {
	t.metaWriter = metaWriter
	return t
}

func (t *SyncTask) WithFailureCallback(callback func(error)) *SyncTask {
	t.failureCallback = callback
	return t
}

// WithPayloadAccounting tracks the in-memory row payload until ReleaseData is
// actually called. The callback runs exactly once, including terminal discard
// paths (Abandon) that never reach Prepare.
func (t *SyncTask) WithPayloadAccounting(insertBytes, deleteBytes int64, onRelease func(int64)) *SyncTask {
	t.payload = &syncTaskPayloadAccounting{onRelease: onRelease}
	t.payload.insertBytes.Store(insertBytes)
	t.payload.deleteBytes.Store(deleteBytes)
	return t
}

// WithIORetryPolicy sets this task's budget AND backoff for the retries inside
// the object storage and meta writers. attempts of 0 means unlimited.
//
// Both halves are per-caller for the same reason: a caller whose own failure
// recovery is expensive (import re-reads and re-parses the whole file) is
// better off retrying more times and waiting longer between them, while the
// flush path — which re-drives the task from its own queue — must not hold a
// queue slot and its payload for either. See DefaultIORetryAttempts.
//
// A non-positive interval is replaced by the default rather than honored; with
// attempts=0 a zero sleep is an unbounded zero-delay loop.
func (t *SyncTask) WithIORetryPolicy(attempts uint, initialInterval, maxInterval time.Duration) *SyncTask {
	t.ioRetry = ioRetryPolicy{
		attempts:        attempts,
		initialInterval: initialInterval,
		maxInterval:     maxInterval,
	}.normalized()
	return t
}

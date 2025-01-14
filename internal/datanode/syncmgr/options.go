package syncmgr

import (
	"github.com/samber/lo"

	"github.com/milvus-io/milvus-proto/go-api/v2/msgpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/internal/allocator"
	"github.com/milvus-io/milvus/internal/datanode/metacache"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/pkg/proto/datapb"
	"github.com/milvus-io/milvus/pkg/util/retry"
	"github.com/milvus-io/milvus/pkg/util/typeutil"
)

func NewSyncTask() *SyncTask {
	return &SyncTask{
		isFlush:       false,
		insertBinlogs: make(map[int64]*datapb.FieldBinlog),
		statsBinlogs:  make(map[int64]*datapb.FieldBinlog),
		deltaBinlog:   &datapb.FieldBinlog{},
		segmentData:   make(map[string][]byte),
		binlogBlobs:   make(map[int64]*storage.Blob),
	}
}

func (t *SyncTask) WithChunkManager(cm storage.ChunkManager) *SyncTask {
	t.chunkManager = cm
	return t
}

func (t *SyncTask) WithAllocator(allocator allocator.Interface) *SyncTask {
	t.allocator = allocator
	return t
}

func (t *SyncTask) WithStartPosition(start *msgpb.MsgPosition) *SyncTask {
	t.startPosition = start
	return t
}

func (t *SyncTask) WithCheckpoint(cp *msgpb.MsgPosition) *SyncTask {
	t.checkpoint = cp
	return t
}

func (t *SyncTask) WithCollectionID(collID int64) *SyncTask {
	t.collectionID = collID
	return t
}

func (t *SyncTask) WithPartitionID(partID int64) *SyncTask {
	t.partitionID = partID
	return t
}

func (t *SyncTask) WithSegmentID(segID int64) *SyncTask {
	t.segmentID = segID
	return t
}

func (t *SyncTask) WithChannelName(chanName string) *SyncTask {
	t.channelName = chanName
	return t
}

func (t *SyncTask) WithSchema(schema *schemapb.CollectionSchema) *SyncTask {
	t.schema = schema
	t.pkField = lo.FindOrElse(schema.GetFields(), nil, func(field *schemapb.FieldSchema) bool {
		return field.GetIsPrimaryKey()
	})
	return t
}

func (t *SyncTask) WithTimeRange(from, to typeutil.Timestamp) *SyncTask {
	t.tsFrom, t.tsTo = from, to
	return t
}

func (t *SyncTask) WithFlush() *SyncTask {
	t.isFlush = true
	return t
}

func (t *SyncTask) WithDrop() *SyncTask {
	t.isDrop = true
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

func (t *SyncTask) WithBatchSize(batchSize int64) *SyncTask {
	t.batchSize = batchSize
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

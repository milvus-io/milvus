package segment

import (
	"context"
	"path"

	"github.com/samber/lo"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/msgpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/allocator"
	"github.com/milvus-io/milvus/internal/flushcommon/metacache"
	"github.com/milvus-io/milvus/internal/flushcommon/metacache/pkoracle"
	"github.com/milvus-io/milvus/internal/flushcommon/syncmgr"
	"github.com/milvus-io/milvus/internal/flushcommon/writebuffer"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/internal/storagecommon"
	"github.com/milvus-io/milvus/internal/storagev2/packed"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/mq/msgstream"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/indexpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/metautil"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v3/util/retry"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

type growingBulkPackWriter struct {
	chunkManager   storage.ChunkManager
	allocator      allocator.Interface
	storageConfig  *indexpb.StorageConfig
	writeRetryOpts []retry.Option
	writeFn        growingBulkWriteFunc
}

type growingBulkWriteFunc func(context.Context, *growingBulkWriteRequest) (*growingBulkWriteResult, error)

type growingBulkWriteRequest struct {
	syncPack       *syncmgr.SyncPack
	metaCache      metacache.MetaCache
	schema         *schemapb.CollectionSchema
	insertData     []*storage.InsertData
	chunkManager   storage.ChunkManager
	allocator      allocator.Interface
	storageConfig  *indexpb.StorageConfig
	writeRetryOpts []retry.Option
	storageVersion int64
	currentSplit   []storagecommon.ColumnGroup
	manifestPath   string
}

type growingBulkWriteResult struct {
	insertBinlogs map[int64]*datapb.FieldBinlog
	statsBinlogs  map[int64]*datapb.FieldBinlog
	bm25Binlogs   map[int64]*datapb.FieldBinlog
	manifestPath  string
	statistics    *datapb.Statistics
}

func NewBulkPackWriter(
	chunkManager storage.ChunkManager,
	allocator allocator.Interface,
	storageConfig *indexpb.StorageConfig,
	writeRetryOpts ...retry.Option,
) PackWriter {
	return &growingBulkPackWriter{
		chunkManager:   chunkManager,
		allocator:      allocator,
		storageConfig:  storageConfig,
		writeRetryOpts: writeRetryOpts,
		writeFn:        writeGrowingBulkPack,
	}
}

func (w *growingBulkPackWriter) FlushInsertBuffer(ctx context.Context, pack *flushPack) (*flushResult, error) {
	writeFn := w.writeFn
	if writeFn == nil {
		writeFn = writeGrowingBulkPack
	}

	schema := pack.Schema
	if schema == nil {
		return nil, retry.Unrecoverable(merr.WrapErrServiceInternalMsg("growing flush pack schema is nil"))
	}
	insertData, bm25Stats, err := buildGrowingInsertData(schema, pack)
	if err != nil {
		return nil, retry.Unrecoverable(err)
	}

	metaCache := newGrowingSegmentMetaCache(pack.Meta, schema)
	syncPack := new(syncmgr.SyncPack).
		WithCollectionID(pack.CollectionID).
		WithPartitionID(pack.PartitionID).
		WithSegmentID(pack.SegmentID).
		WithChannelName(pack.VChannel).
		WithInsertData(insertData).
		WithTimeRange(pack.FromTimeTick, pack.ToTimeTick).
		WithBatchRows(int64(pack.Rows)).
		WithLevel(datapb.SegmentLevel_L1)
	if len(bm25Stats) > 0 {
		syncPack = syncPack.WithBM25Stats(bm25Stats)
	}
	request := &growingBulkWriteRequest{
		syncPack:       syncPack,
		metaCache:      metaCache,
		schema:         schema,
		insertData:     insertData,
		chunkManager:   w.chunkManager,
		allocator:      w.allocator,
		storageConfig:  w.storageConfig,
		writeRetryOpts: w.writeRetryOpts,
		storageVersion: pack.Meta.GetStorageVersion(),
		currentSplit:   currentSplitForGrowingPack(schema, insertData, pack.Meta),
		manifestPath:   manifestPathForGrowingPack(pack.Meta),
	}
	writeResult, err := writeFn(ctx, request)
	if err != nil {
		// Classification is the task layer's job: execute marks retryable
		// errors with ErrDelay and fails the segment on unrecoverable ones.
		return nil, err
	}

	return &flushResult{
		PersistedStorage: &streamingpb.L1SegmentPersistedStorage{
			ManifestPath: writeResult.manifestPath,
			Statistics:   writeResult.statistics,
			Binlogs: []*streamingpb.L1SegmentBinLogs{
				{
					FieldBinlog:  storage.SortFieldBinlogs(writeResult.insertBinlogs),
					StatsBinlog:  storage.SortFieldBinlogs(writeResult.statsBinlogs),
					Bm25Binlog:   storage.SortFieldBinlogs(writeResult.bm25Binlogs),
					FromTimeTick: pack.FromTimeTick,
					ToTimeTick:   pack.ToTimeTick,
				},
			},
		},
	}, nil
}

func buildGrowingInsertData(schema *schemapb.CollectionSchema, pack *flushPack) ([]*storage.InsertData, map[int64]*storage.BM25Stats, error) {
	pkField, err := typeutil.GetPrimaryFieldSchema(schema)
	if err != nil {
		return nil, nil, err
	}
	insertMessages := make([]*msgstream.InsertMsg, 0, len(pack.Inserts))
	for _, raw := range pack.Inserts {
		if err := forEachSegmentInsertMessage(raw, pack.SegmentID, func(insert segmentInsertMessage) error {
			// A single insert message body carries one contiguous row range
			// assigned wholesale to exactly one segment; the producer builds
			// one message per segment (see proxy task_insert_streaming.go).
			// Reject a multi-partition message here instead of persisting the
			// whole body into every segment it is assigned to.
			if partitions := insert.Message.Header().GetPartitions(); len(partitions) > 1 {
				return merr.WrapErrServiceInternalMsg(
					"unsupported multi-partition insert message for segment %d: %d partitions, body cannot be split by segment",
					pack.SegmentID, len(partitions))
			}
			// MustBody panics on unmarshal failure, so request is never nil
			// here.
			request := insert.Message.MustBody()
			request.ShardName = pack.VChannel
			request.CollectionID = pack.CollectionID
			request.PartitionID = insert.Assignment.GetPartitionId()
			request.SegmentID = insert.Assignment.GetSegmentAssignment().GetSegmentId()
			// The proxy fills Timestamps with the TSOs it allocated at receive
			// time; persist the WAL timetick instead so every replica of this
			// chunk and the checkpoint agree on the same timestamp column (see
			// recoverInsertMsgFromHeader in pkg/streaming/util/message/adaptor).
			timestamps := make([]uint64, request.GetNumRows())
			for i := range timestamps {
				timestamps[i] = insert.TimeTick
			}
			request.Timestamps = timestamps
			if request.Base != nil {
				request.Base.Timestamp = insert.TimeTick
			}
			insertMessages = append(insertMessages, &msgstream.InsertMsg{
				BaseMsg: msgstream.BaseMsg{
					BeginTimestamp: insert.TimeTick,
					EndTimestamp:   insert.TimeTick,
				},
				InsertRequest: request,
			})
			return nil
		}); err != nil {
			return nil, nil, err
		}
	}
	if len(insertMessages) == 0 {
		return nil, nil, merr.WrapErrServiceInternalMsg("growing insert pack has no insert messages, segmentID=%d", pack.SegmentID)
	}
	prepared, err := writebuffer.PrepareInsert(schema, pkField, insertMessages)
	if err != nil {
		return nil, nil, err
	}
	insertData := lo.FlatMap(prepared, func(data *writebuffer.InsertData, _ int) []*storage.InsertData {
		if data.GetSegmentID() != pack.SegmentID {
			return nil
		}
		return data.GetDatas()
	})
	var bm25Stats map[int64]*storage.BM25Stats
	for _, data := range prepared {
		if data.GetSegmentID() != pack.SegmentID {
			continue
		}
		for fieldID, stats := range data.GetBM25Stats() {
			if bm25Stats == nil {
				bm25Stats = make(map[int64]*storage.BM25Stats)
			}
			bm25Stats[fieldID] = stats
		}
	}
	return insertData, bm25Stats, nil
}

func writeGrowingBulkPack(ctx context.Context, req *growingBulkWriteRequest) (*growingBulkWriteResult, error) {
	switch req.storageVersion {
	case storage.StorageV2:
		writer := syncmgr.NewBulkPackWriterV2(
			req.metaCache,
			req.schema,
			req.chunkManager,
			req.allocator,
			0,
			packed.DefaultMultiPartUploadSize,
			req.storageConfig,
			req.currentSplit,
			req.writeRetryOpts...,
		)
		inserts, _, stats, bm25Stats, manifest, _, statistics, err := writer.Write(ctx, req.syncPack)
		return &growingBulkWriteResult{insertBinlogs: inserts, statsBinlogs: stats, bm25Binlogs: bm25Stats, manifestPath: manifest, statistics: statistics}, err
	case storage.StorageV3:
		writer := syncmgr.NewBulkPackWriterV3(
			req.metaCache,
			req.schema,
			req.chunkManager,
			req.allocator,
			0,
			packed.DefaultMultiPartUploadSize,
			req.storageConfig,
			req.currentSplit,
			req.manifestPath,
			req.writeRetryOpts...,
		)
		inserts, _, stats, bm25Stats, manifest, _, statistics, err := writer.Write(ctx, req.syncPack)
		return &growingBulkWriteResult{insertBinlogs: inserts, statsBinlogs: stats, bm25Binlogs: bm25Stats, manifestPath: manifest, statistics: statistics}, err
	default:
		writer, err := syncmgr.NewBulkPackWriter(req.metaCache, req.schema, req.chunkManager, req.allocator, req.writeRetryOpts...)
		if err != nil {
			return nil, err
		}
		inserts, _, stats, bm25Stats, _, err := writer.Write(ctx, req.syncPack)
		return &growingBulkWriteResult{insertBinlogs: inserts, statsBinlogs: stats, bm25Binlogs: bm25Stats}, err
	}
}

func newGrowingSegmentMetaCache(meta *streamingpb.SegmentAssignmentMeta, schema *schemapb.CollectionSchema) metacache.MetaCache {
	return metacache.NewMetaCache(&datapb.ChannelWatchInfo{
		Vchan: &datapb.VchannelInfo{
			CollectionID: meta.GetCollectionId(),
			ChannelName:  meta.GetVchannel(),
			UnflushedSegments: []*datapb.SegmentInfo{
				newGrowingSegmentInfo(meta),
			},
		},
		Schema: schema,
	}, func(*datapb.SegmentInfo) pkoracle.PkStat {
		return pkoracle.NewBloomFilterSet()
	}, metacache.NewBM25StatsFactory)
}

func newGrowingSegmentInfo(meta *streamingpb.SegmentAssignmentMeta) *datapb.SegmentInfo {
	persistedStorage := meta.GetPersistedStorage()
	return &datapb.SegmentInfo{
		ID:             meta.GetSegmentId(),
		CollectionID:   meta.GetCollectionId(),
		PartitionID:    meta.GetPartitionId(),
		InsertChannel:  meta.GetVchannel(),
		NumOfRows:      int64(meta.GetStat().GetModifiedRows()),
		State:          commonpb.SegmentState_Growing,
		Level:          meta.GetStat().GetLevel(),
		StorageVersion: meta.GetStorageVersion(),
		Binlogs:        persistedFieldBinlogs(persistedStorage, func(binlog *streamingpb.L1SegmentBinLogs) []*datapb.FieldBinlog { return binlog.GetFieldBinlog() }),
		Statslogs:      persistedFieldBinlogs(persistedStorage, func(binlog *streamingpb.L1SegmentBinLogs) []*datapb.FieldBinlog { return binlog.GetStatsBinlog() }),
		Bm25Statslogs:  persistedFieldBinlogs(persistedStorage, func(binlog *streamingpb.L1SegmentBinLogs) []*datapb.FieldBinlog { return binlog.GetBm25Binlog() }),
		Deltalogs:      persistedStorage.GetDeltaBinlog(),
		Stats:          persistedStorage.GetStatistics(),
		ManifestPath:   manifestPathForGrowingPack(meta),
		StartPosition:  &msgpb.MsgPosition{ChannelName: meta.GetVchannel(), Timestamp: meta.GetStat().GetCreateSegmentTimeTick()},
		DmlPosition:    &msgpb.MsgPosition{ChannelName: meta.GetVchannel(), Timestamp: meta.GetCheckpointTimeTick()},
	}
}

func persistedFieldBinlogs(
	storage *streamingpb.L1SegmentPersistedStorage,
	pick func(*streamingpb.L1SegmentBinLogs) []*datapb.FieldBinlog,
) []*datapb.FieldBinlog {
	if storage == nil {
		return nil
	}
	return lo.FlatMap(storage.GetBinlogs(), func(binlog *streamingpb.L1SegmentBinLogs, _ int) []*datapb.FieldBinlog {
		return lo.Map(pick(binlog), func(fieldBinlog *datapb.FieldBinlog, _ int) *datapb.FieldBinlog {
			return proto.Clone(fieldBinlog).(*datapb.FieldBinlog)
		})
	})
}

func currentSplitFromPersistedStorage(schema *schemapb.CollectionSchema, storage *streamingpb.L1SegmentPersistedStorage) []storagecommon.ColumnGroup {
	if storage == nil {
		return nil
	}
	fieldIndexes := make(map[int64]int)
	for idx, field := range typeutil.GetAllFieldSchemas(schema) {
		fieldIndexes[field.GetFieldID()] = idx
	}
	for _, binlogBatch := range storage.GetBinlogs() {
		if len(binlogBatch.GetFieldBinlog()) == 0 {
			continue
		}
		result := make([]storagecommon.ColumnGroup, 0, len(binlogBatch.GetFieldBinlog()))
		for _, fieldBinlog := range binlogBatch.GetFieldBinlog() {
			fields := fieldBinlog.GetChildFields()
			if len(fields) == 0 {
				return nil
			}
			result = append(result, storagecommon.ColumnGroup{
				GroupID: fieldBinlog.GetFieldID(),
				Fields:  fields,
				Columns: lo.Map(fields, func(fieldID int64, _ int) int { return fieldIndexes[fieldID] }),
				Format:  fieldBinlog.GetFormat(),
			})
		}
		return result
	}
	return nil
}

func currentSplitForGrowingPack(
	schema *schemapb.CollectionSchema,
	insertData []*storage.InsertData,
	meta *streamingpb.SegmentAssignmentMeta,
) []storagecommon.ColumnGroup {
	switch meta.GetStorageVersion() {
	case storage.StorageV2, storage.StorageV3:
	default:
		return nil
	}

	currentSplit := currentSplitFromPersistedStorage(schema, meta.GetPersistedStorage())
	writerFormat := paramtable.Get().DataNodeCfg.StorageFormat.GetValue()
	if len(currentSplit) > 0 {
		if meta.GetStorageVersion() == storage.StorageV3 {
			return currentSplit
		}
		return storagecommon.FillColumnGroupFormats(currentSplit, writerFormat)
	}

	currentSplit = storagecommon.SplitColumns(
		typeutil.GetAllFieldSchemas(schema),
		calcGrowingColumnStats(insertData),
		storagecommon.DefaultPolicies()...,
	)
	return storagecommon.FillColumnGroupFormats(currentSplit, writerFormat)
}

func calcGrowingColumnStats(insertData []*storage.InsertData) map[int64]storagecommon.ColumnStats {
	result := make(map[int64]storagecommon.ColumnStats)
	memorySizes := make(map[int64]int64)
	rowNums := make(map[int64]int64)
	for _, data := range insertData {
		for fieldID, fieldData := range data.Data {
			memorySizes[fieldID] += int64(fieldData.GetMemorySize())
			rowNums[fieldID] += int64(fieldData.RowNum())
		}
	}
	for fieldID, rowNum := range rowNums {
		if rowNum > 0 {
			result[fieldID] = storagecommon.ColumnStats{
				AvgSize: memorySizes[fieldID] / rowNum,
			}
		}
	}
	return result
}

func manifestPathForGrowingPack(meta *streamingpb.SegmentAssignmentMeta) string {
	if manifest := meta.GetPersistedStorage().GetManifestPath(); manifest != "" {
		return manifest
	}
	if meta.GetStorageVersion() != storage.StorageV3 {
		return ""
	}
	k := metautil.JoinIDPath(meta.GetCollectionId(), meta.GetPartitionId(), meta.GetSegmentId())
	basePath := path.Join(paramtable.Get().MinioCfg.RootPath.GetValue(), common.SegmentInsertLogPath, k)
	return packed.MarshalManifestPath(basePath, packed.ManifestEarliest)
}

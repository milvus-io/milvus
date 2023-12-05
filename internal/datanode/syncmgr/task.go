package syncmgr

import (
	"context"
	"path"
	"strconv"

	"github.com/samber/lo"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/msgpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/internal/allocator"
	"github.com/milvus-io/milvus/internal/datanode/metacache"
	"github.com/milvus-io/milvus/internal/proto/datapb"
	"github.com/milvus-io/milvus/internal/proto/etcdpb"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/pkg/common"
	"github.com/milvus-io/milvus/pkg/log"
	"github.com/milvus-io/milvus/pkg/util/merr"
	"github.com/milvus-io/milvus/pkg/util/metautil"
	"github.com/milvus-io/milvus/pkg/util/retry"
	"github.com/milvus-io/milvus/pkg/util/typeutil"
)

type SyncTask struct {
	chunkManager storage.ChunkManager
	allocator    allocator.Interface

	insertData *storage.InsertData
	deleteData *storage.DeleteData

	segment       *metacache.SegmentInfo
	collectionID  int64
	partitionID   int64
	segmentID     int64
	channelName   string
	schema        *schemapb.CollectionSchema
	startPosition *msgpb.MsgPosition
	checkpoint    *msgpb.MsgPosition
	// batchSize is the row number of this sync task,
	// not the total num of rows of segemnt
	batchSize int64
	level     datapb.SegmentLevel

	tsFrom typeutil.Timestamp
	tsTo   typeutil.Timestamp

	isFlush bool
	isDrop  bool

	metacache  metacache.MetaCache
	metaWriter MetaWriter

	insertBinlogs map[int64]*datapb.FieldBinlog // map[int64]*datapb.Binlog
	statsBinlogs  map[int64]*datapb.FieldBinlog // map[int64]*datapb.Binlog
	deltaBinlog   *datapb.FieldBinlog

	segmentData map[string][]byte

	writeRetryOpts []retry.Option

	failureCallback func(err error)
}

func (t *SyncTask) getLogger() *log.MLogger {
	return log.Ctx(context.Background()).With(
		zap.Int64("collectionID", t.collectionID),
		zap.Int64("partitionID", t.partitionID),
		zap.Int64("segmentID", t.segmentID),
		zap.String("channel", t.channelName),
	)
}

func (t *SyncTask) handleError(err error) {
	if t.failureCallback != nil {
		t.failureCallback(err)
	}
}

func (t *SyncTask) Run() error {
	log := t.getLogger()
	var err error
	var has bool

	t.segment, has = t.metacache.GetSegmentByID(t.segmentID)
	if !has {
		log.Warn("failed to sync data, segment not found in metacache")
		err := merr.WrapErrSegmentNotFound(t.segmentID)
		t.handleError(err)
		return err
	}

	if t.segment.CompactTo() == metacache.NullSegment {
		log.Info("segment compacted to zero-length segment, discard sync task")
		return nil
	}

	if t.segment.CompactTo() > 0 {
		log.Info("syncing segment compacted, update segment id", zap.Int64("compactTo", t.segment.CompactTo()))
		// update sync task segment id
		// it's ok to use compactTo segmentID here, since there shall be no insert for compacted segment
		t.segmentID = t.segment.CompactTo()
	}

	err = t.serializeInsertData()
	if err != nil {
		log.Warn("failed to serialize insert data", zap.Error(err))
		t.handleError(err)
		return err
	}

	err = t.serializeDeleteData()
	if err != nil {
		log.Warn("failed to serialize delete data", zap.Error(err))
		t.handleError(err)
		return err
	}

	err = t.writeLogs()
	if err != nil {
		log.Warn("failed to save serialized data into storage", zap.Error(err))
		t.handleError(err)
		return err
	}

	if t.metaWriter != nil {
		err = t.writeMeta()
		if err != nil {
			log.Warn("failed to save serialized data into storage", zap.Error(err))
			t.handleError(err)
			return err
		}
	}

	actions := []metacache.SegmentAction{metacache.FinishSyncing(t.batchSize)}
	switch {
	case t.isDrop:
		actions = append(actions, metacache.UpdateState(commonpb.SegmentState_Dropped))
	case t.isFlush:
		actions = append(actions, metacache.UpdateState(commonpb.SegmentState_Flushed))
	}

	t.metacache.UpdateSegments(metacache.MergeSegmentAction(actions...), metacache.WithSegmentIDs(t.segment.SegmentID()))

	log.Info("task done")
	return nil
}

func (t *SyncTask) serializeInsertData() error {
	err := t.serializeBinlog()
	if err != nil {
		return err
	}

	err = t.serializePkStatsLog()
	if err != nil {
		return err
	}

	return nil
}

func (t *SyncTask) serializeDeleteData() error {
	if t.deleteData == nil {
		return nil
	}

	delCodec := storage.NewDeleteCodec()
	blob, err := delCodec.Serialize(t.collectionID, t.partitionID, t.segmentID, t.deleteData)
	if err != nil {
		return err
	}

	logID, err := t.allocator.AllocOne()
	if err != nil {
		log.Error("failed to alloc ID", zap.Error(err))
		return err
	}

	value := blob.GetValue()
	data := &datapb.Binlog{}

	blobKey := metautil.JoinIDPath(t.collectionID, t.partitionID, t.segmentID, logID)
	blobPath := path.Join(t.chunkManager.RootPath(), common.SegmentDeltaLogPath, blobKey)

	t.segmentData[blobPath] = value
	data.LogSize = int64(len(blob.Value))
	data.LogPath = blobPath
	data.TimestampFrom = t.tsFrom
	data.TimestampTo = t.tsTo
	data.EntriesNum = t.deleteData.RowCount
	t.appendDeltalog(data)

	return nil
}

func (t *SyncTask) serializeBinlog() error {
	if t.insertData == nil {
		return nil
	}

	// get memory size of buffer data
	memSize := make(map[int64]int)
	for fieldID, fieldData := range t.insertData.Data {
		memSize[fieldID] = fieldData.GetMemorySize()
	}

	inCodec := t.getInCodec()

	blobs, err := inCodec.Serialize(t.partitionID, t.segmentID, t.insertData)
	if err != nil {
		return err
	}

	logidx, _, err := t.allocator.Alloc(uint32(len(blobs)))
	if err != nil {
		return err
	}

	for _, blob := range blobs {
		fieldID, err := strconv.ParseInt(blob.GetKey(), 10, 64)
		if err != nil {
			log.Error("Flush failed ... cannot parse string to fieldID ..", zap.Error(err))
			return err
		}

		k := metautil.JoinIDPath(t.collectionID, t.partitionID, t.segmentID, fieldID, logidx)
		// [rootPath]/[insert_log]/key
		key := path.Join(t.chunkManager.RootPath(), common.SegmentInsertLogPath, k)
		t.segmentData[key] = blob.GetValue()
		t.appendBinlog(fieldID, &datapb.Binlog{
			EntriesNum:    blob.RowNum,
			TimestampFrom: t.tsFrom,
			TimestampTo:   t.tsTo,
			LogPath:       key,
			LogSize:       int64(memSize[fieldID]),
		})

		logidx += 1
	}
	return nil
}

func (t *SyncTask) convertInsertData2PkStats(pkFieldID int64, dataType schemapb.DataType) (*storage.PrimaryKeyStats, int64) {
	pkFieldData := t.insertData.Data[pkFieldID]

	rowNum := int64(pkFieldData.RowNum())

	stats, err := storage.NewPrimaryKeyStats(pkFieldID, int64(dataType), rowNum)
	if err != nil {
		return nil, 0
	}
	stats.UpdateByMsgs(pkFieldData)
	return stats, rowNum
}

func (t *SyncTask) serializeSinglePkStats(fieldID int64, stats *storage.PrimaryKeyStats, rowNum int64) error {
	blob, err := t.getInCodec().SerializePkStats(stats, rowNum)
	if err != nil {
		return err
	}

	logidx, err := t.allocator.AllocOne()
	if err != nil {
		return err
	}
	t.convertBlob2StatsBinlog(blob, fieldID, logidx, rowNum)

	return nil
}

func (t *SyncTask) serializeMergedPkStats(fieldID int64, pkType schemapb.DataType) error {
	segments := t.metacache.GetSegmentsBy(metacache.WithSegmentIDs(t.segmentID))
	var statsList []*storage.PrimaryKeyStats
	var totalRowNum int64
	for _, segment := range segments {
		totalRowNum += segment.NumOfRows()
		statsList = append(statsList, lo.Map(segment.GetHistory(), func(pks *storage.PkStatistics, _ int) *storage.PrimaryKeyStats {
			return &storage.PrimaryKeyStats{
				FieldID: fieldID,
				MaxPk:   pks.MaxPK,
				MinPk:   pks.MinPK,
				BF:      pks.PkFilter,
				PkType:  int64(pkType),
			}
		})...)
	}

	blob, err := t.getInCodec().SerializePkStatsList(statsList, totalRowNum)
	if err != nil {
		return err
	}
	t.convertBlob2StatsBinlog(blob, fieldID, int64(storage.CompoundStatsType), totalRowNum)

	return nil
}

func (t *SyncTask) convertBlob2StatsBinlog(blob *storage.Blob, fieldID, logID int64, rowNum int64) {
	key := metautil.JoinIDPath(t.collectionID, t.partitionID, t.segmentID, fieldID, logID)
	key = path.Join(t.chunkManager.RootPath(), common.SegmentStatslogPath, key)

	value := blob.GetValue()
	t.segmentData[key] = value
	t.appendStatslog(fieldID, &datapb.Binlog{
		EntriesNum:    rowNum,
		TimestampFrom: t.tsFrom,
		TimestampTo:   t.tsTo,
		LogPath:       key,
		LogSize:       int64(len(value)),
	})
}

func (t *SyncTask) serializePkStatsLog() error {
	pkField := lo.FindOrElse(t.schema.GetFields(), nil, func(field *schemapb.FieldSchema) bool { return field.GetIsPrimaryKey() })
	if pkField == nil {
		return merr.WrapErrServiceInternal("cannot find pk field")
	}
	fieldID := pkField.GetFieldID()
	if t.insertData != nil {
		stats, rowNum := t.convertInsertData2PkStats(fieldID, pkField.GetDataType())
		if stats != nil && rowNum > 0 {
			err := t.serializeSinglePkStats(fieldID, stats, rowNum)
			if err != nil {
				return err
			}
		}
	}

	// skip statslog for empty segment
	// DO NOT use level check here since Level zero segment may contain insert data in the future
	if t.isFlush && t.segment.NumOfRows() > 0 {
		return t.serializeMergedPkStats(fieldID, pkField.GetDataType())
	}
	return nil
}

func (t *SyncTask) appendBinlog(fieldID int64, binlog *datapb.Binlog) {
	fieldBinlog, ok := t.insertBinlogs[fieldID]
	if !ok {
		fieldBinlog = &datapb.FieldBinlog{
			FieldID: fieldID,
		}
		t.insertBinlogs[fieldID] = fieldBinlog
	}

	fieldBinlog.Binlogs = append(fieldBinlog.Binlogs, binlog)
}

func (t *SyncTask) appendStatslog(fieldID int64, statlog *datapb.Binlog) {
	fieldBinlog, ok := t.statsBinlogs[fieldID]
	if !ok {
		fieldBinlog = &datapb.FieldBinlog{
			FieldID: fieldID,
		}
		t.statsBinlogs[fieldID] = fieldBinlog
	}
	fieldBinlog.Binlogs = append(fieldBinlog.Binlogs, statlog)
}

func (t *SyncTask) appendDeltalog(deltalog *datapb.Binlog) {
	t.deltaBinlog.Binlogs = append(t.deltaBinlog.Binlogs, deltalog)
}

// writeLogs writes log files (binlog/deltalog/statslog) into storage via chunkManger.
func (t *SyncTask) writeLogs() error {
	return retry.Do(context.Background(), func() error {
		return t.chunkManager.MultiWrite(context.Background(), t.segmentData)
	}, t.writeRetryOpts...)
}

// writeMeta updates segments via meta writer in option.
func (t *SyncTask) writeMeta() error {
	return t.metaWriter.UpdateSync(t)
}

func (t *SyncTask) getInCodec() *storage.InsertCodec {
	meta := &etcdpb.CollectionMeta{
		Schema: t.schema,
		ID:     t.collectionID,
	}

	return storage.NewInsertCodecWithSchema(meta)
}

func (t *SyncTask) SegmentID() int64 {
	return t.segmentID
}

func (t *SyncTask) Checkpoint() *msgpb.MsgPosition {
	return t.checkpoint
}

func (t *SyncTask) StartPosition() *msgpb.MsgPosition {
	return t.startPosition
}

func (t *SyncTask) ChannelName() string {
	return t.channelName
}

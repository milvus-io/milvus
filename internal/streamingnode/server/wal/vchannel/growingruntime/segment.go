package growingruntime

import (
	"context"
	"fmt"
	"sync"

	"github.com/cockroachdb/errors"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/msgpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/walview"
	"github.com/milvus-io/milvus/internal/util/segcore"
	"github.com/milvus-io/milvus/internal/views/qviews"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/mq/msgstream"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/querypb"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

// growingSegment wraps the queryable segcore state and lifecycle metadata for
// one growing segment.
type growingSegment struct {
	mu                     sync.Mutex
	collection             *segcore.CCollection
	segmentID              int64
	partitionID            int64
	segment                segcore.CSegment
	flushed                bool
	flushTimeTick          uint64
	sealedAtDataVersion    qviews.DataVersion
	hasSealedAtDataVersion bool
	released               bool
	refs                   int
}

func newGrowingSegment(collection *segcore.CCollection, segmentID int64, partitionID int64) *growingSegment {
	return &growingSegment{
		collection:  collection,
		segmentID:   segmentID,
		partitionID: partitionID,
	}
}

func newGrowingSegmentFromVisible(ctx context.Context, collection *segcore.CCollection, visible walview.VisibleSegment) (*growingSegment, error) {
	segment := newGrowingSegment(collection, visible.SegmentID, visible.PartitionID)
	if visible.Data.PersistedStorage != nil {
		if err := segment.loadPersisted(ctx, visible); err != nil {
			segment.release()
			return nil, err
		}
	}
	for _, msg := range visible.Data.InsertMessages {
		if err := segment.applySnapshotInsert(ctx, msg); err != nil {
			segment.release()
			return nil, err
		}
	}
	if visible.SealedAtDataVersion != nil {
		segment.markFlushed(visible.Assignment.GetCheckpointTimeTick())
		segment.markSealed(qviews.FromProtoDataVersion(visible.SealedAtDataVersion))
	}
	return segment, nil
}

func newGrowingSegmentFromFlushed(collection *segcore.CCollection, flushed walview.FlushedSegment) *growingSegment {
	segment := newGrowingSegment(collection, flushed.SegmentID, flushed.PartitionID)
	segment.markFlushed(flushed.FlushTimeTick)
	segment.markSealed(flushed.SealedAtDataVersion)
	return segment
}

func (s *growingSegment) id() int64 {
	if s == nil {
		return 0
	}
	return s.segmentID
}

func (s *growingSegment) csegment() (segcore.CSegment, bool) {
	if s == nil {
		return nil, false
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.segment, s.segment != nil
}

func (s *growingSegment) pinIfNotReleased() (segcore.CSegment, bool) {
	if s == nil {
		return nil, false
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.released || s.segment == nil {
		return nil, false
	}
	s.refs++
	return s.segment, true
}

func (s *growingSegment) unpin() {
	if s == nil {
		return
	}
	s.mu.Lock()
	s.refs--
	segment := s.releaseIfIdleLocked()
	s.mu.Unlock()
	if segment != nil {
		segment.Release()
	}
}

func (s *growingSegment) isFlushed() bool {
	if s == nil {
		return false
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.flushed
}

func (s *growingSegment) loadPersisted(ctx context.Context, visible walview.VisibleSegment) error {
	if s == nil || s.collection == nil {
		return nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.released {
		return errors.Errorf("growing segment %d released", s.segmentID)
	}
	if s.segment != nil {
		return nil
	}
	csegment, err := segcore.CreateCSegment(&segcore.CreateCSegmentRequest{
		Collection:  s.collection,
		SegmentID:   s.segmentID,
		SegmentType: segcore.SegmentTypeGrowing,
		LoadInfo:    loadInfoFromVisibleSegment(visible),
	})
	if err != nil {
		return err
	}
	s.segment = csegment
	return csegment.Load(ctx)
}

func (s *growingSegment) ensureCSegment(timetick uint64) error {
	if s == nil || s.collection == nil {
		return nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.released {
		return errors.Errorf("growing segment %d released", s.segmentID)
	}
	if s.flushed {
		if s.shouldSkipFlushedReplayLocked(timetick) {
			return nil
		}
		return errors.Errorf("growing segment %d already flushed at timetick %d", s.segmentID, s.flushTimeTick)
	}
	if s.segment != nil {
		return nil
	}
	segment, err := segcore.CreateCSegment(&segcore.CreateCSegmentRequest{
		Collection:  s.collection,
		SegmentID:   s.segmentID,
		SegmentType: segcore.SegmentTypeGrowing,
	})
	if err != nil {
		return err
	}
	s.segment = segment
	return nil
}

func (s *growingSegment) applySnapshotInsert(ctx context.Context, raw message.ImmutableMessage) error {
	if s == nil {
		return nil
	}
	return walview.ForEachSegmentInsertMessage(raw, s.segmentID, func(insert walview.SegmentInsertMessage) error {
		return s.applyInsert(ctx, insert)
	})
}

func (s *growingSegment) applyInsert(ctx context.Context, insert walview.SegmentInsertMessage) error {
	if s == nil {
		return nil
	}
	s.mu.Lock()
	if s.released {
		s.mu.Unlock()
		return errors.Errorf("growing segment %d released", s.segmentID)
	}
	if s.flushed {
		if s.shouldSkipFlushedReplayLocked(insert.TimeTick) {
			s.mu.Unlock()
			return nil
		}
		s.mu.Unlock()
		return errors.Errorf("growing segment %d already flushed at timetick %d", s.segmentID, s.flushTimeTick)
	}
	s.mu.Unlock()
	if s.collection == nil {
		return nil
	}
	body := insert.Message.MustBody()
	if body == nil {
		return errors.New("growing insert message has nil request")
	}
	request := proto.Clone(body).(*msgpb.InsertRequest)
	request.PartitionID = insert.Assignment.GetPartitionId()
	request.SegmentID = s.segmentID
	request.Timestamps = insertTimestampsFromRequest(insert.TimeTick, request)
	insertMsg := &msgstream.InsertMsg{
		BaseMsg: msgstream.BaseMsg{
			BeginTimestamp: insert.TimeTick,
			EndTimestamp:   insert.TimeTick,
		},
		InsertRequest: request,
	}
	schema := s.collection.Schema()
	record, skippedFields, err := storage.TransferInsertMsgToInsertRecord(schema, insertMsg)
	if err != nil {
		return err
	}
	if len(skippedFields) > 0 {
		mlog.Warn(ctx, "skip insert payload fields absent from current schema while applying growing segment insert",
			mlog.FieldCollectionID(request.GetCollectionID()),
			mlog.FieldSegmentID(s.segmentID),
			mlog.Int64("partitionID", request.GetPartitionID()),
			mlog.Int32("schemaVersion", schema.GetVersion()),
			mlog.Int64s("skippedFieldIDs", skippedFields))
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.released {
		return errors.Errorf("growing segment %d released", s.segmentID)
	}
	if s.flushed {
		if s.shouldSkipFlushedReplayLocked(insert.TimeTick) {
			return nil
		}
		return errors.Errorf("growing segment %d already flushed at timetick %d", s.segmentID, s.flushTimeTick)
	}
	if s.segment == nil {
		if err := s.ensureCSegmentLocked(); err != nil {
			return err
		}
	}
	_, err = s.segment.Insert(ctx, &segcore.InsertRequest{
		RowIDs:     request.GetRowIDs(),
		Timestamps: request.GetTimestamps(),
		Record:     record,
	})
	if err == nil {
		minTS, maxTS := timestampBounds(request.GetTimestamps())
		mlog.Debug(ctx, "applied insert to growing segment",
			mlog.FieldCollectionID(request.GetCollectionID()),
			mlog.FieldPartitionID(request.GetPartitionID()),
			mlog.FieldSegmentID(s.segmentID),
			mlog.Uint64("timeTick", insert.TimeTick),
			mlog.Int("rowIDCount", len(request.GetRowIDs())),
			mlog.Int("timestampCount", len(request.GetTimestamps())),
			mlog.Uint64("timestampMin", minTS),
			mlog.Uint64("timestampMax", maxTS),
		)
	}
	return err
}

func (s *growingSegment) ensureCSegmentLocked() error {
	segment, err := segcore.CreateCSegment(&segcore.CreateCSegmentRequest{
		Collection:  s.collection,
		SegmentID:   s.segmentID,
		SegmentType: segcore.SegmentTypeGrowing,
	})
	if err != nil {
		return err
	}
	s.segment = segment
	return nil
}

func (s *growingSegment) applyDelete(ctx context.Context, primaryKeys storage.PrimaryKeys, timestamps []typeutil.Timestamp) error {
	if s == nil || primaryKeys.Len() == 0 {
		return nil
	}
	if len(timestamps) == 0 {
		timestamps = make([]typeutil.Timestamp, primaryKeys.Len())
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.segment == nil {
		return nil
	}
	_, err := s.segment.Delete(ctx, &segcore.DeleteRequest{
		PrimaryKeys: primaryKeys,
		Timestamps:  timestamps,
	})
	if err == nil {
		minTS, maxTS := timestampBounds(timestamps)
		mlog.Debug(ctx, "applied delete to growing segment",
			mlog.FieldSegmentID(s.segmentID),
			mlog.Int("primaryKeyCount", primaryKeys.Len()),
			mlog.Strings("primaryKeySamples", primaryKeySamples(primaryKeys, 8)),
			mlog.Int("timestampCount", len(timestamps)),
			mlog.Uint64("timestampMin", minTS),
			mlog.Uint64("timestampMax", maxTS),
		)
	}
	return err
}

func (s *growingSegment) markFlushed(timetick uint64) {
	if s == nil {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.flushed = true
	if timetick > s.flushTimeTick {
		s.flushTimeTick = timetick
	}
}

func (s *growingSegment) markSealed(sealedAt qviews.DataVersion) {
	if s == nil {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.hasSealedAtDataVersion && !s.sealedAtDataVersion.EQ(sealedAt) {
		panic("conflicting sealed data version for growing segment")
	}
	s.sealedAtDataVersion = sealedAt
	s.hasSealedAtDataVersion = true
}

func (s *growingSegment) shouldReleaseAt(truncateTo qviews.DataVersion, appliedGrowingTimeTick uint64) bool {
	if s == nil {
		return false
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.hasSealedAtDataVersion &&
		truncateTo.GTE(s.sealedAtDataVersion) &&
		s.safeToReleaseForReplayLocked(appliedGrowingTimeTick)
}

func (s *growingSegment) shouldSkipFlushedReplayLocked(timetick uint64) bool {
	return s.flushed && s.flushTimeTick > 0 && timetick <= s.flushTimeTick
}

func (s *growingSegment) safeToReleaseForReplayLocked(appliedGrowingTimeTick uint64) bool {
	return s.flushTimeTick == 0 || appliedGrowingTimeTick > s.flushTimeTick
}

func (s *growingSegment) release() {
	if s == nil {
		return
	}
	s.mu.Lock()
	if s.released {
		s.mu.Unlock()
		return
	}
	s.released = true
	segment := s.releaseIfIdleLocked()
	s.mu.Unlock()
	if segment != nil {
		segment.Release()
	}
}

func (s *growingSegment) releaseIfIdleLocked() segcore.CSegment {
	if !s.released || s.refs > 0 || s.segment == nil {
		return nil
	}
	segment := s.segment
	s.segment = nil
	return segment
}

func loadInfoFromVisibleSegment(segment walview.VisibleSegment) *querypb.SegmentLoadInfo {
	persisted := segment.Data.PersistedStorage
	if persisted == nil {
		return nil
	}
	loadInfo := &querypb.SegmentLoadInfo{
		SegmentID:      segment.SegmentID,
		PartitionID:    segment.PartitionID,
		Level:          datapb.SegmentLevel_L1,
		BinlogPaths:    make([]*datapb.FieldBinlog, 0),
		Statslogs:      make([]*datapb.FieldBinlog, 0),
		Bm25Logs:       make([]*datapb.FieldBinlog, 0),
		ManifestPath:   persisted.GetManifestPath(),
		StorageVersion: storage.StorageV2,
	}
	if persisted.GetManifestPath() != "" {
		loadInfo.StorageVersion = storage.StorageV3
	}
	if segment.Assignment != nil {
		loadInfo.CollectionID = segment.Assignment.GetCollectionId()
		loadInfo.InsertChannel = segment.Assignment.GetVchannel()
	}
	for _, binlogs := range persisted.GetBinlogs() {
		loadInfo.BinlogPaths = append(loadInfo.BinlogPaths, binlogs.GetFieldBinlog()...)
		loadInfo.Statslogs = append(loadInfo.Statslogs, binlogs.GetStatsBinlog()...)
		loadInfo.Bm25Logs = append(loadInfo.Bm25Logs, binlogs.GetBm25Binlog()...)
	}
	if persisted.GetMergedStatsBinlog() != nil {
		loadInfo.Statslogs = append(loadInfo.Statslogs, persisted.GetMergedStatsBinlog())
	}
	return loadInfo
}

func repeatedTimeTicks(timeTick uint64, n int) []typeutil.Timestamp {
	if n == 0 {
		return nil
	}
	timestamps := make([]typeutil.Timestamp, n)
	for i := range timestamps {
		timestamps[i] = timeTick
	}
	return timestamps
}

func deleteTimestampsFromRequest(timeTick uint64, request *msgpb.DeleteRequest) []typeutil.Timestamp {
	if request == nil {
		return nil
	}
	return repeatedTimeTicks(timeTick, primaryKeyCount(request.GetPrimaryKeys()))
}

func deleteTimestampsFromTransformLogBlock(timeTick uint64, block *streamingpb.TransformDeleteBlock) []typeutil.Timestamp {
	if block == nil {
		return nil
	}
	return repeatedTimeTicks(timeTick, primaryKeyCount(block.GetPrimaryKeys()))
}

func insertTimestampsFromRequest(timeTick uint64, request *msgpb.InsertRequest) []typeutil.Timestamp {
	if request == nil {
		return nil
	}
	rowCount := int(request.GetNumRows())
	if rowCount == 0 {
		rowCount = len(request.GetRowIDs())
	}
	if rowCount == 0 {
		rowCount = len(request.GetTimestamps())
	}
	return repeatedTimeTicks(timeTick, rowCount)
}

func primaryKeyCount(ids *schemapb.IDs) int {
	if ids == nil {
		return 0
	}
	switch ids.IdField.(type) {
	case *schemapb.IDs_IntId:
		return len(ids.GetIntId().GetData())
	case *schemapb.IDs_StrId:
		return len(ids.GetStrId().GetData())
	default:
		return 0
	}
}

func timestampBounds(timestamps []typeutil.Timestamp) (typeutil.Timestamp, typeutil.Timestamp) {
	if len(timestamps) == 0 {
		return 0, 0
	}
	minTS := timestamps[0]
	maxTS := timestamps[0]
	for _, ts := range timestamps[1:] {
		if ts < minTS {
			minTS = ts
		}
		if ts > maxTS {
			maxTS = ts
		}
	}
	return minTS, maxTS
}

func primaryKeySamples(primaryKeys storage.PrimaryKeys, limit int) []string {
	if primaryKeys == nil || limit <= 0 {
		return nil
	}
	n := primaryKeys.Len()
	if n > limit {
		n = limit
	}
	samples := make([]string, 0, n)
	for i := 0; i < n; i++ {
		samples = append(samples, fmt.Sprint(primaryKeys.Get(i).GetValue()))
	}
	return samples
}

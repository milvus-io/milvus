package segment

import (
	"context"

	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
)

type PackWriter interface {
	FlushInsertBuffer(ctx context.Context, pack *flushPack) (*flushResult, error)
}

type flushPack struct {
	Meta         *streamingpb.SegmentAssignmentMeta
	CollectionID int64
	PartitionID  int64
	SegmentID    int64
	VChannel     string
	FromTimeTick uint64
	ToTimeTick   uint64
	Schema       *schemapb.CollectionSchema
	Rows         uint64
	BinarySize   uint64
	Inserts      []message.ImmutableMessage
}

type flushResult struct {
	PersistedStorage *streamingpb.L1SegmentPersistedStorage
}

func cloneL1SegmentBinLogs(binlogs []*streamingpb.L1SegmentBinLogs) []*streamingpb.L1SegmentBinLogs {
	if len(binlogs) == 0 {
		return nil
	}
	cloned := make([]*streamingpb.L1SegmentBinLogs, 0, len(binlogs))
	for _, binlog := range binlogs {
		cloned = append(cloned, cloneL1SegmentBinLog(binlog))
	}
	return cloned
}

func cloneL1SegmentBinLog(value *streamingpb.L1SegmentBinLogs) *streamingpb.L1SegmentBinLogs {
	if value == nil {
		return nil
	}
	return proto.Clone(value).(*streamingpb.L1SegmentBinLogs)
}

func cloneFieldBinlog(value *datapb.FieldBinlog) *datapb.FieldBinlog {
	if value == nil {
		return nil
	}
	return proto.Clone(value).(*datapb.FieldBinlog)
}

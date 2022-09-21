package utils

import (
	"fmt"

	"github.com/milvus-io/milvus/api/commonpb"
	"github.com/milvus-io/milvus/api/milvuspb"
	"github.com/milvus-io/milvus/internal/proto/datapb"
	"github.com/milvus-io/milvus/internal/proto/querypb"
	"github.com/milvus-io/milvus/internal/querycoordv2/meta"
	"github.com/milvus-io/milvus/internal/util/funcutil"
)

// WrapStatus wraps status with given error code, message and errors
func WrapStatus(code commonpb.ErrorCode, msg string, errs ...error) *commonpb.Status {
	status := &commonpb.Status{
		ErrorCode: code,
		Reason:    msg,
	}

	for _, err := range errs {
		status.Reason = fmt.Sprintf("%s, err=%v", status.Reason, err)
	}

	return status
}

// WrapError wraps error with given message
func WrapError(msg string, err error) error {
	return fmt.Errorf("%s[%w]", msg, err)
}

func SegmentBinlogs2SegmentInfo(collectionID int64, partitionID int64, segmentBinlogs *datapb.SegmentBinlogs) *datapb.SegmentInfo {
	return &datapb.SegmentInfo{
		ID:            segmentBinlogs.GetSegmentID(),
		CollectionID:  collectionID,
		PartitionID:   partitionID,
		InsertChannel: segmentBinlogs.GetInsertChannel(),
		NumOfRows:     segmentBinlogs.GetNumOfRows(),
		Binlogs:       segmentBinlogs.GetFieldBinlogs(),
		Statslogs:     segmentBinlogs.GetStatslogs(),
		Deltalogs:     segmentBinlogs.GetDeltalogs(),
	}
}

func MergeMetaSegmentIntoSegmentInfo(info *querypb.SegmentInfo, segments ...*meta.Segment) {
	first := segments[0]
	if info.GetSegmentID() == 0 {
		*info = querypb.SegmentInfo{
			SegmentID:    first.GetID(),
			CollectionID: first.GetCollectionID(),
			PartitionID:  first.GetPartitionID(),
			NumRows:      first.GetNumOfRows(),
			DmChannel:    first.GetInsertChannel(),
			NodeIds:      make([]int64, 0),
			SegmentState: commonpb.SegmentState_Sealed,
		}
	}

	for _, segment := range segments {
		info.NodeIds = append(info.NodeIds, segment.Node)
	}
}

// packSegmentLoadInfo packs SegmentLoadInfo for given segment,
// packs with index if withIndex is true, this fetch indexes from IndexCoord
func PackSegmentLoadInfo(segment *datapb.SegmentInfo, indexes []*querypb.FieldIndexInfo) *querypb.SegmentLoadInfo {
	loadInfo := &querypb.SegmentLoadInfo{
		SegmentID:     segment.ID,
		PartitionID:   segment.PartitionID,
		CollectionID:  segment.CollectionID,
		BinlogPaths:   segment.Binlogs,
		NumOfRows:     segment.NumOfRows,
		Statslogs:     segment.Statslogs,
		Deltalogs:     segment.Deltalogs,
		InsertChannel: segment.InsertChannel,
		IndexInfos:    indexes,
	}
	loadInfo.SegmentSize = calculateSegmentSize(loadInfo)
	return loadInfo
}

func calculateSegmentSize(segmentLoadInfo *querypb.SegmentLoadInfo) int64 {
	segmentSize := int64(0)

	fieldIndex := make(map[int64]*querypb.FieldIndexInfo)
	for _, index := range segmentLoadInfo.IndexInfos {
		if index.EnableIndex {
			fieldID := index.FieldID
			fieldIndex[fieldID] = index
		}
	}

	for _, fieldBinlog := range segmentLoadInfo.BinlogPaths {
		fieldID := fieldBinlog.FieldID
		if index, ok := fieldIndex[fieldID]; ok {
			segmentSize += index.IndexSize
		} else {
			segmentSize += funcutil.GetFieldSizeFromFieldBinlog(fieldBinlog)
		}
	}

	// Get size of state data
	for _, fieldBinlog := range segmentLoadInfo.Statslogs {
		segmentSize += funcutil.GetFieldSizeFromFieldBinlog(fieldBinlog)
	}

	// Get size of delete data
	for _, fieldBinlog := range segmentLoadInfo.Deltalogs {
		segmentSize += funcutil.GetFieldSizeFromFieldBinlog(fieldBinlog)
	}

	return segmentSize
}

func MergeDmChannelInfo(infos []*datapb.VchannelInfo) *meta.DmChannel {
	var dmChannel *meta.DmChannel

	for _, info := range infos {
		if dmChannel == nil {
			dmChannel = meta.DmChannelFromVChannel(info)
			continue
		}

		if info.SeekPosition.GetTimestamp() < dmChannel.SeekPosition.GetTimestamp() {
			dmChannel.SeekPosition = info.SeekPosition
		}
		dmChannel.DroppedSegmentIds = append(dmChannel.DroppedSegmentIds, info.DroppedSegmentIds...)
		dmChannel.UnflushedSegmentIds = append(dmChannel.UnflushedSegmentIds, info.UnflushedSegmentIds...)
		dmChannel.FlushedSegmentIds = append(dmChannel.FlushedSegmentIds, info.FlushedSegmentIds...)
	}

	return dmChannel
}

func Replica2ReplicaInfo(replica *querypb.Replica) *milvuspb.ReplicaInfo {
	return &milvuspb.ReplicaInfo{
		ReplicaID:    replica.GetID(),
		CollectionID: replica.GetCollectionID(),
		NodeIds:      replica.GetNodes(),
	}
}

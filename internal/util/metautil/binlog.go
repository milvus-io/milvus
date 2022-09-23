package metautil

import (
	"path"
	"strconv"

	"github.com/milvus-io/milvus/internal/util/typeutil"

	"github.com/milvus-io/milvus/internal/common"
)

func BuildInsertLogPath(rootPath string, collectionID, partitionID, segmentID, fieldID, logID typeutil.UniqueID) string {
	k := JoinIDPath(collectionID, partitionID, segmentID, fieldID, logID)
	return path.Join(rootPath, common.SegmentInsertLogPath, k)
}

func BuildStatsLogPath(rootPath string, collectionID, partitionID, segmentID, fieldID, logID typeutil.UniqueID) string {
	k := JoinIDPath(collectionID, partitionID, segmentID, fieldID, logID)
	return path.Join(rootPath, common.SegmentStatslogPath, k)
}

func BuildDeltaLogPath(rootPath string, collectionID, partitionID, segmentID, logID typeutil.UniqueID) string {
	k := JoinIDPath(collectionID, partitionID, segmentID, logID)
	return path.Join(rootPath, common.SegmentDeltaLogPath, k)
}

// JoinIDPath joins ids to path format.
func JoinIDPath(ids ...typeutil.UniqueID) string {
	idStr := make([]string, 0, len(ids))
	for _, id := range ids {
		idStr = append(idStr, strconv.FormatInt(id, 10))
	}
	return path.Join(idStr...)
}

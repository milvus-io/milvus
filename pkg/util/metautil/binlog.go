package metautil

import (
	"path"
	"strconv"
	"strings"

	"github.com/milvus-io/milvus/pkg/common"
	"github.com/milvus-io/milvus/pkg/util/typeutil"
)

const pathSep = "/"

func BuildInsertLogPath(rootPath string, collectionID, partitionID, segmentID, fieldID, logID typeutil.UniqueID) string {
	k := JoinIDPath(collectionID, partitionID, segmentID, fieldID, logID)
	return path.Join(rootPath, common.SegmentInsertLogPath, k)
}

func GetSegmentIDFromInsertLogPath(logPath string) typeutil.UniqueID {
	return getSegmentIDFromPath(logPath, 3)
}

func BuildStatsLogPath(rootPath string, collectionID, partitionID, segmentID, fieldID, logID typeutil.UniqueID) string {
	k := JoinIDPath(collectionID, partitionID, segmentID, fieldID, logID)
	return path.Join(rootPath, common.SegmentStatslogPath, k)
}

func GetSegmentIDFromStatsLogPath(logPath string) typeutil.UniqueID {
	return getSegmentIDFromPath(logPath, 3)
}

func BuildDeltaLogPath(rootPath string, collectionID, partitionID, segmentID, logID typeutil.UniqueID) string {
	k := JoinIDPath(collectionID, partitionID, segmentID, logID)
	return path.Join(rootPath, common.SegmentDeltaLogPath, k)
}

func GetSegmentIDFromDeltaLogPath(logPath string) typeutil.UniqueID {
	return getSegmentIDFromPath(logPath, 2)
}

func getSegmentIDFromPath(logPath string, segmentIndex int) typeutil.UniqueID {
	infos := strings.Split(logPath, pathSep)
	l := len(infos)
	if l < segmentIndex {
		return 0
	}

	v, err := strconv.ParseInt(infos[l-segmentIndex], 10, 64)
	if err != nil {
		return 0
	}
	return v
}

// JoinIDPath joins ids to path format.
func JoinIDPath(ids ...typeutil.UniqueID) string {
	var b strings.Builder

	// Allocate bytes for the max positive int64 value plus the separator:
	b.Grow(len(ids) * 20)

	for n, id := range ids {
		if n > 0 {
			b.WriteByte('/')
		}
		b.WriteString(strconv.FormatInt(id, 10))
	}

	return b.String()
}

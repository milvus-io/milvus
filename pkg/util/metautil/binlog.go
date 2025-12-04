package metautil

import (
	"path"
	"strconv"
	"strings"

	"github.com/milvus-io/milvus/pkg/v2/common"
	"github.com/milvus-io/milvus/pkg/v2/util/typeutil"
)

const pathSep = "/"

func BuildInsertLogPath(rootPath string, collectionID, partitionID, segmentID, fieldID, logID typeutil.UniqueID) string {
	k := JoinIDPath(collectionID, partitionID, segmentID, fieldID, logID)
	return path.Join(rootPath, common.SegmentInsertLogPath, k)
}

// BuildLOBLogPath builds the path for the LOB log file.
// Format: {root_path}/insert_log/{collection_id}/{partition_id}/{segment_id}/{field_id}/lob/{log_id}
func BuildLOBLogPath(rootPath string, collectionID, partitionID, segmentID, fieldID, logID typeutil.UniqueID) string {
	basePath := BuildInsertLogPath(rootPath, collectionID, partitionID, segmentID, fieldID, logID)
	return path.Join(basePath, "lob", strconv.FormatInt(logID, 10))
}

// ParseLOBFilePath extracts field ID and LOB file ID from a LOB file path.
// Path format: .../insert_log/{coll}/{part}/{seg}/{field}/lob/{lobfile}
// Returns (fieldID, lobFileID, ok) where ok indicates if parsing was successful.
func ParseLOBFilePath(filePath string) (fieldID int64, lobFileID uint64, ok bool) {
	parts := strings.Split(filePath, pathSep)
	if len(parts) < 3 {
		return 0, 0, false
	}

	for i, part := range parts {
		if part == "lob" && i > 0 && i+1 < len(parts) {
			fid, err := strconv.ParseInt(parts[i-1], 10, 64)
			if err != nil {
				return 0, 0, false
			}

			filename := parts[i+1]
			lfid, err := strconv.ParseUint(filename, 10, 64)
			if err != nil {
				return 0, 0, false
			}

			return fid, lfid, true
		}
	}

	return 0, 0, false
}

// ExtractLOBFileID extracts the LOB file ID from a file path.
// It assumes the last path segment is a numeric LOB file ID without extension.
func ExtractLOBFileID(filePath string) (uint64, error) {
	parts := strings.Split(filePath, pathSep)
	if len(parts) == 0 {
		return 0, strconv.ErrSyntax
	}
	filename := parts[len(parts)-1]
	return strconv.ParseUint(filename, 10, 64)
}

func ParseInsertLogPath(path string) (collectionID, partitionID, segmentID, fieldID, logID typeutil.UniqueID, ok bool) {
	infos := strings.Split(path, pathSep)
	l := len(infos)
	if l < 6 {
		ok = false
		return
	}
	var err error
	if collectionID, err = strconv.ParseInt(infos[l-5], 10, 64); err != nil {
		return 0, 0, 0, 0, 0, false
	}
	if partitionID, err = strconv.ParseInt(infos[l-4], 10, 64); err != nil {
		return 0, 0, 0, 0, 0, false
	}
	if segmentID, err = strconv.ParseInt(infos[l-3], 10, 64); err != nil {
		return 0, 0, 0, 0, 0, false
	}
	if fieldID, err = strconv.ParseInt(infos[l-2], 10, 64); err != nil {
		return 0, 0, 0, 0, 0, false
	}
	if logID, err = strconv.ParseInt(infos[l-1], 10, 64); err != nil {
		return 0, 0, 0, 0, 0, false
	}
	ok = true
	return
}

func GetSegmentIDFromInsertLogPath(logPath string) typeutil.UniqueID {
	return getSegmentIDFromPath(logPath, 3)
}

func BuildStatsLogPath(rootPath string, collectionID, partitionID, segmentID, fieldID, logID typeutil.UniqueID) string {
	k := JoinIDPath(collectionID, partitionID, segmentID, fieldID, logID)
	return path.Join(rootPath, common.SegmentStatslogPath, k)
}

func BuildBm25LogPath(rootPath string, collectionID, partitionID, segmentID, fieldID, logID typeutil.UniqueID) string {
	k := JoinIDPath(collectionID, partitionID, segmentID, fieldID, logID)
	return path.Join(rootPath, common.SegmentBm25LogPath, k)
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
	idStr := make([]string, 0, len(ids))
	for _, id := range ids {
		idStr = append(idStr, strconv.FormatInt(id, 10))
	}
	return path.Join(idStr...)
}

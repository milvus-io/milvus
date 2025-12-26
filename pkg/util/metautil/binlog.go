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
// Format: {root_path}/insert_log/{collection_id}/{partition_id}/lobs/{field_id}/{lob_id}
func BuildLOBLogPath(rootPath string, collectionID, partitionID, fieldID, lobID typeutil.UniqueID) string {
	k := JoinIDPath(collectionID, partitionID)
	return path.Join(rootPath, common.SegmentInsertLogPath, k, "lobs", strconv.FormatInt(fieldID, 10), strconv.FormatInt(lobID, 10))
}

// ParseLOBFilePath extracts field ID and LOB file ID from a LOB file path.
// Path format: {root}/insert_log/{coll}/{part}/lobs/{field}/{lob_id}
// Returns (fieldID, lobFileID, ok) where ok indicates if parsing was successful.
func ParseLOBFilePath(filePath string) (fieldID int64, lobFileID uint64, ok bool) {
	_, _, fieldID, lobFileID, ok = ParseLOBFilePathFull(filePath)
	return fieldID, lobFileID, ok
}

// ParseLOBFilePathFull extracts all IDs from a LOB file path.
// Path format: {root}/insert_log/{coll}/{part}/lobs/{field}/{lob_id}
// Returns (collectionID, partitionID, fieldID, lobFileID, ok).
func ParseLOBFilePathFull(filePath string) (collectionID, partitionID, fieldID int64, lobFileID uint64, ok bool) {
	parts := strings.Split(filePath, pathSep)

	insertLogIdx := -1
	for i, part := range parts {
		if part == common.SegmentInsertLogPath {
			insertLogIdx = i
			break
		}
	}

	if insertLogIdx < 0 {
		return 0, 0, 0, 0, false
	}

	// after insert_log, need at least 5 more parts: coll/part/lobs/field/lobfile
	remaining := parts[insertLogIdx+1:]
	if len(remaining) < 5 {
		return 0, 0, 0, 0, false
	}

	// verify "lobs" is at position 2 (after coll/part)
	if remaining[2] != "lobs" {
		return 0, 0, 0, 0, false
	}

	// parse collection ID (position 0)
	cid, err := strconv.ParseInt(remaining[0], 10, 64)
	if err != nil {
		return 0, 0, 0, 0, false
	}

	// parse partition ID (position 1)
	pid, err := strconv.ParseInt(remaining[1], 10, 64)
	if err != nil {
		return 0, 0, 0, 0, false
	}

	// parse field ID (position 3)
	fid, err := strconv.ParseInt(remaining[3], 10, 64)
	if err != nil {
		return 0, 0, 0, 0, false
	}

	// parse lob file ID (position 4)
	lfid, err := strconv.ParseUint(remaining[4], 10, 64)
	if err != nil {
		return 0, 0, 0, 0, false
	}

	return cid, pid, fid, lfid, true
}

// ExtractLOBFileID extracts the LOB file ID from a file path.
// Must prove that the path is a valid LOB file path.
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

package metautil

import (
	"path"
	"strconv"
	"strings"

	"github.com/milvus-io/milvus/pkg/v2/common"
	"github.com/milvus-io/milvus/pkg/v2/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v2/util/typeutil"
)

const pathSep = "/"

func BuildInsertLogPath(rootPath string, collectionID, partitionID, segmentID, fieldID, logID typeutil.UniqueID) string {
	k := JoinIDPath(collectionID, partitionID, segmentID, fieldID, logID)
	return path.Join(rootPath, common.SegmentInsertLogPath, k)
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

// It modifies the TextStatsLogs map in place, compressing full paths to filenames.
func ExtractTextLogFilenames(textStatsLogs map[int64]*datapb.TextIndexStats) {
	for _, textStats := range textStatsLogs {
		if textStats == nil {
			continue
		}
		filenames := make([]string, 0, len(textStats.GetFiles()))
		for _, fullPath := range textStats.GetFiles() {
			idx := strings.LastIndex(fullPath, pathSep)
			if idx < 0 {
				filenames = append(filenames, fullPath)
			} else {
				filenames = append(filenames, fullPath[idx+1:])
			}
		}
		textStats.Files = filenames
	}
}

// BuildTextLogPaths reconstructs full paths from filenames for text index logs.
// This function is compatible with both old version (full paths) and new version (filenames only).
func BuildTextLogPaths(rootPath string, collectionID, partitionID, segmentID typeutil.UniqueID, textStatsLogs map[int64]*datapb.TextIndexStats) {
	for _, textStats := range textStatsLogs {
		if textStats == nil {
			continue
		}
		prefix := path.Join(
			rootPath,
			common.TextIndexPath,
			strconv.FormatInt(textStats.GetBuildID(), 10),
			strconv.FormatInt(textStats.GetVersion(), 10),
			strconv.FormatInt(collectionID, 10),
			strconv.FormatInt(partitionID, 10),
			strconv.FormatInt(segmentID, 10),
			strconv.FormatInt(textStats.GetFieldID(), 10),
		)

		filenames := textStats.GetFiles()
		fullPaths := make([]string, 0, len(filenames))
		for _, filename := range filenames {
			// Check if filename is already a full path (compatible with old version)
			// If it contains the text_log path segment, treat it as a full path
			if strings.Contains(filename, common.TextIndexPath+pathSep) {
				fullPaths = append(fullPaths, filename)
			} else {
				// New version: filename only, need to join with prefix
				fullPaths = append(fullPaths, path.Join(prefix, filename))
			}
		}
		textStats.Files = fullPaths
	}
}

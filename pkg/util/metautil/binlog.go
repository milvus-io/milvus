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

// BuildDeltaLogPathV3 builds a deltalog path under the segment's basePath/_delta/ directory.
// This places the deltalog alongside the insert data instead of under a separate delta_log/ prefix.
// The resulting path is: {basePath}/_delta/{logID}
func BuildDeltaLogPathV3(basePath string, logID typeutil.UniqueID) string {
	return path.Join(basePath, "_delta", strconv.FormatInt(logID, 10))
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

// ExtractTextLogFilenames extracts only filenames from full paths to save space.
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

// BuildTextIndexPrefix returns the remote base path for text index files.
// Format: {rootPath}/text_log/{buildID}/{version}/{collID}/{partID}/{segID}/{fieldID}
func BuildTextIndexPrefix(rootPath string, buildID, version, collectionID, partitionID, segmentID, fieldID int64) string {
	return path.Join(rootPath, common.TextIndexPath,
		strconv.FormatInt(buildID, 10), strconv.FormatInt(version, 10),
		strconv.FormatInt(collectionID, 10), strconv.FormatInt(partitionID, 10),
		strconv.FormatInt(segmentID, 10), strconv.FormatInt(fieldID, 10))
}

// BuildJSONKeyStatsPrefix returns the remote base path for JSON key stats files.
// Format: {rootPath}/json_stats/{dataFormat}/{buildID}/{version}/{collID}/{partID}/{segID}/{fieldID}
func BuildJSONKeyStatsPrefix(rootPath string, dataFormat, buildID, version, collectionID, partitionID, segmentID, fieldID int64) string {
	return path.Join(rootPath, common.JSONStatsPath,
		strconv.FormatInt(dataFormat, 10),
		strconv.FormatInt(buildID, 10), strconv.FormatInt(version, 10),
		strconv.FormatInt(collectionID, 10), strconv.FormatInt(partitionID, 10),
		strconv.FormatInt(segmentID, 10), strconv.FormatInt(fieldID, 10))
}

// BuildTextLogPaths reconstructs full paths from filenames for text index logs.
// This function is compatible with both old version (full paths) and new version (filenames only).
func BuildTextLogPaths(rootPath string, collectionID, partitionID, segmentID typeutil.UniqueID, textStatsLogs map[int64]*datapb.TextIndexStats) {
	for _, textStats := range textStatsLogs {
		if textStats == nil {
			continue
		}
		prefix := BuildTextIndexPrefix(rootPath,
			textStats.GetBuildID(), textStats.GetVersion(),
			collectionID, partitionID, segmentID, textStats.GetFieldID())

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

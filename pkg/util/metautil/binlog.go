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

// ExtractTextLogFilenames extracts only filenames from full paths to save space.
// It takes a slice of full paths and returns a slice of filenames.
func ExtractTextLogFilenames(files []string) []string {
	filenames := make([]string, 0, len(files))
	for _, fullPath := range files {
		idx := strings.LastIndex(fullPath, pathSep)
		if idx < 0 {
			filenames = append(filenames, fullPath)
		} else {
			filenames = append(filenames, fullPath[idx+1:])
		}
	}
	return filenames
}

// BuildTextLogPaths reconstructs full paths from filenames for text index logs.
// Files stored in TextIndexStats only contain filenames to save space.
func BuildTextLogPaths(rootPath string, buildID, version, collectionID, partitionID, segmentID, fieldID typeutil.UniqueID, filenames []string) []string {
	prefix := path.Join(
		rootPath,
		common.TextIndexPath,
		strconv.FormatInt(buildID, 10),
		strconv.FormatInt(version, 10),
		strconv.FormatInt(collectionID, 10),
		strconv.FormatInt(partitionID, 10),
		strconv.FormatInt(segmentID, 10),
		strconv.FormatInt(fieldID, 10),
	)

	fullPaths := make([]string, 0, len(filenames))
	for _, filename := range filenames {
		fullPaths = append(fullPaths, path.Join(prefix, filename))
	}
	return fullPaths
}

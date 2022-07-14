package storage

import (
	"strconv"
	"strings"
)

func ParseSegmentIDByBinlog(path string) (UniqueID, error) {
	// binlog path should consist of "files/insertLog/collID/partID/segID/fieldID/fileName"
	keyStr := strings.Split(path, "/")
	return strconv.ParseInt(keyStr[len(keyStr)-3], 10, 64)
}

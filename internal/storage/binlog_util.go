package storage

import (
	"fmt"
	"strconv"
	"strings"
)

// ParseSegmentIDByBinlog parse segment id from binlog paths
// if path format is not expected, returns error
func ParseSegmentIDByBinlog(path string) (UniqueID, error) {
	// binlog path should consist of "[prefix]/insertLog/collID/partID/segID/fieldID/fileName"
	keyStr := strings.Split(path, "/")
	if len(keyStr) != 7 {
		return 0, fmt.Errorf("%s is not a valid binlog path", path)
	}
	return strconv.ParseInt(keyStr[len(keyStr)-3], 10, 64)
}

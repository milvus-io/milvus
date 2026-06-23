package tasks

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/milvus-io/milvus/internal/util/function/chain"
)

const groupByColumnPrefix = "$group_by_"

func groupByColumnName(fieldID int64) string {
	return fmt.Sprintf("%s%d", groupByColumnPrefix, fieldID)
}

func isGroupByColumnName(name string) bool {
	if !strings.HasPrefix(name, groupByColumnPrefix) {
		return false
	}
	fieldID := strings.TrimPrefix(name, groupByColumnPrefix)
	if fieldID == "" {
		return false
	}
	_, err := strconv.ParseInt(fieldID, 10, 64)
	return err == nil
}

func groupByColumnNames(df *chain.DataFrame) []string {
	if df == nil {
		return nil
	}
	names := make([]string, 0)
	for _, name := range df.ColumnNames() {
		if isGroupByColumnName(name) {
			names = append(names, name)
		}
	}
	return names
}

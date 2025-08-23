package message

import (
	"fmt"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
)

func GetWALName(name string) commonpb.WALName {
	walName, ok := commonpb.WALName_value[name]
	if !ok {
		panic(fmt.Sprintf("invalid wal name: %s", name))
	}
	return commonpb.WALName(walName)
}

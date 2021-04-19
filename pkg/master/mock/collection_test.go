package mock

import (
	"fmt"
	"testing"
	"time"
)

var s = FieldMeta{
	FieldName: "test-schema-1",
	Type:      1,
	DIM:       int64(512),
}

var C = Collection{
	ID:            uint64(11111),
	Name:          "test-collection",
	CreateTime:    uint64(time.Now().Unix()),
	SegmentIDs:    []uint64{uint64(10111)},
	Schema:        []FieldMeta{s},
	PartitionTags: []string{"default"},
}

func TestCollection2JSON(t *testing.T) {
	res, err := Collection2JSON(C)
	if err != nil {
		t.Error(err)
	}
	fmt.Println(res)
}

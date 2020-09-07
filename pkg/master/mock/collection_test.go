package mock

import (
	"fmt"
	"testing"
	"time"
)

var C = Collection{
	ID:            uint64(11111),
	Name:          "test-collection",
	CreateTime:    time.Now(),
	SegmentIDs:    []uint64{uint64(10111)},
	PartitionTags: []string{"default"},
}

func TestCollection2JSON(t *testing.T) {
	res, err := Collection2JSON(C)
	if err != nil {
		t.Error(err)
	}
	fmt.Println(res)
}

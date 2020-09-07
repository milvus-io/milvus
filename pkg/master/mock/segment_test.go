package mock

import (
	"fmt"
	"testing"
	"time"
)

func TestSegmentMarshal(t *testing.T) {
	s := SegmentStats{
		SegementID: uint64(12315),
		MemorySize: uint64(233113),
		MemoryRate: float64(0.13),
	}

	data, err := SegmentMarshal(s)
	if err != nil {
		t.Error(err)
	}

	ss, err := SegmentUnMarshal(data)
	if err != nil {
		t.Error(err)
	}
	if ss.MemoryRate != s.MemoryRate {
		fmt.Println(ss.MemoryRate)
		fmt.Println(s.MemoryRate)
		t.Error("Error when marshal")
	}
}

var Ts = Segment{
	SegmentID: uint64(101111),
	Collection: Collection{
		ID:            uint64(11111),
		Name:          "test-collection",
		CreateTime:    time.Now(),
		SegmentIDs:    []uint64{uint64(10111)},
		PartitionTags: []string{"default"},
	},
	PartitionTag:   "default",
	ChannelStart:   1,
	ChannelEnd:     100,
	OpenTimeStamp:  time.Now(),
	CloseTimeStamp: time.Now().Add(1 * time.Hour),
}

func TestSegment2JSON(t *testing.T) {
	res, err := Segment2JSON(Ts)
	if err != nil {
		t.Error(err)
	}
	fmt.Println(res)
}

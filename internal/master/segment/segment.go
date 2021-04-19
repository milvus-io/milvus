package segment

import (
	"time"

	masterpb "github.com/zilliztech/milvus-distributed/internal/proto/master"
	jsoniter "github.com/json-iterator/go"
)

var json = jsoniter.ConfigCompatibleWithStandardLibrary

type Segment struct {
	SegmentID      uint64                 `json:"segment_id"`
	CollectionID   uint64                 `json:"collection_id"`
	PartitionTag   string                 `json:"partition_tag"`
	ChannelStart   int                    `json:"channel_start"`
	ChannelEnd     int                    `json:"channel_end"`
	OpenTimeStamp  uint64                 `json:"open_timestamp"`
	CloseTimeStamp uint64                 `json:"close_timestamp"`
	CollectionName string                 `json:"collection_name"`
	Status         masterpb.SegmentStatus `json:"segment_status"`
	Rows           int64                  `json:"rows"`
}

func NewSegment(id uint64, collectioID uint64, cName string, ptag string, chStart int, chEnd int, openTime time.Time, closeTime time.Time) Segment {
	return Segment{
		SegmentID:      id,
		CollectionID:   collectioID,
		CollectionName: cName,
		PartitionTag:   ptag,
		ChannelStart:   chStart,
		ChannelEnd:     chEnd,
		OpenTimeStamp:  uint64(openTime.Unix()),
		CloseTimeStamp: uint64(closeTime.Unix()),
	}
}

func Segment2JSON(s Segment) (string, error) {
	b, err := json.Marshal(&s)
	if err != nil {
		return "", err
	}

	return string(b), nil
}

func JSON2Segment(s string) (*Segment, error) {
	var c Segment
	err := json.Unmarshal([]byte(s), &c)
	if err != nil {
		return &Segment{}, err
	}
	return &c, nil
}

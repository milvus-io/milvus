package segment

import (
	"time"

	"github.com/zilliztech/milvus-distributed/internal/util/typeutil"

	jsoniter "github.com/json-iterator/go"
)

type UniqueID = typeutil.UniqueID
type Timestamp = typeutil.Timestamp

var json = jsoniter.ConfigCompatibleWithStandardLibrary

type Segment struct {
	SegmentID      UniqueID  `json:"segment_id"`
	CollectionID   UniqueID  `json:"collection_id"`
	PartitionTag   string    `json:"partition_tag"`
	ChannelStart   int       `json:"channel_start"`
	ChannelEnd     int       `json:"channel_end"`
	OpenTimeStamp  Timestamp `json:"open_timestamp"`
	CloseTimeStamp Timestamp `json:"close_timestamp"`
	CollectionName string    `json:"collection_name"`
	Rows           int64     `json:"rows"`
}

func NewSegment(id UniqueID, collectioID UniqueID, cName string, ptag string, chStart int, chEnd int, openTime time.Time, closeTime time.Time) Segment {
	return Segment{
		SegmentID:      id,
		CollectionID:   collectioID,
		CollectionName: cName,
		PartitionTag:   ptag,
		ChannelStart:   chStart,
		ChannelEnd:     chEnd,
		OpenTimeStamp:  Timestamp(openTime.Unix()),
		CloseTimeStamp: Timestamp(closeTime.Unix()),
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

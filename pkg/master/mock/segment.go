package mock

import (
	"bytes"
	"encoding/gob"
	"time"
)

type SegmentStats struct {
	SegementID uint64
	MemorySize uint64
	MemoryRate float64
}

func SegmentMarshal(s SegmentStats) ([]byte, error) {
	var nb bytes.Buffer
	enc := gob.NewEncoder(&nb)
	err := enc.Encode(s)
	if err != nil {
		return []byte{}, err
	}
	return nb.Bytes(), nil
}

func SegmentUnMarshal(data []byte) (SegmentStats, error) {
	var ss SegmentStats
	dec := gob.NewDecoder(bytes.NewBuffer(data))
	err := dec.Decode(&ss)
	if err != nil {
		return SegmentStats{}, err
	}
	return ss, nil
}

type Segment struct {
	SegmentID      uint64     `json:"segment_id"`
	Collection     Collection `json:"collection"`
	PartitionTag   string     `json:"partition_tag"`
	ChannelStart   int        `json:"channel_start"`
	ChannelEnd     int        `json:"channel_end"`
	OpenTimeStamp  time.Time  `json:"open_timestamp"`
	CloseTimeStamp time.Time  `json:"clost_timestamp"`
}

func Segment2JSON(s Segment) (string, error) {
	b, err := json.Marshal(&s)
	if err != nil {
		return "", err
	}

	return string(b), nil
}

func JSON2Segment(s string) (Segment, error) {
	var c Segment
	err := json.Unmarshal([]byte(s), &c)
	if err != nil {
		return Segment{}, err
	}
	return c, nil
}

func FakeCreateSegment(id uint64, cl Collection, opentime time.Time, closetime time.Time) Segment {
	seg := Segment{
		SegmentID:      id,
		Collection:     cl,
		PartitionTag:   "default",
		ChannelStart:   0,
		ChannelEnd:     100,
		OpenTimeStamp:  opentime,
		CloseTimeStamp: closetime,
	}
	return seg
}

package mock

import (
	"time"

	jsoniter "github.com/json-iterator/go"
)

var json = jsoniter.ConfigCompatibleWithStandardLibrary

type Collection struct {
	ID            uint64    `json:"id"`
	Name          string    `json:"name"`
	CreateTime    time.Time `json:"creat_time"`
	SegmentIDs    []uint64  `json:"segment_ids"`
	PartitionTags []string  `json:"partition_tags"`
}

func FakeCreateCollection(id uint64) Collection {
	cl := Collection{
		ID:            id,
		Name:          "test-collection",
		CreateTime:    time.Now(),
		SegmentIDs:    []uint64{uint64(10111)},
		PartitionTags: []string{"default"},
	}
	return cl
}

func Collection2JSON(c Collection) (string, error) {
	b, err := json.Marshal(&c)
	if err != nil {
		return "", err
	}

	return string(b), nil
}

func JSON2Collection(s string) (Collection, error) {
	var c Collection
	err := json.Unmarshal([]byte(s), &c)
	if err != nil {
		return Collection{}, err
	}
	return c, nil
}

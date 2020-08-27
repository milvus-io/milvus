package mock

import (
	"context"
)

type Key = []byte
type Value = []byte
type Timestamp = uint64
type DriverType string

type TikvStore struct {
	kvMap      map[string][]byte
	segmentMap map[string]string
}

func NewTikvStore() (*TikvStore, error) {
	var map1 map[string][]byte
	map1 = make(map[string][]byte)
	var segment map[string]string
	segment = make(map[string]string)
	return &TikvStore{
		kvMap:      map1,
		segmentMap: segment,
	}, nil
}

func (s *TikvStore) PutRows(ctx context.Context, keys [][]byte, values [][]byte, segment string, timestamp []Timestamp) error {
	var i int
	for i = 0; i < len(keys); i++ {
		s.kvMap[string(keys[i])] = values[i]
		s.segmentMap[string(keys[i])] = segment
	}
	return nil
}

func (s *TikvStore) DeleteRows(ctx context.Context, keys [][]byte, timestamps []Timestamp) error {
	var i int
	for i = 0; i < len(keys); i++ {
		delete(s.kvMap, string(keys[i]))
		delete(s.segmentMap, string(keys[i]))
	}
	return nil
}

func (s *TikvStore) GetSegment(ctx context.Context, keys [][]byte) []string {
	var segmentId []string
	var i int
	for i = 0; i < len(keys); i++ {
		segmentId = append(segmentId, s.segmentMap[string(keys[i])])
	}
	return segmentId
}

func (s *TikvStore) GetData(ctx context.Context) map[string][]byte {
	return s.kvMap
}

func DeliverSegmentIds(keys [][]byte, segmentIds []string) {

}

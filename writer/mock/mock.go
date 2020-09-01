package mock

import (
	"context"
	"strconv"
	"strings"
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

func (s *TikvStore) PutRows(ctx context.Context, prefixKeys [][]byte, timeStamp []Timestamp, suffixKeys [][]byte, values [][]byte) error {
	var i int
	for i = 0; i < len(prefixKeys); i++ {
		keys := string(prefixKeys[i]) + "_" + string(suffixKeys[i]) + "_" + strconv.FormatUint(timeStamp[i], 10)
		s.kvMap[keys] = values[i]
		s.segmentMap[string(prefixKeys[i])] = string(suffixKeys[i])
	}
	return nil
}

func (s *TikvStore) DeleteRows(ctx context.Context, keys [][]byte, timestamps []Timestamp) error {
	var i int
	for i = 0; i < len(keys); i++ {
		for k, _ := range s.kvMap {
			if strings.Index(k, string(keys[i])) != -1 {
				delete(s.kvMap, k)
			}
		}
		delete(s.segmentMap, string(keys[i]))
	}
	return nil
}

func (s *TikvStore) GetSegment(ctx context.Context, keys [][]byte) *[]string {
	var segmentId []string
	var i int
	for i = 0; i < len(keys); i++ {
		segmentId = append(segmentId, s.segmentMap[string(keys[i])])
	}
	return &segmentId
}

func (s *TikvStore) GetData(ctx context.Context) map[string][]byte {
	return s.kvMap
}

func DeliverSegmentIds(keys [][]byte, segmentIds []string) {

}

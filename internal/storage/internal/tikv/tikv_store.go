package tikv_driver

import (
	"context"
	"errors"
	"strconv"
	"strings"

	"github.com/tikv/client-go/config"
	"github.com/tikv/client-go/rawkv"
	"github.com/zilliztech/milvus-distributed/internal/conf"
	. "github.com/zilliztech/milvus-distributed/internal/storage/internal/tikv/codec"
	. "github.com/zilliztech/milvus-distributed/internal/storage/type"
)

func keyAddOne(key Key) Key {
	if key == nil {
		return nil
	}
	lenKey := len(key)
	ret := make(Key, lenKey)
	copy(ret, key)
	ret[lenKey-1] += 0x01
	return ret
}

type tikvEngine struct {
	client *rawkv.Client
	conf   config.Config
}

func (e tikvEngine) Put(ctx context.Context, key Key, value Value) error {
	return e.client.Put(ctx, key, value)
}

func (e tikvEngine) BatchPut(ctx context.Context, keys []Key, values []Value) error {
	return e.client.BatchPut(ctx, keys, values)
}

func (e tikvEngine) Get(ctx context.Context, key Key) (Value, error) {
	return e.client.Get(ctx, key)
}

func (e tikvEngine) GetByPrefix(ctx context.Context, prefix Key, keyOnly bool) (keys []Key, values []Value, err error) {
	startKey := prefix
	endKey := keyAddOne(prefix)
	limit := e.conf.Raw.MaxScanLimit
	for {
		ks, vs, err := e.Scan(ctx, startKey, endKey, limit, keyOnly)
		if err != nil {
			return keys, values, err
		}
		keys = append(keys, ks...)
		values = append(values, vs...)
		if len(ks) < limit {
			break
		}
		// update the start key, and exclude the start key
		startKey = append(ks[len(ks)-1], '\000')
	}
	return
}

func (e tikvEngine) Scan(ctx context.Context, startKey Key, endKey Key, limit int, keyOnly bool) ([]Key, []Value, error) {
	return e.client.Scan(ctx, startKey, endKey, limit, rawkv.ScanOption{KeyOnly: keyOnly})
}

func (e tikvEngine) Delete(ctx context.Context, key Key) error {
	return e.client.Delete(ctx, key)
}

func (e tikvEngine) DeleteByPrefix(ctx context.Context, prefix Key) error {
	startKey := prefix
	endKey := keyAddOne(prefix)
	return e.client.DeleteRange(ctx, startKey, endKey)
}

func (e tikvEngine) DeleteRange(ctx context.Context, startKey Key, endKey Key) error {
	return e.client.DeleteRange(ctx, startKey, endKey)
}

func (e tikvEngine) Close() error {
	return e.client.Close()
}

type TikvStore struct {
	engine *tikvEngine
}

func NewTikvStore(ctx context.Context) (*TikvStore, error) {
	conf.LoadConfig("config.yaml")
	var pdAddress0 = conf.Config.Storage.Address + ":" + strconv.FormatInt(int64(conf.Config.Storage.Port), 10)
	pdAddrs := []string{pdAddress0}
	conf := config.Default()
	client, err := rawkv.NewClient(ctx, pdAddrs, conf)
	if err != nil {
		return nil, err
	}
	return &TikvStore{
		&tikvEngine{
			client: client,
			conf:   conf,
		},
	}, nil
}

func (s *TikvStore) Name() string {
	return "TiKV storage"
}

func (s *TikvStore) put(ctx context.Context, key Key, value Value, timestamp Timestamp, suffix string) error {
	return s.engine.Put(ctx, EncodeKey(key, timestamp, suffix), value)
}

func (s *TikvStore) scanLE(ctx context.Context, key Key, timestamp Timestamp, keyOnly bool) ([]Timestamp, []Key, []Value, error) {
	panic("implement me")
}

func (s *TikvStore) scanGE(ctx context.Context, key Key, timestamp Timestamp, keyOnly bool) ([]Timestamp, []Key, []Value, error) {
	panic("implement me")
}

func (s *TikvStore) scan(ctx context.Context, key Key, start Timestamp, end Timestamp, keyOnly bool) ([]Timestamp, []Key, []Value, error) {
	//startKey := EncodeKey(key, start, "")
	//endKey := EncodeKey(EncodeDelimiter(key, DelimiterPlusOne), end, "")
	//return s.engine.Scan(ctx, startKey, endKey, -1, keyOnly)
	panic("implement me")
}

func (s *TikvStore) deleteLE(ctx context.Context, key Key, timestamp Timestamp) error {
	panic("implement me")
}

func (s *TikvStore) deleteGE(ctx context.Context, key Key, timestamp Timestamp) error {
	panic("implement me")
}

func (s *TikvStore) deleteRange(ctx context.Context, key Key, start Timestamp, end Timestamp) error {
	panic("implement me")
}

func (s *TikvStore) GetRow(ctx context.Context, key Key, timestamp Timestamp) (Value, error) {
	startKey := EncodeKey(key, timestamp, "")
	endKey := EncodeDelimiter(key, DelimiterPlusOne)
	keys, values, err := s.engine.Scan(ctx, startKey, endKey, 1, false)
	if err != nil || keys == nil {
		return nil, err
	}
	_, _, suffix, err := DecodeKey(keys[0])
	if err != nil {
		return nil, err
	}
	// key is marked deleted
	if suffix == string(DeleteMark) {
		return nil, nil
	}
	return values[0], nil
}

// TODO: how to spilt keys to some batches
var batchSize = 100

type kvPair struct {
	key   Key
	value Value
	err   error
}

func batchKeys(keys []Key) [][]Key {
	keysLen := len(keys)
	numBatch := (keysLen-1)/batchSize + 1
	batches := make([][]Key, numBatch)

	for i := 0; i < numBatch; i++ {
		batchStart := i * batchSize
		batchEnd := batchStart + batchSize
		// the last batch
		if i == numBatch-1 {
			batchEnd = keysLen
		}
		batches[i] = keys[batchStart:batchEnd]
	}
	return batches
}

func (s *TikvStore) GetRows(ctx context.Context, keys []Key, timestamps []Timestamp) ([]Value, error) {
	if len(keys) != len(timestamps) {
		return nil, errors.New("the len of keys is not equal to the len of timestamps")
	}

	batches := batchKeys(keys)
	ch := make(chan kvPair, len(keys))
	ctx, cancel := context.WithCancel(ctx)
	for n, b := range batches {
		batch := b
		numBatch := n
		go func() {
			for i, key := range batch {
				select {
				case <-ctx.Done():
					return
				default:
					v, err := s.GetRow(ctx, key, timestamps[numBatch*batchSize+i])
					ch <- kvPair{
						key:   key,
						value: v,
						err:   err,
					}
				}
			}
		}()
	}

	var err error
	var values []Value
	kvMap := make(map[string]Value)
	for i := 0; i < len(keys); i++ {
		kv := <-ch
		if kv.err != nil {
			cancel()
			if err == nil {
				err = kv.err
			}
		}
		kvMap[string(kv.key)] = kv.value
	}
	for _, key := range keys {
		values = append(values, kvMap[string(key)])
	}
	return values, err
}

func (s *TikvStore) PutRow(ctx context.Context, key Key, value Value, segment string, timestamp Timestamp) error {
	return s.put(ctx, key, value, timestamp, segment)
}

func (s *TikvStore) PutRows(ctx context.Context, keys []Key, values []Value, segments []string, timestamps []Timestamp) error {
	if len(keys) != len(values) {
		return errors.New("the len of keys is not equal to the len of values")
	}
	if len(keys) != len(timestamps) {
		return errors.New("the len of keys is not equal to the len of timestamps")
	}

	encodedKeys := make([]Key, len(keys))
	for i, key := range keys {
		encodedKeys[i] = EncodeKey(key, timestamps[i], segments[i])
	}
	return s.engine.BatchPut(ctx, encodedKeys, values)
}

func (s *TikvStore) DeleteRow(ctx context.Context, key Key, timestamp Timestamp) error {
	return s.put(ctx, key, Value{0x00}, timestamp, string(DeleteMark))
}

func (s *TikvStore) DeleteRows(ctx context.Context, keys []Key, timestamps []Timestamp) error {
	encodeKeys := make([]Key, len(keys))
	values := make([]Value, len(keys))
	for i, key := range keys {
		encodeKeys[i] = EncodeKey(key, timestamps[i], string(DeleteMark))
		values[i] = Value{0x00}
	}
	return s.engine.BatchPut(ctx, encodeKeys, values)
}

//func (s *TikvStore) DeleteRows(ctx context.Context, keys []Key, timestamp Timestamp) error {
//	batches := batchKeys(keys)
//	ch := make(chan error, len(batches))
//	ctx, cancel := context.WithCancel(ctx)
//
//	for _, b := range batches {
//		batch := b
//		go func() {
//			for _, key := range batch {
//				select {
//				case <-ctx.Done():
//					return
//				default:
//					ch <- s.DeleteRow(ctx, key, timestamp)
//				}
//			}
//		}()
//	}
//
//	var err error
//	for i := 0; i < len(keys); i++ {
//		if e := <-ch; e != nil {
//			cancel()
//			if err == nil {
//				err = e
//			}
//		}
//	}
//	return err
//}

func (s *TikvStore) PutLog(ctx context.Context, key Key, value Value, timestamp Timestamp, channel int) error {
	suffix := string(EncodeDelimiter(key, DelimiterPlusOne)) + strconv.Itoa(channel)
	return s.put(ctx, Key("log"), value, timestamp, suffix)
}

func (s *TikvStore) GetLog(ctx context.Context, start Timestamp, end Timestamp, channels []int) (logs []Value, err error) {
	key := Key("log")
	startKey := EncodeKey(key, end, "")
	endKey := EncodeKey(key, start, "")
	// TODO: use for loop to ensure get all keys
	keys, values, err := s.engine.Scan(ctx, startKey, endKey, s.engine.conf.Raw.MaxScanLimit, false)
	if err != nil || keys == nil {
		return nil, err
	}

	for i, key := range keys {
		_, _, suffix, err := DecodeKey(key)
		log := values[i]
		if err != nil {
			return logs, err
		}

		// no channels filter
		if len(channels) == 0 {
			logs = append(logs, log)
		}
		slice := strings.Split(suffix, string(DelimiterPlusOne))
		channel, err := strconv.Atoi(slice[len(slice)-1])
		for _, item := range channels {
			if item == channel {
				logs = append(logs, log)
				break
			}
		}
	}
	return
}

func (s *TikvStore) GetSegmentIndex(ctx context.Context, segment string) (SegmentIndex, error) {
	return s.engine.Get(ctx, EncodeSegment([]byte(segment), SegmentIndexMark))
}

func (s *TikvStore) PutSegmentIndex(ctx context.Context, segment string, index SegmentIndex) error {
	return s.engine.Put(ctx, EncodeSegment([]byte(segment), SegmentIndexMark), index)
}

func (s *TikvStore) DeleteSegmentIndex(ctx context.Context, segment string) error {
	return s.engine.Delete(ctx, EncodeSegment([]byte(segment), SegmentIndexMark))
}

func (s *TikvStore) GetSegmentDL(ctx context.Context, segment string) (SegmentDL, error) {
	return s.engine.Get(ctx, EncodeSegment([]byte(segment), SegmentDLMark))
}

func (s *TikvStore) PutSegmentDL(ctx context.Context, segment string, log SegmentDL) error {
	return s.engine.Put(ctx, EncodeSegment([]byte(segment), SegmentDLMark), log)
}

func (s *TikvStore) DeleteSegmentDL(ctx context.Context, segment string) error {
	return s.engine.Delete(ctx, EncodeSegment([]byte(segment), SegmentDLMark))
}

func (s *TikvStore) GetSegments(ctx context.Context, key Key, timestamp Timestamp) ([]string, error) {
	keys, _, err := s.engine.GetByPrefix(ctx, EncodeDelimiter(key, Delimiter), true)
	if err != nil {
		return nil, err
	}
	segmentsSet := map[string]bool{}
	for _, key := range keys {
		_, ts, segment, err := DecodeKey(key)
		if err != nil {
			panic("must no error")
		}
		if ts <= timestamp && segment != string(DeleteMark) {
			segmentsSet[segment] = true
		}
	}

	var segments []string
	for k, v := range segmentsSet {
		if v == true {
			segments = append(segments, k)
		}
	}
	return segments, err
}

func (s *TikvStore) Close() error {
	return s.engine.Close()
}

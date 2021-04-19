package storagetype

import (
	"context"
)

type Key = []byte
type Value = []byte
type Timestamp = uint64
type DriverType = string
type SegmentIndex = []byte
type SegmentDL = []byte

const (
	MinIODriver DriverType = "MinIO"
	TIKVDriver  DriverType = "TIKV"
	S3DRIVER    DriverType = "S3"
)

/*
type Store interface {
	Get(ctx context.Context, key Key, timestamp Timestamp) (Value, error)
	BatchGet(ctx context.Context, keys [] Key, timestamp Timestamp) ([]Value, error)
	Set(ctx context.Context, key Key, v Value, timestamp Timestamp) error
	BatchSet(ctx context.Context, keys []Key, v []Value, timestamp Timestamp) error
	Delete(ctx context.Context, key Key, timestamp Timestamp) error
	BatchDelete(ctx context.Context, keys []Key, timestamp Timestamp) error
	Close() error
}
*/

type storeEngine interface {
	Put(ctx context.Context, key Key, value Value) error
	Get(ctx context.Context, key Key) (Value, error)
	GetByPrefix(ctx context.Context, prefix Key, keyOnly bool) ([]Key, []Value, error)
	Scan(ctx context.Context, startKey Key, endKey Key, limit int, keyOnly bool) ([]Key, []Value, error)
	Delete(ctx context.Context, key Key) error
	DeleteByPrefix(ctx context.Context, prefix Key) error
	DeleteRange(ctx context.Context, keyStart Key, keyEnd Key) error
}

type Store interface {
	//put(ctx context.Context, key Key, value Value, timestamp Timestamp, suffix string) error
	//scanLE(ctx context.Context, key Key, timestamp Timestamp, keyOnly bool) ([]Timestamp, []Key, []Value, error)
	//scanGE(ctx context.Context, key Key, timestamp Timestamp, keyOnly bool) ([]Timestamp, []Key, []Value, error)
	//deleteLE(ctx context.Context, key Key, timestamp Timestamp) error
	//deleteGE(ctx context.Context, key Key, timestamp Timestamp) error
	//deleteRange(ctx context.Context, key Key, start Timestamp, end Timestamp) error

	GetRow(ctx context.Context, key Key, timestamp Timestamp) (Value, error)
	GetRows(ctx context.Context, keys []Key, timestamps []Timestamp) ([]Value, error)

	PutRow(ctx context.Context, key Key, value Value, segment string, timestamp Timestamp) error
	PutRows(ctx context.Context, keys []Key, values []Value, segments []string, timestamps []Timestamp) error

	GetSegments(ctx context.Context, key Key, timestamp Timestamp) ([]string, error)

	DeleteRow(ctx context.Context, key Key, timestamp Timestamp) error
	DeleteRows(ctx context.Context, keys []Key, timestamps []Timestamp) error

	PutLog(ctx context.Context, key Key, value Value, timestamp Timestamp, channel int) error
	GetLog(ctx context.Context, start Timestamp, end Timestamp, channels []int) ([]Value, error)

	GetSegmentIndex(ctx context.Context, segment string) (SegmentIndex, error)
	PutSegmentIndex(ctx context.Context, segment string, index SegmentIndex) error
	DeleteSegmentIndex(ctx context.Context, segment string) error

	GetSegmentDL(ctx context.Context, segment string) (SegmentDL, error)
	PutSegmentDL(ctx context.Context, segment string, log SegmentDL) error
	DeleteSegmentDL(ctx context.Context, segment string) error
}

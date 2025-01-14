package delegator

import (
	"fmt"
	"time"

	"github.com/cockroachdb/errors"

	"github.com/milvus-io/milvus/pkg/proto/querypb"
	"github.com/milvus-io/milvus/pkg/util/typeutil"
)

const (
	rowIDFieldID     FieldID = 0
	timestampFieldID FieldID = 1
)

type (
	// UniqueID is an identifier that is guaranteed to be unique among all the collections, partitions and segments
	UniqueID = typeutil.UniqueID
	// Timestamp is timestamp
	Timestamp = typeutil.Timestamp
	// FieldID is to uniquely identify the field
	FieldID = int64
	// IntPrimaryKey is the primary key of int type
	IntPrimaryKey = typeutil.IntPrimaryKey
	// DSL is the Domain Specific Language
	DSL = string
	// ConsumeSubName is consumer's subscription name of the message stream
	ConsumeSubName = string
)

// TimeRange is a range of time periods
type TimeRange struct {
	timestampMin Timestamp
	timestampMax Timestamp
}

// loadType is load collection or load partition
type loadType = querypb.LoadType

const (
	loadTypeCollection = querypb.LoadType_LoadCollection
	loadTypePartition  = querypb.LoadType_LoadPartition
)

// TSafeUpdater is the interface for type provides tsafe update event
type TSafeUpdater interface {
	RegisterChannel(string) chan Timestamp
	UnregisterChannel(string) error
}

// ErrTsLagTooLarge serviceable and guarantee lag too large.
var ErrTsLagTooLarge = errors.New("Timestamp lag too large")

// WrapErrTsLagTooLarge wraps ErrTsLagTooLarge with lag and max value.
func WrapErrTsLagTooLarge(duration time.Duration, maxLag time.Duration) error {
	return fmt.Errorf("%w lag(%s) max(%s)", ErrTsLagTooLarge, duration, maxLag)
}

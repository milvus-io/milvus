package tso

import (
	"log"
	"sync/atomic"
	"time"

	"github.com/zilliztech/milvus-distributed/internal/errors"
	"github.com/zilliztech/milvus-distributed/internal/util/kvutil"
	"github.com/zilliztech/milvus-distributed/internal/util/tsoutil"
	"github.com/zilliztech/milvus-distributed/internal/util/typeutil"
	"go.uber.org/zap"
)

// Allocator is a Timestamp Oracle allocator.
type Allocator interface {
	// Initialize is used to initialize a TSO allocator.
	// It will synchronize TSO with etcd and initialize the
	// memory for later allocation work.
	Initialize() error
	// UpdateTSO is used to update the TSO in memory and the time window in etcd.
	UpdateTSO() error
	// SetTSO sets the physical part with given tso. It's mainly used for BR restore
	// and can not forcibly set the TSO smaller than now.
	SetTSO(tso uint64) error
	// GenerateTSO is used to generate a given number of TSOs.
	// Make sure you have initialized the TSO allocator before calling.
	GenerateTSO(count uint32) (uint64, error)
	// Reset is used to reset the TSO allocator.
	Reset()
}

// GlobalTSOAllocator is the global single point TSO allocator.
type GlobalTSOAllocator struct {
	tso *timestampOracle
}

var allocator *GlobalTSOAllocator

func Init(etcdAddr []string, rootPath string) {
	InitGlobalTsoAllocator("timestamp", tsoutil.NewTSOKVBase(etcdAddr, rootPath, "tso"))
}

func InitGlobalTsoAllocator(key string, base kvutil.Base) {
	allocator = NewGlobalTSOAllocator(key, base)
	allocator.Initialize()
}

// NewGlobalTSOAllocator creates a new global TSO allocator.
func NewGlobalTSOAllocator(key string, kvBase kvutil.Base) *GlobalTSOAllocator {
	var saveInterval = 3 * time.Second
	return &GlobalTSOAllocator{
		tso: &timestampOracle{
			kvBase:        kvBase,
			saveInterval:  saveInterval,
			maxResetTSGap: func() time.Duration { return 3 * time.Second },
			key:           key,
		},
	}
}

// Initialize will initialize the created global TSO allocator.
func (gta *GlobalTSOAllocator) Initialize() error {
	return gta.tso.InitTimestamp()
}

// UpdateTSO is used to update the TSO in memory and the time window in etcd.
func (gta *GlobalTSOAllocator) UpdateTSO() error {
	return gta.tso.UpdateTimestamp()
}

// SetTSO sets the physical part with given tso.
func (gta *GlobalTSOAllocator) SetTSO(tso uint64) error {
	return gta.tso.ResetUserTimestamp(tso)
}

// GenerateTSO is used to generate a given number of TSOs.
// Make sure you have initialized the TSO allocator before calling.
func (gta *GlobalTSOAllocator) GenerateTSO(count uint32) (uint64, error) {
	var physical, logical int64
	if count == 0 {
		return 0, errors.New("tso count should be positive")
	}

	maxRetryCount := 10

	for i := 0; i < maxRetryCount; i++ {
		current := (*atomicObject)(atomic.LoadPointer(&gta.tso.TSO))
		if current == nil || current.physical.Equal(typeutil.ZeroTime) {
			// If it's leader, maybe SyncTimestamp hasn't completed yet
			log.Println("sync hasn't completed yet, wait for a while")
			time.Sleep(200 * time.Millisecond)
			continue
		}

		physical = current.physical.UnixNano() / int64(time.Millisecond)
		logical = atomic.AddInt64(&current.logical, int64(count))
		if logical >= maxLogical {
			log.Println("logical part outside of max logical interval, please check ntp time",
				zap.Int("retry-count", i))
			time.Sleep(UpdateTimestampStep)
			continue
		}
		return tsoutil.ComposeTS(physical, logical), nil
	}
	return 0, errors.New("can not get timestamp")
}

func (gta *GlobalTSOAllocator) Alloc(count uint32) (typeutil.Timestamp, error) {
	//return gta.tso.SyncTimestamp()
	start, err := gta.GenerateTSO(count)
	if err != nil {
		return typeutil.ZeroTimestamp, err
	}
	//ret := make([]typeutil.Timestamp, count)
	//for i:=uint32(0); i < count; i++{
	//	ret[i] = start + uint64(i)
	//}
	return start, err
}

func (gta *GlobalTSOAllocator) AllocOne() (typeutil.Timestamp, error) {
	return gta.GenerateTSO(1)
}

// Reset is used to reset the TSO allocator.
func (gta *GlobalTSOAllocator) Reset() {
	gta.tso.ResetTimestamp()
}

func AllocOne() (typeutil.Timestamp, error) {
	return allocator.AllocOne()
}

// Reset is used to reset the TSO allocator.
func Alloc(count uint32) (typeutil.Timestamp, error) {
	return allocator.Alloc(count)
}

func UpdateTSO() error {
	return allocator.UpdateTSO()
}

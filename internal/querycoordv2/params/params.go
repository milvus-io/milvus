package params

import (
	"errors"
	"math/rand"
	"strconv"
	"sync/atomic"
	"time"

	"github.com/milvus-io/milvus/internal/util/paramtable"
)

var Params paramtable.ComponentParam

var (
	ErrFailedAllocateID = errors.New("failed to allocate ID")
)

// GenerateEtcdConfig returns a etcd config with a random root path,
// NOTE: for test only
func GenerateEtcdConfig() paramtable.EtcdConfig {
	config := Params.EtcdCfg
	rand.Seed(time.Now().UnixNano())
	suffix := "-test-querycoord" + strconv.FormatInt(rand.Int63(), 10)
	config.MetaRootPath = config.MetaRootPath + suffix
	return config
}

func RandomMetaRootPath() string {
	return "test-query-coord-" + strconv.FormatInt(rand.Int63(), 10)
}

func RandomIncrementIDAllocator() func() (int64, error) {
	id := rand.Int63() / 2
	return func() (int64, error) {
		return atomic.AddInt64(&id, 1), nil
	}
}

func ErrorIDAllocator() func() (int64, error) {
	return func() (int64, error) {
		return 0, ErrFailedAllocateID
	}
}

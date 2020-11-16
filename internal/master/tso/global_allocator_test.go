package tso

import (
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	gparams "github.com/zilliztech/milvus-distributed/internal/util/paramtableutil"
	"github.com/zilliztech/milvus-distributed/internal/util/tsoutil"
)

var GTsoAllocator Allocator

func TestMain(m *testing.M) {
	err := gparams.GParams.LoadYaml("config.yaml")
	if err != nil {
		panic(err)
	}
	GTsoAllocator = NewGlobalTSOAllocator("timestamp", tsoutil.NewTSOKVBase("tso"))

	exitCode := m.Run()
	os.Exit(exitCode)
}

func TestGlobalTSOAllocator_Initialize(t *testing.T) {
	err := GTsoAllocator.Initialize()
	assert.Nil(t, err)
}

func TestGlobalTSOAllocator_GenerateTSO(t *testing.T) {
	count := 1000
	perCount := uint32(100)
	startTs, err := GTsoAllocator.GenerateTSO(perCount)
	assert.Nil(t, err)
	lastPhysical, lastLogical := tsoutil.ParseTS(startTs)
	for i := 0; i < count; i++ {
		ts, _ := GTsoAllocator.GenerateTSO(perCount)
		physical, logical := tsoutil.ParseTS(ts)
		if lastPhysical.Equal(physical) {
			diff := logical - lastLogical
			assert.Equal(t, uint64(perCount), diff)
		}
		lastPhysical, lastLogical = physical, logical
	}
}

func TestGlobalTSOAllocator_SetTSO(t *testing.T) {
	curTime := time.Now()
	nextTime := curTime.Add(2 * time.Second)
	physical := nextTime.UnixNano() / int64(time.Millisecond)
	logical := int64(0)
	err := GTsoAllocator.SetTSO(tsoutil.ComposeTS(physical, logical))
	assert.Nil(t, err)
}

func TestGlobalTSOAllocator_UpdateTSO(t *testing.T) {
	err := GTsoAllocator.UpdateTSO()
	assert.Nil(t, err)
}

func TestGlobalTSOAllocator_Reset(t *testing.T) {
	GTsoAllocator.Reset()
}

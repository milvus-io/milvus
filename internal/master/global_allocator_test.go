package master

import (
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/zilliztech/milvus-distributed/internal/util/tsoutil"
)

var gTestTsoAllocator Allocator
var gTestIDAllocator *GlobalIDAllocator

func TestMain(m *testing.M) {
	Params.Init()

	etcdAddr := Params.EtcdAddress
	gTestTsoAllocator = NewGlobalTSOAllocator("timestamp", tsoutil.NewTSOKVBase([]string{etcdAddr}, "/test/root/kv", "tso"))
	gTestIDAllocator = NewGlobalIDAllocator("idTimestamp", tsoutil.NewTSOKVBase([]string{etcdAddr}, "/test/root/kv", "gid"))
	exitCode := m.Run()
	os.Exit(exitCode)
}

func TestGlobalTSOAllocator_Initialize(t *testing.T) {
	err := gTestTsoAllocator.Initialize()
	assert.Nil(t, err)
}

func TestGlobalTSOAllocator_GenerateTSO(t *testing.T) {
	count := 1000
	perCount := uint32(100)
	startTs, err := gTestTsoAllocator.GenerateTSO(perCount)
	assert.Nil(t, err)
	lastPhysical, lastLogical := tsoutil.ParseTS(startTs)
	for i := 0; i < count; i++ {
		ts, _ := gTestTsoAllocator.GenerateTSO(perCount)
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
	err := gTestTsoAllocator.SetTSO(tsoutil.ComposeTS(physical, logical))
	assert.Nil(t, err)
}

func TestGlobalTSOAllocator_UpdateTSO(t *testing.T) {
	err := gTestTsoAllocator.UpdateTSO()
	assert.Nil(t, err)
}

func TestGlobalTSOAllocator_Reset(t *testing.T) {
	gTestTsoAllocator.Reset()
}

func TestGlobalIdAllocator_Initialize(t *testing.T) {
	err := gTestIDAllocator.Initialize()
	assert.Nil(t, err)
}

func TestGlobalIdAllocator_AllocOne(t *testing.T) {
	one, err := gTestIDAllocator.AllocOne()
	assert.Nil(t, err)
	ano, err := gTestIDAllocator.AllocOne()
	assert.Nil(t, err)
	assert.NotEqual(t, one, ano)
}

func TestGlobalIdAllocator_Alloc(t *testing.T) {
	count := uint32(2 << 10)
	idStart, idEnd, err := gTestIDAllocator.Alloc(count)
	assert.Nil(t, err)
	assert.Equal(t, count, uint32(idEnd-idStart))
}

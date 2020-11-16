package id

import (
	"os"
	"testing"

	"github.com/stretchr/testify/assert"

	gparams "github.com/zilliztech/milvus-distributed/internal/util/paramtableutil"
	"github.com/zilliztech/milvus-distributed/internal/util/tsoutil"
)

var GIdAllocator *GlobalIDAllocator

func TestMain(m *testing.M) {
	err := gparams.GParams.LoadYaml("config.yaml")
	if err != nil {
		panic(err)
	}
	GIdAllocator = NewGlobalIDAllocator("idTimestamp", tsoutil.NewTSOKVBase("gid"))
	exitCode := m.Run()
	os.Exit(exitCode)
}

func TestGlobalIdAllocator_Initialize(t *testing.T) {
	err := GIdAllocator.Initialize()
	assert.Nil(t, err)
}

func TestGlobalIdAllocator_AllocOne(t *testing.T) {
	one, err := GIdAllocator.AllocOne()
	assert.Nil(t, err)
	ano, err := GIdAllocator.AllocOne()
	assert.Nil(t, err)
	assert.NotEqual(t, one, ano)
}

func TestGlobalIdAllocator_Alloc(t *testing.T) {
	count := uint32(2 << 10)
	idStart, idEnd, err := GIdAllocator.Alloc(count)
	assert.Nil(t, err)
	assert.Equal(t, count, uint32(idEnd-idStart))
}

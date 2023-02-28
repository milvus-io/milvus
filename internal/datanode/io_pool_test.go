package datanode

import (
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/milvus-io/milvus/internal/util/conc"
	"github.com/milvus-io/milvus/internal/util/paramtable"
)

func Test_getOrCreateIOPool(t *testing.T) {
	Params.Init()
	ioConcurrency := Params.DataNodeCfg.IOConcurrency
	paramtable.Get().Save(Params.DataNodeCfg.IOConcurrency.Key, "64")
	defer func() { Params.DataNodeCfg.IOConcurrency = ioConcurrency }()
	nP := 10
	nTask := 10
	wg := sync.WaitGroup{}
	for i := 0; i < nP; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			p := getOrCreateIOPool()
			futures := make([]*conc.Future[any], 0, nTask)
			for j := 0; j < nTask; j++ {
				future := p.Submit(func() (interface{}, error) {
					return nil, nil
				})
				futures = append(futures, future)
			}
			err := conc.AwaitAll(futures...)
			assert.NoError(t, err)
		}()
	}
	wg.Wait()
}

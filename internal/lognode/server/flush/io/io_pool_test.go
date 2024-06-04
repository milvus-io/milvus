package io

import (
	"sync"
	"testing"

	"github.com/milvus-io/milvus/internal/lognode/server"
	"github.com/stretchr/testify/assert"

	"github.com/milvus-io/milvus/pkg/util/conc"
	"github.com/milvus-io/milvus/pkg/util/paramtable"
)

func Test_getOrCreateIOPool(t *testing.T) {
	ioConcurrency := server.Params.DataNodeCfg.IOConcurrency.GetValue()
	paramtable.Get().Save(server.Params.DataNodeCfg.IOConcurrency.Key, "64")
	defer func() { server.Params.Save(server.Params.DataNodeCfg.IOConcurrency.Key, ioConcurrency) }()
	nP := 10
	nTask := 10
	wg := sync.WaitGroup{}
	for i := 0; i < nP; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			p := GetOrCreateIOPool()
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

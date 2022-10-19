package datanode

import (
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/milvus-io/milvus/internal/util/concurrency"
)

func Test_getOrCreateIOPool(t *testing.T) {
	Params.InitOnce()
	ioConcurrency := Params.DataNodeCfg.IOConcurrency
	Params.DataNodeCfg.IOConcurrency = 64
	defer func() { Params.DataNodeCfg.IOConcurrency = ioConcurrency }()
	nP := 10
	nTask := 10
	wg := sync.WaitGroup{}
	for i := 0; i < nP; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			p := getOrCreateIOPool()
			futures := make([]*concurrency.Future, 0, nTask)
			for j := 0; j < nTask; j++ {
				future := p.Submit(func() (interface{}, error) {
					return nil, nil
				})
				futures = append(futures, future)
			}
			err := concurrency.AwaitAll(futures...)
			assert.NoError(t, err)
		}()
	}
	wg.Wait()
}

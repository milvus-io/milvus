package observers

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/suite"

	"github.com/milvus-io/milvus/pkg/v2/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v2/util/typeutil"
)

type taskDispatcherSuite struct {
	suite.Suite
}

func (s *taskDispatcherSuite) SetupSuite() {
	paramtable.Get().Init(paramtable.NewBaseTable())
}

func (s *taskDispatcherSuite) TestMultipleSubmit() {
	var wg sync.WaitGroup
	wg.Add(6)

	set := typeutil.NewConcurrentSet[int64]()
	dispatcher := newTaskDispatcher(func(ctx context.Context, key int64) {
		defer wg.Done()
		set.Insert(key)
		time.Sleep(time.Second)
	})
	dispatcher.Start()

	dispatcher.AddTask(1, 2, 3, 4, 5)
	dispatcher.AddTask(2, 3, 4, 5, 6)
	wg.Wait()

	s.ElementsMatch([]int64{1, 2, 3, 4, 5, 6}, set.Collect())

	dispatcher.Stop()
}

func TestTaskDispatcher(t *testing.T) {
	suite.Run(t, new(taskDispatcherSuite))
}

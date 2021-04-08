package proxynode

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/zilliztech/milvus-distributed/internal/log"
	"go.uber.org/zap"
)

func TestTaskCondition_Ctx(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())

	c := NewTaskCondition(ctx)
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		<-c.Ctx().Done()
		log.Debug("TestTaskCondition_Ctx", zap.Any("exit", c.Ctx().Err()))
	}()

	cancel()

	wg.Wait()
}

func TestTaskCondition_WaitToFinish(t *testing.T) {
	ctx1, cancel1 := context.WithCancel(context.Background())
	defer cancel1()
	c1 := NewTaskCondition(ctx1)

	var wg sync.WaitGroup

	wg.Add(1)
	go func() {
		defer wg.Done()
		err := c1.WaitToFinish()
		assert.Equal(t, nil, err)
	}()
	c1.Notify(nil)
	wg.Wait()

	wg.Add(1)
	go func() {
		defer wg.Done()
		err := c1.WaitToFinish()
		assert.NotEqual(t, nil, err)
	}()
	c1.Notify(errors.New("TestTaskCondition"))
	wg.Wait()

	ctx2, cancel2 := context.WithTimeout(context.Background(), time.Millisecond*100)
	defer cancel2()
	c2 := NewTaskCondition(ctx2)

	wg.Add(1)
	go func() {
		defer wg.Done()
		err := c2.WaitToFinish() // timeout
		assert.NotEqual(t, nil, err)
	}()
	wg.Wait()
}

func TestTaskCondition_Notify(t *testing.T) {
	ctx1, cancel1 := context.WithCancel(context.Background())
	defer cancel1()
	c1 := NewTaskCondition(ctx1)

	var wg sync.WaitGroup

	wg.Add(1)
	go func() {
		defer wg.Done()
		err := c1.WaitToFinish()
		assert.Equal(t, nil, err)
	}()
	c1.Notify(nil)
	wg.Wait()

	wg.Add(1)
	go func() {
		defer wg.Done()
		err := c1.WaitToFinish()
		assert.NotEqual(t, nil, err)
	}()
	c1.Notify(errors.New("TestTaskCondition"))
	wg.Wait()

	ctx2, cancel2 := context.WithTimeout(context.Background(), time.Millisecond*100)
	defer cancel2()
	c2 := NewTaskCondition(ctx2)

	wg.Add(1)
	go func() {
		defer wg.Done()
		err := c2.WaitToFinish() // timeout
		assert.NotEqual(t, nil, err)
	}()
	wg.Wait()
}

package proxyservice

import (
	"context"
	"math/rand"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestTaskScheduler_Start(t *testing.T) {
	sched := newTaskScheduler(context.Background())
	sched.Start()
	defer sched.Close()

	num := 64
	var wg sync.WaitGroup
	for i := 0; i < num; i++ {
		wg.Add(1)
		switch rand.Int() % 3 {
		case 0:
			go func() {
				defer wg.Done()
				tsk := newMockRegisterLinkTask(context.Background())
				err := sched.RegisterLinkTaskQueue.Enqueue(tsk)
				assert.Equal(t, nil, err)
			}()
		case 1:
			go func() {
				defer wg.Done()
				tsk := newMockRegisterNodeTask(context.Background())
				err := sched.RegisterNodeTaskQueue.Enqueue(tsk)
				assert.Equal(t, nil, err)
			}()
		case 2:
			go func() {
				defer wg.Done()
				tsk := newMockInvalidateCollectionMetaCacheTask(context.Background())
				err := sched.InvalidateCollectionMetaCacheTaskQueue.Enqueue(tsk)
				assert.Equal(t, nil, err)
			}()
		default:
			go func() {
				defer wg.Done()
				tsk := newMockRegisterLinkTask(context.Background())
				err := sched.RegisterLinkTaskQueue.Enqueue(tsk)
				assert.Equal(t, nil, err)
			}()
		}
	}
	wg.Wait()

	time.Sleep(3 * time.Second)
}

func TestTaskScheduler_Close(t *testing.T) {
	sched := newTaskScheduler(context.Background())
	sched.Start()
	defer sched.Close()

	num := 64
	var wg sync.WaitGroup
	for i := 0; i < num; i++ {
		wg.Add(1)
		switch rand.Int() % 3 {
		case 0:
			go func() {
				defer wg.Done()
				tsk := newMockRegisterLinkTask(context.Background())
				err := sched.RegisterLinkTaskQueue.Enqueue(tsk)
				assert.Equal(t, nil, err)
			}()
		case 1:
			go func() {
				defer wg.Done()
				tsk := newMockRegisterNodeTask(context.Background())
				err := sched.RegisterNodeTaskQueue.Enqueue(tsk)
				assert.Equal(t, nil, err)
			}()
		case 2:
			go func() {
				defer wg.Done()
				tsk := newMockInvalidateCollectionMetaCacheTask(context.Background())
				err := sched.InvalidateCollectionMetaCacheTaskQueue.Enqueue(tsk)
				assert.Equal(t, nil, err)
			}()
		default:
			go func() {
				defer wg.Done()
				tsk := newMockRegisterLinkTask(context.Background())
				err := sched.RegisterLinkTaskQueue.Enqueue(tsk)
				assert.Equal(t, nil, err)
			}()
		}
	}

	wg.Wait()
}

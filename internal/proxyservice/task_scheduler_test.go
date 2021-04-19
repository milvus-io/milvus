// Copyright (C) 2019-2020 Zilliz. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software distributed under the License
// is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
// or implied. See the License for the specific language governing permissions and limitations under the License.

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

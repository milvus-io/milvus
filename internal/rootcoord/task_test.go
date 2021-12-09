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

package rootcoord

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/milvus-io/milvus/internal/log"
	"go.uber.org/zap"

	"github.com/stretchr/testify/assert"

	"github.com/milvus-io/milvus/internal/proto/commonpb"
)

type mockTask struct {
	ctx         context.Context
	core        *Core
	executeFunc func(ctx context.Context) error
	t           commonpb.MsgType
}

func (t mockTask) Ctx() context.Context {
	return t.ctx
}

func (t mockTask) Type() commonpb.MsgType {
	return t.t
}

func (t mockTask) Execute(ctx context.Context) error {
	if t.executeFunc != nil {
		return t.executeFunc(ctx)
	}
	return nil
}

func (t mockTask) Core() *Core {
	return t.core
}

func newMockTask() *mockTask {
	return &mockTask{}
}

func Test_executeTask(t *testing.T) {
	// normal case
	m1 := newMockTask()
	m1.core = &Core{ctx: context.Background()}
	m1.ctx = context.Background()
	assert.NoError(t, executeTask(m1))

	// core context canceled
	m2 := newMockTask()
	timeout2 := time.Millisecond
	ctx2, cancel2 := context.WithTimeout(context.Background(), timeout2)
	time.Sleep(timeout2)
	defer cancel2()
	m2.core = &Core{ctx: ctx2}
	m2.ctx = context.Background()
	ch2 := make(chan struct{}, 1)
	m2.executeFunc = func(ctx context.Context) error {
		<-ch2
		return nil
	}
	assert.Error(t, executeTask(m2))
	ch2 <- struct{}{}

	// task context canceled
	m3 := newMockTask()
	timeout3 := time.Millisecond
	ctx3, cancel3 := context.WithTimeout(context.Background(), timeout3)
	time.Sleep(timeout3)
	defer cancel3()
	m3.core = &Core{ctx: context.Background()}
	m3.ctx = ctx3
	ch3 := make(chan struct{}, 1)
	m3.executeFunc = func(ctx context.Context) error {
		<-ch3
		return nil
	}
	assert.Error(t, executeTask(m3))
	ch3 <- struct{}{}

	// execution done but core context canceled,
	// can be covered occasionally
	ctx4, cancel4 := context.WithCancel(context.Background())
	m4 := newMockTask()
	m4.core = &Core{ctx: ctx4}
	m4.ctx = context.Background()
	ch4 := make(chan struct{}, 1)
	m4.executeFunc = func(ctx context.Context) error {
		defer cancel4()
		<-ch4
		return nil
	}
	var wg4 sync.WaitGroup
	wg4.Add(1)
	go func() {
		defer wg4.Done()

		// assert.Error(t, executeTask(m4))
		log.Debug("Test_executeTask, case4", zap.Error(executeTask(m4)))
	}()
	ch4 <- struct{}{}
	wg4.Wait()

	// execution done but task context canceled,
	// can be covered occasionally
	ctx5, cancel5 := context.WithCancel(context.Background())
	m5 := newMockTask()
	m5.core = &Core{ctx: context.Background()}
	m5.ctx = ctx5
	ch5 := make(chan struct{}, 1)
	m5.executeFunc = func(ctx context.Context) error {
		defer cancel5()
		<-ch5
		return nil
	}
	var wg5 sync.WaitGroup
	wg5.Add(1)
	go func() {
		defer wg5.Done()

		// assert.Error(t, executeTask(m5))
		log.Debug("Test_executeTask, case5", zap.Error(executeTask(m5)))
	}()
	ch5 <- struct{}{}
	wg5.Wait()
}

// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package proxy

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/assert"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus/pkg/v2/log"
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
	c1.Notify(errors.New("testTaskCondition"))
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
	c1.Notify(errors.New("testTaskCondition"))
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

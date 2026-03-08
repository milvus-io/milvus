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

package conc

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/milvus-io/milvus/pkg/v2/util/hardware"
)

func TestPool(t *testing.T) {
	pool := NewDefaultPool[any]()

	taskNum := pool.Cap() * 2
	futures := make([]*Future[any], 0, taskNum)
	for i := 0; i < taskNum; i++ {
		res := i
		future := pool.Submit(func() (any, error) {
			time.Sleep(500 * time.Millisecond)
			return res, nil
		})
		futures = append(futures, future)
	}

	assert.Greater(t, pool.Running(), 0)
	AwaitAll(futures...)
	for i, future := range futures {
		res, err := future.Await()
		assert.NoError(t, err)
		assert.Equal(t, err, future.Err())
		assert.True(t, future.OK())
		assert.Equal(t, res, future.Value())
		assert.Equal(t, i, res.(int))

		// Await() should be idempotent
		<-future.Inner()
		resDup, errDup := future.Await()
		assert.Equal(t, res, resDup)
		assert.Equal(t, err, errDup)
	}
}

func TestPoolResize(t *testing.T) {
	cpuNum := hardware.GetCPUNum()

	pool := NewPool[any](cpuNum)

	assert.Equal(t, cpuNum, pool.Cap())

	err := pool.Resize(cpuNum * 2)
	assert.NoError(t, err)
	assert.Equal(t, cpuNum*2, pool.Cap())

	err = pool.Resize(0)
	assert.Error(t, err)

	pool = NewDefaultPool[any]()
	err = pool.Resize(cpuNum * 2)
	assert.Error(t, err)
}

func TestPoolWithPanic(t *testing.T) {
	pool := NewPool[any](1, WithConcealPanic(true))

	future := pool.Submit(func() (any, error) {
		panic("mocked panic")
	})

	// make sure error returned when conceal panic
	_, err := future.Await()
	assert.Error(t, err)
}

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

package producer

import (
	"context"
	"testing"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/assert"

	"github.com/milvus-io/milvus/internal/distributed/streaming/internal/errs"
	"github.com/milvus-io/milvus/internal/mocks/streamingnode/client/handler/mock_producer"
)

func TestProducerWithResumingError(t *testing.T) {
	p := newProducerWithResumingError("test-pchannel")

	t.Run("GetProducerAfterAvailable_Available", func(t *testing.T) {
		mockP := mock_producer.NewMockProducer(t)
		mockP.EXPECT().IsAvailable().Return(true)
		p.SwapProducer(mockP, nil)

		res, err := p.GetProducerAfterAvailable(context.Background())
		assert.NoError(t, err)
		assert.NotNil(t, res)
	})

	t.Run("GetProducerAfterAvailable_WaitUntilAvailable", func(t *testing.T) {
		p := newProducerWithResumingError("test-pchannel")
		mockP := mock_producer.NewMockProducer(t)
		mockP.EXPECT().IsAvailable().Return(true).Maybe()
		mockP.EXPECT().Available().Return(make(chan struct{})).Maybe()
		mockP.EXPECT().Close().Return().Maybe()

		ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
		defer cancel()

		resCh := make(chan struct {
			p   any
			err error
		}, 1)
		go func() {
			prod, err := p.GetProducerAfterAvailable(ctx)
			resCh <- struct {
				p   any
				err error
			}{prod, err}
		}()

		time.Sleep(50 * time.Millisecond)
		p.SwapProducer(mockP, nil)

		res := <-resCh
		assert.NoError(t, res.err)
		assert.NotNil(t, res.p)
	})

	t.Run("SwapProducer_NilOld", func(t *testing.T) {
		p := newProducerWithResumingError("test-pchannel")
		mockP := mock_producer.NewMockProducer(t)
		p.SwapProducer(mockP, nil)
		assert.NotNil(t, p.producer)
	})

	t.Run("GetProducerAfterAvailable_Error", func(t *testing.T) {
		p := newProducerWithResumingError("test-pchannel")
		testErr := errors.New("test-error")
		p.SwapProducer(nil, testErr)

		res, err := p.GetProducerAfterAvailable(context.Background())
		assert.Error(t, err)
		assert.Nil(t, res)
		assert.True(t, errors.Is(err, errs.ErrClosed))
	})

	t.Run("GetProducerAfterAvailable_ContextCanceled", func(t *testing.T) {
		p := newProducerWithResumingError("test-pchannel")
		ctx, cancel := context.WithCancel(context.Background())
		cancel()

		res, err := p.GetProducerAfterAvailable(ctx)
		assert.Error(t, err)
		assert.Nil(t, res)
		assert.True(t, errors.Is(err, errs.ErrCanceledOrDeadlineExceed))
	})

	t.Run("SwapProducer_CloseOld", func(t *testing.T) {
		p := newProducerWithResumingError("test-pchannel")
		mockP1 := mock_producer.NewMockProducer(t)
		mockP1.EXPECT().Close().Return().Once()

		p.SwapProducer(mockP1, nil)

		mockP2 := mock_producer.NewMockProducer(t)
		p.SwapProducer(mockP2, nil)
		// mockP1 should have been closed
		mockP1.AssertExpectations(t)
	})
}

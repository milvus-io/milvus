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

	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"

	"github.com/milvus-io/milvus/internal/mocks/streamingnode/client/handler/mock_producer"
	"github.com/milvus-io/milvus/pkg/v2/mocks/streaming/util/mock_message"
	"github.com/milvus-io/milvus/pkg/v2/streaming/util/types"
)

func TestResumingProducerMetrics(t *testing.T) {
	m := newResumingProducerMetrics("test-channel")
	assert.False(t, m.available)

	m.IntoAvailable()
	assert.True(t, m.available)
	m.IntoAvailable() // Already available

	m.IntoUnavailable()
	assert.False(t, m.available)
	m.IntoUnavailable() // Already unavailable

	m.IntoAvailable()
	m.Close()

	m2 := newResumingProducerMetrics("test-channel-2")
	m2.Close()
}

func TestProducerMetrics(t *testing.T) {
	m := newProducerMetrics("test-channel", true)
	guard := m.StartProduce(100)
	guard.Finish(nil)

	guard2 := m.StartProduce(200)
	guard2.Finish(errors.New("test"))

	guard3 := m.StartProduce(300)
	guard3.Finish(context.Canceled)

	m.Close()
}

func TestProducerWithMetrics(t *testing.T) {
	p := mock_producer.NewMockProducer(t)
	pm := newProducerWithMetrics("test-channel", p)
	assert.NotNil(t, pm)

	msg := mock_message.NewMockMutableMessage(t)
	msg.EXPECT().EstimateSize().Return(100)
	p.EXPECT().Append(mock.Anything, mock.Anything).Return(&types.AppendResult{}, nil)
	p.EXPECT().Close().Return()

	_, err := pm.Append(context.Background(), msg)
	assert.NoError(t, err)

	pm.Close()

	assert.Nil(t, newProducerWithMetrics("test", nil))
}

func TestParseError(t *testing.T) {
	assert.Equal(t, "ok", parseError(nil))
	assert.Equal(t, "cancel", parseError(context.Canceled))
	assert.Equal(t, "error", parseError(errors.New("test")))
}

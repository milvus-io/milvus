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

package pulsar

import (
	"bytes"
	"context"
	"math"
	"sync"
	"testing"

	"github.com/apache/pulsar-client-go/pulsar"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/pkg/v3/proto/messagespb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/types"
	"github.com/milvus-io/milvus/pkg/v3/streaming/walimpls"
	"github.com/milvus-io/milvus/pkg/v3/streaming/walimpls/helper"
	"github.com/milvus-io/milvus/pkg/v3/util/syncutil"
)

func TestEstimatePulsarRecordSize(t *testing.T) {
	tests := []struct {
		name       string
		payload    []byte
		properties map[string]string
		expected   int
	}{
		{name: "empty"},
		{name: "payload only", payload: []byte("payload"), expected: len("payload")},
		{
			name:       "properties only",
			properties: map[string]string{"": "", "键": "值"},
			expected:   len("键") + len("值"),
		},
		{
			name:       "payload and properties",
			payload:    []byte("payload"),
			properties: map[string]string{"key": "value", "longer-key": "v"},
			expected:   len("payload") + len("key") + len("value") + len("longer-key") + len("v"),
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			assert.Equal(t, test.expected, estimatePulsarRecordSize(test.payload, test.properties))
		})
	}
}

func TestEstimatePulsarRecordSizeForEncryptedChunks(t *testing.T) {
	const (
		logicalPlaintextSize = 64 << 10
		chunkSize            = 2 << 10
	)
	cipherHeader, err := message.EncodeProto(&messagespb.CipherHeader{
		EzId:         1,
		CollectionId: 2,
		SafeKey:      []byte("safe-key"),
		PayloadBytes: logicalPlaintextSize,
	})
	require.NoError(t, err)

	ciphertext := bytes.Repeat([]byte{0xab}, logicalPlaintextSize)
	original := message.NewMutableMessageBeforeAppend(ciphertext, map[string]string{
		"_ch": cipherHeader,
		"_vc": "by-dev-rootcoord-dml_0v0",
	})
	chunks := message.SplitIntoChunks(original, chunkSize)
	require.Len(t, chunks, logicalPlaintextSize/chunkSize)

	producer := &recordingPulsarProducer{}
	producerFuture := syncutil.NewFuture[pulsar.Producer]()
	producerFuture.Set(producer)
	backlogHelper := &backlogClearHelper{
		cond:      syncutil.NewContextCond(&sync.Mutex{}),
		threshold: math.MaxInt64,
	}
	w := &walImpl{
		WALHelper: helper.NewWALHelper(&walimpls.OpenOption{
			Channel: types.PChannelInfo{
				Name:       "test-channel",
				Term:       1,
				AccessMode: types.AccessModeRW,
			},
		}),
		p:                  producerFuture,
		backlogClearHelper: backlogHelper,
	}

	physicalSize := 0
	logicalEstimate := 0
	payloadSize := 0
	propertiesSize := 0
	for _, chunk := range chunks {
		pb := chunk.IntoMessageProto()
		physicalSize += estimatePulsarRecordSize(pb.Payload, pb.Properties)
		logicalEstimate += chunk.EstimateSize()
		payloadSize += len(pb.Payload)
		for key, value := range pb.Properties {
			propertiesSize += len(key) + len(value)
		}
		_, err := w.Append(context.Background(), chunk)
		require.NoError(t, err)
	}

	assert.Equal(t, len(ciphertext), payloadSize)
	assert.Equal(t, payloadSize+propertiesSize, physicalSize)
	assert.Greater(t, logicalEstimate, physicalSize)
	assert.Equal(t, int64(physicalSize), backlogHelper.written)
	assert.Len(t, producer.messages, len(chunks))
}

type recordingPulsarProducer struct {
	pulsar.Producer
	messages []*pulsar.ProducerMessage
}

func (p *recordingPulsarProducer) Send(_ context.Context, msg *pulsar.ProducerMessage) (pulsar.MessageID, error) {
	p.messages = append(p.messages, msg)
	return pulsar.EarliestMessageID(), nil
}

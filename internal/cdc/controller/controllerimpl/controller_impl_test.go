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

package controllerimpl

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"go.etcd.io/etcd/api/v3/mvccpb"
	clientv3 "go.etcd.io/etcd/client/v3"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus/internal/cdc/replication"
	"github.com/milvus-io/milvus/internal/cdc/resource"
	"github.com/milvus-io/milvus/internal/metastore/kv/streamingcoord"
	"github.com/milvus-io/milvus/internal/mocks/mock_metastore"
	"github.com/milvus-io/milvus/pkg/v2/mocks/mock_kv"
	"github.com/milvus-io/milvus/pkg/v2/proto/streamingpb"
)

func TestController_StartAndStop_WithEvents(t *testing.T) {
	mockReplicateManagerClient := replication.NewMockReplicateManagerClient(t)
	mockReplicateManagerClient.EXPECT().Close().Return()

	mockReplicationCatalog := mock_metastore.NewMockReplicationCatalog(t)
	mockReplicationCatalog.EXPECT().ListReplicatePChannels(mock.Anything).Return([]*streamingpb.ReplicatePChannelMeta{}, nil)

	// Create test data
	replicateMeta := &streamingpb.ReplicatePChannelMeta{
		SourceChannelName: "by-dev-test-source-channel",
		TargetChannelName: "by-dev-test-target-channel",
	}
	metaBytes, _ := proto.Marshal(replicateMeta)

	// Create mock events
	putEvent := &clientv3.Event{
		Type: mvccpb.PUT,
		Kv: &mvccpb.KeyValue{
			Value: metaBytes,
		},
	}

	deleteEvent := &clientv3.Event{
		Type: mvccpb.DELETE,
		Kv: &mvccpb.KeyValue{
			Value: metaBytes,
		},
	}

	mockWatchKV := mock_kv.NewMockWatchKV(t)
	mockWatchKV.EXPECT().WatchWithPrefix(mock.Anything, streamingcoord.ReplicatePChannelMetaPrefix).RunAndReturn(func(ctx context.Context, prefix string) clientv3.WatchChan {
		eventCh := make(chan clientv3.WatchResponse, 2)

		// Send events
		go func() {
			eventCh <- clientv3.WatchResponse{
				Events: []*clientv3.Event{putEvent},
			}
			eventCh <- clientv3.WatchResponse{
				Events: []*clientv3.Event{deleteEvent},
			}
		}()

		return eventCh
	})

	notifyCh := make(chan struct{}, 2)
	mockReplicateManagerClient.EXPECT().CreateReplicator(mock.Anything).RunAndReturn(func(replicate *streamingpb.ReplicatePChannelMeta) {
		notifyCh <- struct{}{}
	})
	mockReplicateManagerClient.EXPECT().RemoveReplicator(mock.Anything).RunAndReturn(func(replicate *streamingpb.ReplicatePChannelMeta) {
		notifyCh <- struct{}{}
	})

	resource.InitForTest(t,
		resource.OptReplicateManagerClient(mockReplicateManagerClient),
		resource.OptWatchKV(mockWatchKV),
		resource.OptReplicationCatalog(mockReplicationCatalog),
	)

	ctrl := NewController()
	go ctrl.Start()

	// Wait for events to be processed
	<-notifyCh
	<-notifyCh

	ctrl.Stop()
}

func TestController_StartAndStop_WithCompactError(t *testing.T) {
	mockReplicateManagerClient := replication.NewMockReplicateManagerClient(t)
	mockReplicateManagerClient.EXPECT().Close().Return()

	mockReplicationCatalog := mock_metastore.NewMockReplicationCatalog(t)
	mockReplicationCatalog.EXPECT().ListReplicatePChannels(mock.Anything).Return([]*streamingpb.ReplicatePChannelMeta{}, nil)

	// Create test data
	replicateMeta := &streamingpb.ReplicatePChannelMeta{
		SourceChannelName: "by-dev-test-source-channel",
		TargetChannelName: "by-dev-test-target-channel",
	}
	metaBytes, _ := proto.Marshal(replicateMeta)

	// Create mock events
	putEvent := &clientv3.Event{
		Type: mvccpb.PUT,
		Kv: &mvccpb.KeyValue{
			Value: metaBytes,
		},
	}

	// Track how many times WatchWithPrefix is called
	watchCallCount := 0
	notifyCh := make(chan struct{}, 2)

	mockWatchKV := mock_kv.NewMockWatchKV(t)
	mockWatchKV.EXPECT().WatchWithPrefix(mock.Anything, streamingcoord.ReplicatePChannelMetaPrefix).RunAndReturn(func(ctx context.Context, prefix string) clientv3.WatchChan {
		watchCallCount++
		eventCh := make(chan clientv3.WatchResponse, 2)

		go func() {
			if watchCallCount == 1 {
				// First call: send compact error
				eventCh <- clientv3.WatchResponse{
					CompactRevision: 1,
				}
			} else {
				// Second call: send normal event after recovery
				eventCh <- clientv3.WatchResponse{
					Events: []*clientv3.Event{putEvent},
				}
			}
		}()

		return eventCh
	})

	mockReplicateManagerClient.EXPECT().CreateReplicator(mock.Anything).RunAndReturn(func(replicate *streamingpb.ReplicatePChannelMeta) {
		notifyCh <- struct{}{}
	})

	resource.InitForTest(t,
		resource.OptReplicateManagerClient(mockReplicateManagerClient),
		resource.OptWatchKV(mockWatchKV),
		resource.OptReplicationCatalog(mockReplicationCatalog),
	)

	ctrl := NewController()
	go ctrl.Start()

	// Wait for the event to be processed after recovery
	<-notifyCh

	// Verify that WatchWithPrefix was called twice (once for initial watch, once after compact error)
	assert.Equal(t, 2, watchCallCount)
	ctrl.Stop()
}

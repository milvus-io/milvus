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

package controller

import (
	"testing"
	"time"

	"github.com/stretchr/testify/mock"
	"go.etcd.io/etcd/api/v3/mvccpb"
	clientv3 "go.etcd.io/etcd/client/v3"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus/internal/cdc/meta"
	"github.com/milvus-io/milvus/internal/cdc/replication"
	"github.com/milvus-io/milvus/internal/cdc/resource"
	"github.com/milvus-io/milvus/internal/metastore/kv/streamingcoord"
	"github.com/milvus-io/milvus/pkg/v2/proto/streamingpb"
)

func TestController_StartAndStop_WithEvents(t *testing.T) {
	mockReplicateManagerClient := replication.NewMockReplicateManagerClient(t)
	mockReplicateManagerClient.EXPECT().Close().Return()

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
			Key:   []byte(streamingcoord.BuildReplicatePChannelMetaKey(replicateMeta)),
			Value: metaBytes,
		},
	}

	deleteEvent := &clientv3.Event{
		Type: mvccpb.DELETE,
		Kv: &mvccpb.KeyValue{
			Key: []byte(streamingcoord.BuildReplicatePChannelMetaKey(replicateMeta)),
		},
		PrevKv: &mvccpb.KeyValue{
			Value: metaBytes,
		},
	}

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

	notifyCh := make(chan struct{}, 2)
	mockReplicateManagerClient.EXPECT().CreateReplicator(mock.Anything).RunAndReturn(func(replicate *meta.ReplicateChannel) {
		notifyCh <- struct{}{}
	})
	mockReplicateManagerClient.EXPECT().RemoveReplicator(mock.Anything, mock.Anything).RunAndReturn(func(key string, modRevision int64) {
		notifyCh <- struct{}{}
	})

	resource.InitForTest(t,
		resource.OptReplicateManagerClient(mockReplicateManagerClient),
	)

	ctrl := NewController()
	go ctrl.watchLoop(eventCh)

	// Wait for put event to be processed
	select {
	case <-notifyCh:
	case <-time.After(10 * time.Second):
		t.Fatal("timeout waiting for put event")
	}
	// Wait for delete event to be processed
	select {
	case <-notifyCh:
	case <-time.After(10 * time.Second):
		t.Fatal("timeout waiting for delete event")
	}
	ctrl.Stop()
}

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

package rootcoord

import (
	"context"
	"testing"
	"time"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/internal/distributed/streaming"
	"github.com/milvus-io/milvus/internal/mocks/distributed/mock_streaming"
	"github.com/milvus-io/milvus/internal/streamingcoord/server/broadcaster/registry"
	qvresource "github.com/milvus-io/milvus/internal/views/coord/resource"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/util/funcutil"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

// Drop WAL consumption may already have removed the target vchannels while
// split adoption holds the lifecycle guard waiting for their serving views.
// Withdrawing load requirements must be possible while that guard is held.
func TestDropCollectionWithdrawsLoadBeforeWaitingForSplitLifecycle(t *testing.T) {
	mockey.PatchConvey("withdraw serving requirements before taking the split guard", t, func() {
		const collectionID int64 = 912345
		control := funcutil.GetControlChannel("drop-split-test")
		wal := mock_streaming.NewMockWALAccesser(t)
		wal.EXPECT().ControlChannel().Return(control).Maybe()
		streaming.SetWALForTest(wal)
		defer streaming.SetWALForTest(nil)

		release, err := qvresource.AcquireSplitLifecycle(t.Context(), collectionID)
		require.NoError(t, err)
		defer release()
		withdrawn := make(chan struct{})
		stop := merr.WrapErrServiceUnavailableMsg("stop before physical collection cleanup")
		mockey.Mock(registry.CallMessageAckCallback).To(func(_ context.Context, msg message.BroadcastMutableMessage, _ map[string]*message.AppendResult) error {
			if msg.MessageType() == message.MessageTypeDropLoadConfig {
				close(withdrawn)
				return nil
			}
			return stop
		}).Build()
		raw := message.NewDropCollectionMessageBuilderV1().
			WithHeader(&message.DropCollectionMessageHeader{CollectionId: collectionID}).
			WithBody(&message.DropCollectionRequest{}).
			WithBroadcast([]string{control}).MustBuildBroadcast().WithBroadcastID(1)
		result := message.BroadcastResultDropCollectionMessageV1{
			Message: message.MustAsBroadcastDropCollectionMessageV1(raw),
			Results: map[string]*message.AppendResult{control: {TimeTick: 100}},
		}
		ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
		defer cancel()
		done := make(chan error, 1)
		go func() { done <- (&DDLCallback{}).dropCollectionV1AckCallback(ctx, result) }()
		select {
		case <-withdrawn:
		case <-time.After(time.Second):
			cancel()
			<-done
			t.Fatal("drop could not withdraw serving requirements while split held its lifecycle guard")
		}
		select {
		case err := <-done:
			t.Fatalf("physical cleanup must wait for the lifecycle guard: %v", err)
		default:
		}
		release()
		require.ErrorIs(t, <-done, stop)
	})
}

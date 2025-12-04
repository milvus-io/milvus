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

//go:build test

package streaming

import (
	"context"

	"github.com/cockroachdb/errors"
	"google.golang.org/protobuf/types/known/anypb"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal"
	kvfactory "github.com/milvus-io/milvus/internal/util/dependency/kv"
	"github.com/milvus-io/milvus/pkg/v2/proto/messagespb"
	"github.com/milvus-io/milvus/pkg/v2/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v2/streaming/util/types"
	"github.com/milvus-io/milvus/pkg/v2/streaming/walimpls/impls/rmq"
	"github.com/milvus-io/milvus/pkg/v2/util/funcutil"
)

var expectErr = make(chan error, 10)

// SetWALForTest initializes the singleton of wal for test.
func SetWALForTest(w WALAccesser) {
	singleton = w
}

func RecoverWALForTest() {
	c, _ := kvfactory.GetEtcdAndPath()
	singleton = newWALAccesser(c)
}

func ExpectErrorOnce(err error) {
	expectErr <- err
}

func SetupNoopWALForTest() {
	singleton = &noopWALAccesser{}
}

type noopReplicateService struct{}

func (n *noopReplicateService) Append(ctx context.Context, msg message.ReplicateMutableMessage) (*types.AppendResult, error) {
	return nil, nil
}

func (n *noopReplicateService) UpdateReplicateConfiguration(ctx context.Context, config *commonpb.ReplicateConfiguration) error {
	return nil
}

func (n *noopReplicateService) GetReplicateCheckpoint(ctx context.Context, channelName string) (*wal.ReplicateCheckpoint, error) {
	return nil, nil
}

type noopBalancer struct{}

func (n *noopBalancer) ListStreamingNode(ctx context.Context) ([]types.StreamingNodeInfo, error) {
	return nil, nil
}

func (n *noopBalancer) GetWALDistribution(ctx context.Context, nodeID int64) (*types.StreamingNodeAssignment, error) {
	return nil, nil
}

func (n *noopBalancer) GetFrozenNodeIDs(ctx context.Context) ([]int64, error) {
	return nil, nil
}

func (n *noopBalancer) IsRebalanceSuspended(ctx context.Context) (bool, error) {
	return false, nil
}

func (n *noopBalancer) SuspendRebalance(ctx context.Context) error {
	return nil
}

func (n *noopBalancer) ResumeRebalance(ctx context.Context) error {
	return nil
}

func (n *noopBalancer) FreezeNodeIDs(ctx context.Context, nodeIDs []int64) error {
	return nil
}

func (n *noopBalancer) DefreezeNodeIDs(ctx context.Context, nodeIDs []int64) error {
	return nil
}

type noopLocal struct{}

func (n *noopLocal) GetLatestMVCCTimestampIfLocal(ctx context.Context, vchannel string) (uint64, error) {
	return 0, errors.New("not implemented")
}

func (n *noopLocal) GetMetricsIfLocal(ctx context.Context) (*types.StreamingNodeMetrics, error) {
	return &types.StreamingNodeMetrics{}, nil
}

type noopBroadcast struct{}

func (n *noopBroadcast) Append(ctx context.Context, msg message.BroadcastMutableMessage) (*types.BroadcastAppendResult, error) {
	if err := getExpectErr(); err != nil {
		return nil, err
	}
	return &types.BroadcastAppendResult{
		BroadcastID: 1,
		AppendResults: map[string]*types.AppendResult{
			"v1": {
				MessageID: rmq.NewRmqID(1),
				TimeTick:  10,
				Extra:     &anypb.Any{},
			},
		},
	}, nil
}

func (n *noopBroadcast) Ack(ctx context.Context, msg message.ImmutableMessage) error {
	return nil
}

type noopTxn struct{}

func (n *noopTxn) Append(ctx context.Context, msg message.MutableMessage, opts ...AppendOption) error {
	if err := getExpectErr(); err != nil {
		return err
	}
	return nil
}

func (n *noopTxn) Commit(ctx context.Context) (*types.AppendResult, error) {
	if err := getExpectErr(); err != nil {
		return nil, err
	}
	return &types.AppendResult{}, nil
}

func (n *noopTxn) Rollback(ctx context.Context) error {
	if err := getExpectErr(); err != nil {
		return err
	}
	return nil
}

type noopWALAccesser struct{}

func (n *noopWALAccesser) Replicate() ReplicateService {
	return &noopReplicateService{}
}

func (n *noopWALAccesser) ControlChannel() string {
	return funcutil.GetControlChannel("noop")
}

func (n *noopWALAccesser) Balancer() Balancer {
	return &noopBalancer{}
}

func (n *noopWALAccesser) WALName() string {
	return "noop"
}

func (n *noopWALAccesser) Local() Local {
	return &noopLocal{}
}

func (n *noopWALAccesser) Txn(ctx context.Context, opts TxnOption) (Txn, error) {
	if err := getExpectErr(); err != nil {
		return nil, err
	}
	return &noopTxn{}, nil
}

func (n *noopWALAccesser) RawAppend(ctx context.Context, msgs message.MutableMessage, opts ...AppendOption) (*types.AppendResult, error) {
	if err := getExpectErr(); err != nil {
		return nil, err
	}
	extra, _ := anypb.New(&messagespb.ManualFlushExtraResponse{
		SegmentIds: []int64{1, 2, 3},
	})
	return &types.AppendResult{
		MessageID: rmq.NewRmqID(1),
		TimeTick:  10,
		Extra:     extra,
	}, nil
}

func (n *noopWALAccesser) Broadcast() Broadcast {
	return &noopBroadcast{}
}

func (n *noopWALAccesser) Read(ctx context.Context, opts ReadOption) Scanner {
	return &noopScanner{}
}

func (n *noopWALAccesser) AppendMessages(ctx context.Context, msgs ...message.MutableMessage) AppendResponses {
	if err := getExpectErr(); err != nil {
		return AppendResponses{
			Responses: []AppendResponse{
				{
					AppendResult: nil,
					Error:        err,
				},
			},
		}
	}
	return AppendResponses{}
}

func (n *noopWALAccesser) AppendMessagesWithOption(ctx context.Context, opts AppendOption, msgs ...message.MutableMessage) AppendResponses {
	return AppendResponses{}
}

func (n *noopWALAccesser) GetReplicateConfiguration(ctx context.Context) (*commonpb.ReplicateConfiguration, error) {
	return nil, nil
}

func (n *noopWALAccesser) GetReplicateCheckpoint(ctx context.Context, channelName string) (*commonpb.ReplicateCheckpoint, error) {
	return nil, nil
}

func (n *noopWALAccesser) UpdateReplicateConfiguration(ctx context.Context, config *commonpb.ReplicateConfiguration) error {
	return nil
}

func (n *noopWALAccesser) ForwardService() ForwardService {
	return &noopForwardService{}
}

type noopForwardService struct{}

func (n *noopForwardService) ForwardLegacyProxy(ctx context.Context, request any) (any, error) {
	return nil, ErrForwardDisabled
}

type noopScanner struct{}

func (n *noopScanner) Done() <-chan struct{} {
	return make(chan struct{})
}

func (n *noopScanner) Error() error {
	return nil
}

func (n *noopScanner) Close() {
}

// getExpectErr is a helper function to get the error from the expectErr channel.
func getExpectErr() error {
	select {
	case err := <-expectErr:
		return err
	default:
		return nil
	}
}

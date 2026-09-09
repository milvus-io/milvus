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

package grpcproxyclient

import (
	"context"
	"strconv"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/encoding"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus/internal/mocks"
	"github.com/milvus-io/milvus/pkg/v3/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/proxypb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

func setRatesSnapshotForTest(collections int) *proxypb.SetRatesRequest {
	children := make(map[int64]*proxypb.LimiterNode, collections)
	for i := range collections {
		children[int64(i)] = &proxypb.LimiterNode{Limiter: &proxypb.Limiter{
			Rates: []*internalpb.Rate{{Rt: internalpb.RateType_DQLSearch, R: 100}},
		}}
	}
	return &proxypb.SetRatesRequest{
		Base:  &commonpb.MsgBase{MsgID: 42, Timestamp: 100, SourceID: 7, TargetID: 8},
		Rates: []*proxypb.CollectionRate{{Collection: 1}},
		RootLimiter: &proxypb.LimiterNode{Children: map[int64]*proxypb.LimiterNode{
			1: {Children: children},
		}},
	}
}

func TestCloneSetRatesEnvelope(t *testing.T) {
	require.Nil(t, cloneSetRatesEnvelope(nil))
	require.Nil(t, cloneSetRatesEnvelope(&proxypb.SetRatesRequest{}).GetBase())
	request := setRatesSnapshotForTest(2)
	// Unknown fields must survive old clients without sharing mutable header bytes.
	request.ProtoReflect().SetUnknown([]byte{0xa0, 0x06, 0x01})
	request.Base.ProtoReflect().SetUnknown([]byte{0xa0, 0x06, 0x02})
	before := proto.Clone(request)
	cloned := cloneSetRatesEnvelope(request)
	require.True(t, proto.Equal(request, cloned))
	require.NotSame(t, request, cloned)
	require.NotSame(t, request.Base, cloned.Base)
	require.Same(t, request.RootLimiter, cloned.RootLimiter)
	require.Same(t, request.Rates[0], cloned.Rates[0])
	cloned.Base.TargetID = 99
	cloned.ProtoReflect().GetUnknown()[2] = 3
	cloned.Base.ProtoReflect().GetUnknown()[2] = 4
	require.True(t, proto.Equal(before, request))
}

func TestSetRatesConcurrentSnapshotAndRetries(t *testing.T) {
	paramtable.Init()
	request := setRatesSnapshotForTest(128)
	request.ProtoReflect().SetUnknown([]byte{0xa0, 0x06, 0x01})
	original := proto.Clone(request)
	codec := encoding.GetCodecV2("proto")
	require.NotNil(t, codec)

	const clients = 8
	var wg sync.WaitGroup
	for i := range clients {
		targetID := int64(i + 100)
		proxy := mocks.NewMockProxyClient(t)
		transport := mocks.NewMockGrpcClient[proxypb.ProxyClient](t)
		transport.EXPECT().GetNodeID().Return(targetID).Once()
		calls := 0
		proxy.EXPECT().SetRates(mock.Anything, mock.Anything).RunAndReturn(
			func(_ context.Context, sent *proxypb.SetRatesRequest, _ ...grpc.CallOption) (*commonpb.Status, error) {
				calls++
				assert.Same(t, request.RootLimiter, sent.RootLimiter)
				assert.Equal(t, targetID, sent.GetBase().GetTargetID())
				assert.Equal(t, request.GetBase().GetSourceID(), sent.GetBase().GetSourceID())
				// Use the actual registered codec, including its size-cache path.
				data, err := codec.Marshal(sent)
				if !assert.NoError(t, err) {
					return nil, err
				}
				defer data.Free()
				decoded := &proxypb.SetRatesRequest{}
				assert.NoError(t, codec.Unmarshal(data, decoded))
				assert.True(t, proto.Equal(sent, decoded))
				if calls == 1 {
					return nil, context.DeadlineExceeded
				}
				return merr.Success(), nil
			}).Twice()
		transport.EXPECT().ReCall(mock.Anything, mock.Anything).RunAndReturn(
			func(_ context.Context, call func(proxypb.ProxyClient) (interface{}, error)) (interface{}, error) {
				_, err := call(proxy)
				assert.ErrorIs(t, err, context.DeadlineExceeded)
				return call(proxy)
			}).Once()
		client := &Client{grpcClient: transport}
		wg.Add(1)
		go func() {
			defer wg.Done()
			status, err := client.SetRates(context.Background(), request)
			assert.NoError(t, err)
			assert.NoError(t, merr.Error(status))
		}()
	}
	wg.Wait()
	require.True(t, proto.Equal(original, request))
}

var benchmarkSetRatesRequest *proxypb.SetRatesRequest

func BenchmarkSetRatesClone(b *testing.B) {
	for _, collections := range []int{10000, 100000} {
		request := setRatesSnapshotForTest(collections)
		b.Run(strconv.Itoa(collections)+"/deep", func(b *testing.B) {
			b.ReportAllocs()
			for b.Loop() {
				benchmarkSetRatesRequest = proto.Clone(request).(*proxypb.SetRatesRequest)
			}
		})
		b.Run(strconv.Itoa(collections)+"/envelope", func(b *testing.B) {
			b.ReportAllocs()
			for b.Loop() {
				benchmarkSetRatesRequest = cloneSetRatesEnvelope(request)
			}
		})
	}
}

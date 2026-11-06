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

package adaptor

import (
	"context"
	"io"
	"net"
	"testing"
	"time"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	clientconsumer "github.com/milvus-io/milvus/internal/streamingnode/client/handler/consumer"
	"github.com/milvus-io/milvus/internal/streamingnode/server/resource"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal"
	streamingserviceinterceptor "github.com/milvus-io/milvus/internal/util/streamingutil/service/interceptor"
	streamingstatus "github.com/milvus-io/milvus/internal/util/streamingutil/status"
	"github.com/milvus-io/milvus/pkg/v3/mocks/streaming/mock_walimpls"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	msgadaptor "github.com/milvus-io/milvus/pkg/v3/streaming/util/message/adaptor"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/options"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/types"
	"github.com/milvus-io/milvus/pkg/v3/streaming/walimpls"
	"github.com/milvus-io/milvus/pkg/v3/streaming/walimpls/impls/rmq"
	"github.com/milvus-io/milvus/pkg/v3/streaming/walimpls/impls/walimplstest"
)

func TestRemoteConsumerTransparentlyBridgesHistoricalAndCurrentWAL(t *testing.T) {
	resource.InitForTest(t)
	channel := types.PChannelInfo{
		Name:       "remote-cross-wal-bridge",
		Term:       1,
		AccessMode: types.AccessModeRO,
	}

	currentMessages := make(chan message.ImmutableMessage, 1)
	currentMessages <- newTestTimeTickMessage(
		101,
		walimplstest.NewTestMessageID(1),
		walimplstest.NewTestMessageID(1),
	)
	currentScanner := mock_walimpls.NewMockScannerImpls(t)
	currentScanner.EXPECT().Chan().Return(currentMessages).Maybe()
	currentScanner.EXPECT().Close().Return(nil).Once()
	currentWAL := mock_walimpls.NewMockWALImpls(t)
	currentWAL.EXPECT().WALName().Return(message.WALNameTest).Maybe()
	currentWAL.EXPECT().Channel().Return(channel).Maybe()
	currentWAL.EXPECT().Close().Return().Once()

	currentReadWAL := mock_walimpls.NewMockWALImpls(t)
	currentReadWAL.EXPECT().WALName().Return(message.WALNameTest).Maybe()
	currentReadWAL.EXPECT().Channel().Return(channel).Maybe()
	currentReadWAL.EXPECT().Close().Return().Once()
	currentReadWAL.EXPECT().Read(mock.Anything, mock.MatchedBy(func(opt walimpls.ReadOption) bool {
		_, ok := opt.DeliverPolicy.GetPolicy().(*streamingpb.DeliverPolicy_All)
		return ok
	})).Return(currentScanner, nil).Once()

	historicalMessages := make(chan message.ImmutableMessage, 1)
	historicalMessages <- newTestAlterWALMessage(
		commonpb.WALName_Test,
		100,
		rmq.NewRmqID(2),
		rmq.NewRmqID(1),
	)
	historicalScanner := mock_walimpls.NewMockScannerImpls(t)
	historicalScanner.EXPECT().Chan().Return(historicalMessages).Maybe()
	historicalScanner.EXPECT().Close().Return(nil).Once()
	historicalWAL := mock_walimpls.NewMockWALImpls(t)
	historicalWAL.EXPECT().WALName().Return(message.WALNameRocksmq).Maybe()
	historicalWAL.EXPECT().Close().Return().Once()
	historicalWAL.EXPECT().Read(mock.Anything, mock.MatchedBy(func(opt walimpls.ReadOption) bool {
		_, ok := opt.DeliverPolicy.GetPolicy().(*streamingpb.DeliverPolicy_StartFrom)
		return ok
	})).Return(historicalScanner, nil).Once()

	type historicalOpen struct {
		walName message.WALName
		channel types.PChannelInfo
	}
	historicalOpenCh := make(chan historicalOpen, 2)
	roWAL := adaptImplsToROWAL(currentWAL, func() {}, func(
		_ context.Context,
		walName message.WALName,
		gotChannel types.PChannelInfo,
	) (walimpls.ROWALImpls, error) {
		historicalOpenCh <- historicalOpen{walName: walName, channel: gotChannel}
		switch walName {
		case message.WALNameRocksmq:
			return historicalWAL, nil
		case message.WALNameTest:
			return currentReadWAL, nil
		default:
			t.Fatalf("unexpected WAL name %s", walName)
			return nil, nil
		}
	})
	defer roWAL.Close()

	grpcServer, conn := startRemoteConsumerTestServer(t, roWAL)
	defer grpcServer.Stop()
	defer conn.Close()

	resultCh := make(msgadaptor.ChanMessageHandler, 4)
	consumer, err := clientconsumer.CreateConsumer(
		context.Background(),
		&clientconsumer.ConsumerOptions{
			Assignment: &types.PChannelInfoAssigned{
				Channel: channel,
				Node:    types.StreamingNodeInfo{ServerID: 1, Address: "bufnet"},
			},
			VChannel:       "test-vchannel",
			DeliverPolicy:  options.DeliverPolicyStartFrom(rmq.NewRmqID(1)),
			MessageHandler: resultCh,
		},
		streamingpb.NewStreamingNodeHandlerServiceClient(conn),
	)
	require.NoError(t, err)

	timeTicks := make([]uint64, 0, 1)
	walNames := make([]message.WALName, 0, 1)
	for len(timeTicks) < 1 {
		select {
		case msg := <-resultCh:
			if msg.MessageType() == message.MessageTypeTimeTick {
				timeTicks = append(timeTicks, msg.TimeTick())
				walNames = append(walNames, msg.MessageID().WALName())
			}
		case <-consumer.Done():
			t.Fatalf("remote consumer closed while crossing WAL boundary: %v", consumer.Error())
		case <-time.After(time.Second):
			t.Fatalf("timed out waiting for bridged TimeTicks, got %v", timeTicks)
		}
	}
	require.Equal(t, []uint64{101}, timeTicks)
	require.Equal(t, []message.WALName{message.WALNameTest}, walNames)
	opened := <-historicalOpenCh
	require.Equal(t, message.WALNameRocksmq, opened.walName)
	require.Equal(t, channel, opened.channel)
	opened = <-historicalOpenCh
	require.Equal(t, message.WALNameTest, opened.walName)
	require.Equal(t, channel, opened.channel)
	require.NoError(t, consumer.Close())
}

func TestRemoteConsumerDoesNotAdvanceWhenHistoricalWALNameMismatches(t *testing.T) {
	resource.InitForTest(t)
	channel := types.PChannelInfo{
		Name:       "remote-cross-wal-unavailable",
		Term:       1,
		AccessMode: types.AccessModeRO,
	}

	currentWAL := mock_walimpls.NewMockWALImpls(t)
	currentWAL.EXPECT().WALName().Return(message.WALNameTest).Maybe()
	currentWAL.EXPECT().Channel().Return(channel).Maybe()
	currentWAL.EXPECT().Close().Return().Once()

	roWAL := adaptImplsToROWAL(currentWAL, func() {}, func(
		context.Context,
		message.WALName,
		types.PChannelInfo,
	) (walimpls.ROWALImpls, error) {
		return nil, streamingstatus.NewWALNameMismatchError(message.WALNameTest.String(), message.WALNameRocksmq.String())
	})
	defer roWAL.Close()

	grpcServer, conn := startRemoteConsumerTestServer(t, roWAL)
	defer grpcServer.Stop()
	defer conn.Close()

	resultCh := make(msgadaptor.ChanMessageHandler, 1)
	consumer, err := clientconsumer.CreateConsumer(
		context.Background(),
		&clientconsumer.ConsumerOptions{
			Assignment: &types.PChannelInfoAssigned{
				Channel: channel,
				Node:    types.StreamingNodeInfo{ServerID: 1, Address: "bufnet"},
			},
			VChannel:       "test-vchannel",
			DeliverPolicy:  options.DeliverPolicyStartFrom(rmq.NewRmqID(1)),
			MessageHandler: resultCh,
		},
		streamingpb.NewStreamingNodeHandlerServiceClient(conn),
	)
	require.NoError(t, err)

	select {
	case msg := <-resultCh:
		t.Fatalf("received current WAL message after historical WAL failure: %s", msg.MessageID())
	case <-consumer.Done():
		require.True(t, streamingstatus.AsStreamingError(consumer.Error()).IsUnrecoverable())
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for the consumer to fail-stop")
	}
	_ = consumer.Close()
}

type remoteConsumerTestServer struct {
	streamingpb.UnimplementedStreamingNodeHandlerServiceServer
	wal wal.WAL
}

func (s *remoteConsumerTestServer) Consume(stream streamingpb.StreamingNodeHandlerService_ConsumeServer) error {
	if err := stream.Send(&streamingpb.ConsumeResponse{
		Response: &streamingpb.ConsumeResponse_Create{
			Create: &streamingpb.CreateConsumerResponse{WalName: s.wal.WALName().String()},
		},
	}); err != nil {
		return err
	}

	req, err := stream.Recv()
	if err != nil {
		return err
	}
	createReq := req.GetCreateVchannelConsumer()
	if createReq == nil {
		return io.ErrUnexpectedEOF
	}
	scanner, err := s.wal.Read(stream.Context(), wal.ReadOption{
		VChannel:               createReq.GetVchannel(),
		DeliverPolicy:          createReq.GetDeliverPolicy(),
		MessageFilter:          createReq.GetDeliverFilters(),
		IgnorePauseConsumption: createReq.GetIgnorePauseConsumption(),
	})
	if err != nil {
		return err
	}
	defer scanner.Close()

	if err := stream.Send(&streamingpb.ConsumeResponse{
		Response: &streamingpb.ConsumeResponse_CreateVchannel{
			CreateVchannel: &streamingpb.CreateVChannelConsumerResponse{
				Response: &streamingpb.CreateVChannelConsumerResponse_ConsumerId{ConsumerId: 1},
			},
		},
	}); err != nil {
		return err
	}

	closeCh := make(chan error, 1)
	go func() {
		closeReq, err := stream.Recv()
		if err == nil && closeReq.GetClose() == nil {
			err = io.ErrUnexpectedEOF
		}
		closeCh <- err
	}()

	for {
		select {
		case msg, ok := <-scanner.Chan():
			if !ok {
				return scanner.Error()
			}
			if err := stream.Send(&streamingpb.ConsumeResponse{
				Response: &streamingpb.ConsumeResponse_Consume{
					Consume: &streamingpb.ConsumeMessageReponse{
						ConsumerId: 1,
						Message:    msg.IntoImmutableMessageProto(),
					},
				},
			}); err != nil {
				return err
			}
		case err := <-closeCh:
			if err != nil {
				return err
			}
			return stream.Send(&streamingpb.ConsumeResponse{
				Response: &streamingpb.ConsumeResponse_Close{
					Close: &streamingpb.CloseConsumerResponse{},
				},
			})
		case <-stream.Context().Done():
			return stream.Context().Err()
		}
	}
}

func startRemoteConsumerTestServer(t *testing.T, l wal.WAL) (*grpc.Server, *grpc.ClientConn) {
	t.Helper()
	listener := bufconn.Listen(1024 * 1024)
	grpcServer := grpc.NewServer(grpc.StreamInterceptor(
		streamingserviceinterceptor.NewStreamingServiceStreamServerInterceptor(),
	))
	streamingpb.RegisterStreamingNodeHandlerServiceServer(grpcServer, &remoteConsumerTestServer{wal: l})
	go func() {
		_ = grpcServer.Serve(listener)
	}()
	t.Cleanup(func() {
		_ = listener.Close()
	})

	conn, err := grpc.DialContext(
		context.Background(),
		"bufnet",
		grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) {
			return listener.Dial()
		}),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithChainStreamInterceptor(
			streamingserviceinterceptor.NewStreamingServiceStreamClientInterceptor(),
		),
	)
	require.NoError(t, err)
	return grpcServer, conn
}

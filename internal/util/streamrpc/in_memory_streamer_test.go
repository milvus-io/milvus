// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package streamrpc

import (
	"context"
	"io"
	"testing"

	"github.com/stretchr/testify/suite"
	"google.golang.org/grpc/metadata"
)

type InMemoryStreamerSuite struct {
	suite.Suite
}

func (s *InMemoryStreamerSuite) TestBufferedClose() {
	streamer := NewInMemoryStreamer[int64](context.Background(), 10)
	err := streamer.Send(1)
	s.NoError(err)
	err = streamer.Send(2)
	s.NoError(err)

	streamer.Close()

	r, err := streamer.Recv()
	s.NoError(err)
	s.EqualValues(1, r)

	r, err = streamer.Recv()
	s.NoError(err)
	s.EqualValues(2, r)

	_, err = streamer.Recv()
	s.Error(err)
}

func (s *InMemoryStreamerSuite) TestStreamerCtxCanceled() {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	streamer := NewInMemoryStreamer[int64](ctx, 10)
	err := streamer.Send(1)
	s.Error(err)

	_, err = streamer.Recv()
	s.Error(err)
	s.ErrorIs(err, io.EOF)
}

func (s *InMemoryStreamerSuite) TestMockedMethods() {
	streamer := NewInMemoryStreamer[int64](context.Background(), 10)

	s.NotPanics(func() {
		err := streamer.SetHeader(make(metadata.MD))
		s.Error(err)

		err = streamer.SendHeader(make(metadata.MD))
		s.Error(err)

		streamer.SetTrailer(make(metadata.MD))

		err = streamer.SendMsg(1)
		s.Error(err)

		err = streamer.RecvMsg(1)
		s.Error(err)

		trailer := streamer.Trailer()
		s.Nil(trailer)

		err = streamer.CloseSend()
		s.Error(err)
	})
}

func TestInMemoryStreamer(t *testing.T) {
	suite.Run(t, new(InMemoryStreamerSuite))
}

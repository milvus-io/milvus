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

package streamrpc

import (
	"context"
	"io"
	"sync"

	"go.uber.org/atomic"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"

	"github.com/milvus-io/milvus/pkg/v2/util/generic"
	"github.com/milvus-io/milvus/pkg/v2/util/merr"
)

// InMemoryStreamer is a utility to wrap in-memory stream methods.
type InMemoryStreamer[Msg any] struct {
	grpc.ClientStream
	grpc.ServerStream

	ctx       context.Context
	closed    atomic.Bool
	closeOnce sync.Once
	buffer    chan Msg
}

// SetHeader sets the header metadata. It may be called multiple times.
// When call multiple times, all the provided metadata will be merged.
// All the metadata will be sent out when one of the following happens:
//   - ServerStream.SendHeader() is called;
//   - The first response is sent out;
//   - An RPC status is sent out (error or success).
func (s *InMemoryStreamer[Msg]) SetHeader(_ metadata.MD) error {
	return merr.WrapErrServiceInternal("shall not be called")
}

// SendHeader sends the header metadata.
// The provided md and headers set by SetHeader() will be sent.
// It fails if called multiple times.
func (s *InMemoryStreamer[Msg]) SendHeader(_ metadata.MD) error {
	return merr.WrapErrServiceInternal("shall not be called")
}

// SetTrailer sets the trailer metadata which will be sent with the RPC status.
// When called more than once, all the provided metadata will be merged.
func (s *InMemoryStreamer[Msg]) SetTrailer(_ metadata.MD) {}

// SendMsg sends a message. On error, SendMsg aborts the stream and the
// error is returned directly.
//
// SendMsg blocks until:
//   - There is sufficient flow control to schedule m with the transport, or
//   - The stream is done, or
//   - The stream breaks.
//
// SendMsg does not wait until the message is received by the client. An
// untimely stream closure may result in lost messages.
//
// It is safe to have a goroutine calling SendMsg and another goroutine
// calling RecvMsg on the same stream at the same time, but it is not safe
// to call SendMsg on the same stream in different goroutines.
//
// It is not safe to modify the message after calling SendMsg. Tracing
// libraries and stats handlers may use the message lazily.
func (s *InMemoryStreamer[Msg]) SendMsg(m interface{}) error {
	return merr.WrapErrServiceInternal("shall not be called")
}

// RecvMsg blocks until it receives a message into m or the stream is
// done. It returns io.EOF when the client has performed a CloseSend. On
// any non-EOF error, the stream is aborted and the error contains the
// RPC status.
//
// It is safe to have a goroutine calling SendMsg and another goroutine
// calling RecvMsg on the same stream at the same time, but it is not
// safe to call RecvMsg on the same stream in different goroutines.
func (s *InMemoryStreamer[Msg]) RecvMsg(m interface{}) error {
	return merr.WrapErrServiceInternal("shall not be called")
}

// Header returns the header metadata received from the server if there
// is any. It blocks if the metadata is not ready to read.
func (s *InMemoryStreamer[Msg]) Header() (metadata.MD, error) {
	return nil, merr.WrapErrServiceInternal("shall not be called")
}

// Trailer returns the trailer metadata from the server, if there is any.
// It must only be called after stream.CloseAndRecv has returned, or
// stream.Recv has returned a non-nil error (including io.EOF).
func (s *InMemoryStreamer[Msg]) Trailer() metadata.MD {
	return nil
}

// CloseSend closes the send direction of the stream. It closes the stream
// when non-nil error is met. It is also not safe to call CloseSend
// concurrently with SendMsg.
func (s *InMemoryStreamer[Msg]) CloseSend() error {
	return merr.WrapErrServiceInternal("shall not be called")
}

func NewInMemoryStreamer[Msg any](ctx context.Context, bufferSize int) *InMemoryStreamer[Msg] {
	return &InMemoryStreamer[Msg]{
		ctx:    ctx,
		buffer: make(chan Msg, bufferSize),
	}
}

func (s *InMemoryStreamer[Msg]) Context() context.Context {
	return s.ctx
}

func (s *InMemoryStreamer[Msg]) Recv() (Msg, error) {
	select {
	case result, ok := <-s.buffer:
		if !ok {
			return generic.Zero[Msg](), io.EOF
		}
		return result, nil
	case <-s.ctx.Done():
		return generic.Zero[Msg](), io.EOF
	}
}

func (s *InMemoryStreamer[Msg]) Send(req Msg) error {
	if s.closed.Load() || s.ctx.Err() != nil {
		return merr.WrapErrIoFailedReason("streamer closed")
	}
	select {
	case s.buffer <- req:
		return nil
	case <-s.ctx.Done():
		return io.EOF
	}
}

func (s *InMemoryStreamer[Msg]) Close() {
	s.closeOnce.Do(func() {
		s.closed.Store(true)
		close(s.buffer)
	})
}

func (s *InMemoryStreamer[Msg]) IsClosed() bool {
	return s.closed.Load()
}

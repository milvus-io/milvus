// Copyright 2017 TiKV Project Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package server

import (
	"context"
	"io"
	//"strconv"
	"sync/atomic"
	"time"

	"github.com/czs007/suvlim/pkg/pdpb"
	"github.com/czs007/suvlim/errors"
	"github.com/pingcap/log"
	//"github.com/czs007/suvlim/util/tsoutil"
	"go.uber.org/zap"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const slowThreshold = 5 * time.Millisecond

// gRPC errors
var (
	// ErrNotLeader is returned when current server is not the leader and not possible to process request.
	// TODO: work as proxy.
	ErrNotLeader  = status.Errorf(codes.Unavailable, "not leader")
	ErrNotStarted = status.Errorf(codes.Unavailable, "server not started")
)

// Tso implements gRPC PDServer.
func (s *Server) Tso(stream pdpb.PD_TsoServer) error {
	for {
		request, err := stream.Recv()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return errors.WithStack(err)
		}
		start := time.Now()
		// TSO uses leader lease to determine validity. No need to check leader here.
		if s.IsClosed() {
			return status.Errorf(codes.Unknown, "server not started")
		}
		if request.GetHeader().GetClusterId() != s.clusterID {
			return status.Errorf(codes.FailedPrecondition, "mismatch cluster id, need %d but got %d", s.clusterID, request.GetHeader().GetClusterId())
		}
		count := request.GetCount()
		ts, err := s.tsoAllocator.GenerateTSO(count)
		if err != nil {
			return status.Errorf(codes.Unknown, err.Error())
		}

		elapsed := time.Since(start)
		if elapsed > slowThreshold {
			log.Warn("get timestamp too slow", zap.Duration("cost", elapsed))
		}
		response := &pdpb.TsoResponse{
			Header:    s.header(),
			Timestamp: &ts,
			Count:     count,
		}
		if err := stream.Send(response); err != nil {
			return errors.WithStack(err)
		}
	}
}


// AllocID implements gRPC PDServer.
func (s *Server) AllocID(ctx context.Context, request *pdpb.AllocIDRequest) (*pdpb.AllocIDResponse, error) {
	if err := s.validateRequest(request.GetHeader()); err != nil {
		return nil, err
	}

	// We can use an allocator for all types ID allocation.
	id, err := s.idAllocator.Alloc()
	if err != nil {
		return nil, status.Errorf(codes.Unknown, err.Error())
	}

	return &pdpb.AllocIDResponse{
		Header: s.header(),
		Id:     id,
	}, nil
}


const heartbeatSendTimeout = 5 * time.Second

var errSendHeartbeatTimeout = errors.New("send region heartbeat timeout")

// heartbeatServer wraps PD_RegionHeartbeatServer to ensure when any error
// occurs on Send() or Recv(), both endpoints will be closed.
type heartbeatServer struct {
	stream pdpb.PD_HeartbeatServer
	closed int32
}

func (s *heartbeatServer) Send(m *pdpb.HeartbeatResponse) error {
	if atomic.LoadInt32(&s.closed) == 1 {
		return io.EOF
	}
	done := make(chan error, 1)
	go func() { done <- s.stream.Send(m) }()
	select {
	case err := <-done:
		if err != nil {
			atomic.StoreInt32(&s.closed, 1)
		}
		return errors.WithStack(err)
	case <-time.After(heartbeatSendTimeout):
		atomic.StoreInt32(&s.closed, 1)
		return errors.WithStack(errSendHeartbeatTimeout)
	}
}

func (s *heartbeatServer) Recv() (*pdpb.HeartbeatRequest, error) {
	if atomic.LoadInt32(&s.closed) == 1 {
		return nil, io.EOF
	}
	req, err := s.stream.Recv()
	if err != nil {
		atomic.StoreInt32(&s.closed, 1)
		return nil, errors.WithStack(err)
	}
	return req, nil
}

// RegionHeartbeat implements gRPC PDServer.
func (s *Server) Heartbeat(stream pdpb.PD_HeartbeatServer) error {
	server := &heartbeatServer{stream: stream}

	for {
		request, err := server.Recv()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return errors.WithStack(err)
		}

		if err = s.validateRequest(request.GetHeader()); err != nil {
			return err
		}
		//msg:= "OK"
		//s.hbStreams.SendMsg(msg)
	}
}

// validateRequest checks if Server is leader and clusterID is matched.
// TODO: Call it in gRPC interceptor.
func (s *Server) validateRequest(header *pdpb.RequestHeader) error {
	if s.IsClosed() {
		return errors.WithStack(ErrNotLeader)
	}
	if header.GetClusterId() != s.clusterID {
		return status.Errorf(codes.FailedPrecondition, "mismatch cluster id, need %d but got %d", s.clusterID, header.GetClusterId())
	}
	return nil
}

func (s *Server) header() *pdpb.ResponseHeader {
	return &pdpb.ResponseHeader{ClusterId: s.clusterID}
}

func (s *Server) errorHeader(err *pdpb.Error) *pdpb.ResponseHeader {
	return &pdpb.ResponseHeader{
		ClusterId: s.clusterID,
		Error:     err,
	}
}

func (s *Server) notBootstrappedHeader() *pdpb.ResponseHeader {
	return s.errorHeader(&pdpb.Error{
		Type:    pdpb.ErrorType_NOT_BOOTSTRAPPED,
		Message: "cluster is not bootstrapped",
	})
}



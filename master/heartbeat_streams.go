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
	"sync"
	"time"

	"github.com/czs007/suvlim/util/logutil"
	"github.com/czs007/suvlim/pkg/pdpb"
)

// HeartbeatStream is an interface.
type HeartbeatStream interface {
	Send(*pdpb.HeartbeatResponse) error
}

// HeartbeatStreams is an interface of async region heartbeat.
type HeartbeatStreams interface {
	SendMsg(msg *pdpb.HeartbeatResponse)
	BindStream(peerID uint64, stream HeartbeatStream)
}

const (
	heartbeatStreamKeepAliveInterval = time.Minute
	heartbeatChanCapacity            = 1024
)

type streamUpdate struct {
	peerID uint64
	stream HeartbeatStream
}

type heartbeatStreams struct {
	wg             sync.WaitGroup
	hbStreamCtx    context.Context
	hbStreamCancel context.CancelFunc
	clusterID      uint64
	streams        map[uint64]HeartbeatStream
	msgCh          chan *pdpb.HeartbeatResponse
	streamCh       chan streamUpdate
}

func newHeartbeatStreams(ctx context.Context, clusterID uint64) *heartbeatStreams {
	hbStreamCtx, hbStreamCancel := context.WithCancel(ctx)
	hs := &heartbeatStreams{
		hbStreamCtx:    hbStreamCtx,
		hbStreamCancel: hbStreamCancel,
		clusterID:      clusterID,
		streams:        make(map[uint64]HeartbeatStream),
		msgCh:          make(chan *pdpb.HeartbeatResponse, heartbeatChanCapacity),
		streamCh:       make(chan streamUpdate, 1),
	}
	hs.wg.Add(1)
	go hs.run()
	return hs
}

func (s *heartbeatStreams) run() {
	defer logutil.LogPanic()

	defer s.wg.Done()

	keepAliveTicker := time.NewTicker(heartbeatStreamKeepAliveInterval)
	defer keepAliveTicker.Stop()

	//keepAlive := &pdpb.HeartbeatResponse{Header: &pdpb.ResponseHeader{ClusterId: s.clusterID}}

	for {
		select {
		case update := <-s.streamCh:
			s.streams[update.peerID] = update.stream
		case msg := <-s.msgCh:
			println("msgCh", msg)
		case <-keepAliveTicker.C:
			println("keepAlive")
		case <-s.hbStreamCtx.Done():
			return
		}
	}
}

func (s *heartbeatStreams) Close() {
	s.hbStreamCancel()
	s.wg.Wait()
}

func (s *heartbeatStreams) BindStream(peerID uint64, stream HeartbeatStream) {
	update := streamUpdate{
		peerID: peerID,
		stream: stream,
	}
	select {
	case s.streamCh <- update:
	case <-s.hbStreamCtx.Done():
	}
}

func (s *heartbeatStreams) SendMsg(msg *pdpb.HeartbeatResponse) {
	msg.Header = &pdpb.ResponseHeader{ClusterId: s.clusterID}
	select {
	case s.msgCh <- msg:
	case <-s.hbStreamCtx.Done():
	}
}

func (s *heartbeatStreams) sendErr(errType pdpb.ErrorType, errMsg string) {

	msg := &pdpb.HeartbeatResponse{
		Header: &pdpb.ResponseHeader{
			ClusterId: s.clusterID,
			Error: &pdpb.Error{
				Type:    errType,
				Message: errMsg,
			},
		},
	}

	select {
	case s.msgCh <- msg:
	case <-s.hbStreamCtx.Done():
	}
}

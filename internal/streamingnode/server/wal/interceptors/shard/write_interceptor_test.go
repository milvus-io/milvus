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

package shard

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/interceptors"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
)

// lifecycleStage tracks the resources that the composite must release.
type lifecycleStage struct {
	ready  chan struct{}
	events *[]string
	name   string
}

func (s *lifecycleStage) DoAppend(ctx context.Context, msg message.MutableMessage, next interceptors.Append) (message.MessageID, error) {
	return next(ctx, msg)
}
func (s *lifecycleStage) Ready() <-chan struct{} { return s.ready }
func (s *lifecycleStage) GracefulClose()         { *s.events = append(*s.events, s.name+"-graceful") }
func (s *lifecycleStage) Close()                 { *s.events = append(*s.events, s.name+"-close") }

func TestShardWriteLifecycle(t *testing.T) {
	var events []string
	timeTick := &lifecycleStage{ready: make(chan struct{}), events: &events, name: "timetick"}
	assignment := &lifecycleStage{ready: make(chan struct{}), events: &events, name: "assignment"}
	s := &shardWriteInterceptor{
		timeTick: timeTick,
		physical: interceptors.NewChainedInterceptor(timeTick, assignment),
	}
	ready := s.Ready()
	select {
	case <-ready:
		t.Fatal("shard became ready before its internal stages")
	default:
	}
	close(timeTick.ready)
	close(assignment.ready)
	<-ready
	s.GracefulClose()
	s.Close()
	require.Equal(t, []string{"timetick-graceful", "timetick-close", "assignment-close"}, events)
}

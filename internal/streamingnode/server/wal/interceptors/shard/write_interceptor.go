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

	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/interceptors"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
)

var (
	_ interceptors.InterceptorWithReady         = (*shardWriteInterceptor)(nil)
	_ interceptors.InterceptorWithGracefulClose = (*shardWriteInterceptor)(nil)
)

// shardWriteInterceptor owns logical writes. Each physical message then passes
// through timetick and segment assignment without repeating the decision.
type shardWriteInterceptor struct {
	pkindex  *pkIndexWriter
	timeTick interceptors.Interceptor
	physical interceptors.InterceptorWithReady
}

func (s *shardWriteInterceptor) DoAppend(ctx context.Context, msg message.MutableMessage, next interceptors.Append) (message.MessageID, error) {
	appendOne := func(ctx context.Context, msg message.MutableMessage) (message.MessageID, error) {
		return s.physical.DoAppend(ctx, msg, next)
	}
	if s.pkindex == nil {
		return appendOne(ctx, msg)
	}
	return interceptors.CollectAppendMetrics(pkIndexMetricName, s.pkindex.append)(ctx, msg, appendOne)
}

func (s *shardWriteInterceptor) Ready() <-chan struct{} {
	return s.physical.Ready()
}

func (s *shardWriteInterceptor) GracefulClose() {
	if closer, ok := s.timeTick.(interceptors.InterceptorWithGracefulClose); ok {
		closer.GracefulClose()
	}
}

func (s *shardWriteInterceptor) Close() {
	s.pkindex.Close()
	s.physical.Close()
}

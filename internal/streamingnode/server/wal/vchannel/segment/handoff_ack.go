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

package segment

import (
	"context"

	"github.com/cockroachdb/errors"

	"github.com/milvus-io/milvus/internal/dataview"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/retry"
)

type flushHandoffAcknowledger interface {
	AcknowledgeFlushHandoff(context.Context, *streamingpb.SegmentAssignmentMeta) error
}

type acknowledgeFlushHandoffTask struct{ segmentTaskBase }

func (t *acknowledgeFlushHandoffTask) timeTick() uint64 { return 0 }

func (t *acknowledgeFlushHandoffTask) Execute(ctx context.Context) error {
	return t.execute(ctx, func(ctx context.Context) error {
		return t.segment.lifecycle.(flushHandoffAcknowledger).AcknowledgeFlushHandoff(ctx, t.segment.AssignmentMeta())
	})
}

func (s *SegmentView) scheduleHandoffAckLocked() {
	if s.handoffAckScheduled || !s.meta.GetFlushDataDurable() || s.meta.GetSealedAtDataVersion() == nil {
		return
	}
	if _, ok := s.lifecycle.(flushHandoffAcknowledger); !ok {
		return
	}
	s.handoffAckScheduled = true
	s.pendingTasks = append(s.pendingTasks, &acknowledgeFlushHandoffTask{segmentTaskBase: s.newSegmentTaskBaseLocked("acknowledgeFlushHandoffTask")})
	s.maybeSubmitNextLocked()
}

func (w *segmentLifecycleWriter) AcknowledgeFlushHandoff(ctx context.Context, meta *streamingpb.SegmentAssignmentMeta) error {
	req := buildCommitL1SegmentRequest(w.serverID, meta)
	req.Base.Properties = map[string]string{dataview.FlushGroupProtocol: "1", dataview.FlushHandoffAck: dataview.FlushHandoffToken(meta.GetSealedAtDataVersion())}
	resp, err := w.coord.SaveBinlogPaths(retry.WithMaxAttemptsContext(ctx, maxRPCAttempts), req)
	err = merr.CheckRPCCall(resp, err)
	if errors.IsAny(err, merr.ErrSegmentNotFound, merr.ErrChannelNotFound) {
		return nil
	}
	return err
}

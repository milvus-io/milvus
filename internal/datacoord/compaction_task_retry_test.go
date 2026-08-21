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

package datacoord

import (
	"context"
	"testing"

	"github.com/stretchr/testify/suite"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

func TestClassifyFailureSuite(t *testing.T) {
	suite.Run(t, new(ClassifyFailureSuite))
}

type ClassifyFailureSuite struct {
	suite.Suite
}

func (s *ClassifyFailureSuite) SetupTest() {
	paramtable.Get().Save(paramtable.Get().DataCoordCfg.CompactionMaxRetryTimes.Key, "3")
}

func (s *ClassifyFailureSuite) TearDownTest() {
	paramtable.Get().Reset(paramtable.Get().DataCoordCfg.CompactionMaxRetryTimes.Key)
}

// recordingSave stands in for a task type's updateAndSaveTaskMeta: it applies the opts to
// a clone, exactly like the real ShadowClone-based implementations do, and remembers how
// many times it was invoked.
type recordingSave struct {
	task  *datapb.CompactionTask
	calls int
}

func (r *recordingSave) apply(opts ...compactionTaskOpt) error {
	r.calls++
	clone := proto.Clone(r.task).(*datapb.CompactionTask)
	for _, opt := range opts {
		opt(clone)
	}
	r.task = clone
	return nil
}

func (s *ClassifyFailureSuite) TestRetryableUnderBudgetRetries() {
	rec := &recordingSave{task: &datapb.CompactionTask{PlanID: 1, RetryTimes: 0}}
	retried := classifyFailure(context.Background(), "test", rec.task, merr.WrapErrServiceUnavailable("transient"), rec.apply)

	s.True(retried)
	s.Equal(int32(1), rec.task.GetRetryTimes())
	s.Equal(datapb.CompactionTaskState_unknown, rec.task.GetState())
	s.Empty(rec.task.GetFailReason())
	s.Equal(1, rec.calls)
}

func (s *ClassifyFailureSuite) TestRetryableAtBudgetTerminates() {
	rec := &recordingSave{task: &datapb.CompactionTask{PlanID: 1, RetryTimes: 3}}
	retried := classifyFailure(context.Background(), "test", rec.task, merr.WrapErrServiceUnavailable("transient"), rec.apply)

	s.False(retried)
	s.Equal(datapb.CompactionTaskState_failed, rec.task.GetState())
	s.NotEmpty(rec.task.GetFailReason())
	// retry_times is not bumped further once the budget is exhausted.
	s.Equal(int32(3), rec.task.GetRetryTimes())
}

func (s *ClassifyFailureSuite) TestNonRetryableTerminatesImmediately() {
	rec := &recordingSave{task: &datapb.CompactionTask{PlanID: 1, RetryTimes: 0}}
	retried := classifyFailure(context.Background(), "test", rec.task, merr.WrapErrIllegalCompactionPlan("bad plan"), rec.apply)

	s.False(retried)
	s.Equal(datapb.CompactionTaskState_failed, rec.task.GetState())
	s.Contains(rec.task.GetFailReason(), "bad plan")
	s.Equal(int32(0), rec.task.GetRetryTimes())
}

func (s *ClassifyFailureSuite) TestRetryBudgetIsConfigurable() {
	paramtable.Get().Save(paramtable.Get().DataCoordCfg.CompactionMaxRetryTimes.Key, "1")
	rec := &recordingSave{task: &datapb.CompactionTask{PlanID: 1, RetryTimes: 1}}
	retried := classifyFailure(context.Background(), "test", rec.task, merr.WrapErrServiceUnavailable("transient"), rec.apply)

	s.False(retried, "retry_times already at the configured max of 1, should terminate")
	s.Equal(datapb.CompactionTaskState_failed, rec.task.GetState())
}

func (s *ClassifyFailureSuite) TestRawErrorIsTreatedAsNonRetryable() {
	// An error that isn't a merr sentinel (e.g. a raw KV/storage error) is not classified
	// retryable by merr.IsRetryableErr, and must not be retried indefinitely.
	rec := &recordingSave{task: &datapb.CompactionTask{PlanID: 1, RetryTimes: 0}}
	retried := classifyFailure(context.Background(), "test", rec.task, errPlain("boom"), rec.apply)

	s.False(retried)
	s.Equal(datapb.CompactionTaskState_failed, rec.task.GetState())
}

type errPlain string

func (e errPlain) Error() string { return string(e) }

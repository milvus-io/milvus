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

package querynode

import (
	"context"
	"fmt"
	"time"

	"github.com/milvus-io/milvus/internal/log"
	"github.com/milvus-io/milvus/internal/proto/querypb"
	"github.com/milvus-io/milvus/internal/util/funcutil"
	"github.com/milvus-io/milvus/internal/util/timerecord"
	"github.com/milvus-io/milvus/internal/util/tsoutil"
	"go.uber.org/zap"
)

type readTask interface {
	task

	Ctx() context.Context

	GetTimeRecorder() *timerecord.TimeRecorder
	GetCollectionID() UniqueID

	Ready() (bool, error)
	Merge(readTask)
	CanMergeWith(readTask) bool
	CPUUsage() int32
	Timeout() bool

	SetMaxCPUUSage(int32)
	SetStep(step TaskStep)
}

var _ readTask = (*baseReadTask)(nil)

type baseReadTask struct {
	baseTask

	QS *queryShard

	DataScope          querypb.DataScope
	cpu                int32
	maxCPU             int32
	DbID               int64
	CollectionID       int64
	TravelTimestamp    uint64
	GuaranteeTimestamp uint64
	TimeoutTimestamp   uint64
	step               TaskStep
	queueDur           time.Duration
	reduceDur          time.Duration
	tr                 *timerecord.TimeRecorder
}

func (b *baseReadTask) SetStep(step TaskStep) {
	b.step = step
	switch step {
	case TaskStepEnqueue:
		b.queueDur = 0
		b.tr.Record("enqueueStart")
	case TaskStepPreExecute:
		b.queueDur = b.tr.Record("enqueueEnd")
	}
}

func (b *baseReadTask) OnEnqueue() error {
	b.SetStep(TaskStepEnqueue)
	return nil
}

func (b *baseReadTask) SetMaxCPUUSage(cpu int32) {
	b.maxCPU = cpu
}

func (b *baseReadTask) PreExecute(ctx context.Context) error {
	b.SetStep(TaskStepPreExecute)
	return nil
}

func (b *baseReadTask) Execute(ctx context.Context) error {
	b.SetStep(TaskStepExecute)
	return nil
}

func (b *baseReadTask) PostExecute(ctx context.Context) error {
	b.SetStep(TaskStepPostExecute)
	return nil
}

func (b *baseReadTask) Notify(err error) {
	switch b.step {
	case TaskStepEnqueue:
		b.queueDur = b.tr.Record("enqueueEnd")
	}
	b.baseTask.Notify(err)
}

// GetCollectionID return CollectionID.
func (b *baseReadTask) GetCollectionID() UniqueID {
	return b.CollectionID
}

func (b *baseReadTask) GetTimeRecorder() *timerecord.TimeRecorder {
	return b.tr
}

func (b *baseReadTask) CanMergeWith(t readTask) bool {
	return false
}

func (b *baseReadTask) Merge(t readTask) {
}

func (b *baseReadTask) CPUUsage() int32 {
	return 0
}

func (b *baseReadTask) Timeout() bool {
	return !funcutil.CheckCtxValid(b.Ctx())
}

func (b *baseReadTask) Ready() (bool, error) {
	if b.Timeout() {
		return false, fmt.Errorf("deadline exceed")
	}
	var err error
	var tType tsType
	if b.DataScope == querypb.DataScope_Streaming {
		tType = tsTypeDML
	} else if b.DataScope == querypb.DataScope_Historical {
		tType = tsTypeDelta
	}
	if err != nil {
		return false, err
	}

	if _, released := b.QS.collection.getReleaseTime(); released {
		log.Debug("collection release before search", zap.Int64("collectionID", b.CollectionID))
		return false, fmt.Errorf("collection has been released, taskID = %d, collectionID = %d", b.ID(), b.CollectionID)
	}

	serviceTime, err2 := b.QS.getServiceableTime(tType)
	if err2 != nil {
		return false, fmt.Errorf("failed to get service timestamp, taskID = %d, collectionID = %d, err=%w", b.ID(), b.CollectionID, err2)
	}
	guaranteeTs := b.GuaranteeTimestamp
	gt, _ := tsoutil.ParseTS(guaranteeTs)
	st, _ := tsoutil.ParseTS(serviceTime)
	if guaranteeTs > serviceTime {
		log.Debug("query msg can't do",
			zap.Any("collectionID", b.CollectionID),
			zap.Any("sm.GuaranteeTimestamp", gt),
			zap.Any("serviceTime", st),
			zap.Any("delta seconds", (guaranteeTs-serviceTime)/(1000*1000*1000)),
			zap.Any("msgID", b.ID()))
		return false, nil
	}
	return true, nil
}

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

package job

import (
	"context"
)

// Job is request of loading/releasing collection/partitions,
// the execution flow is:
// 1. PreExecute()
// 2. Execute(), skip this step if PreExecute() failed
// 3. PostExecute()
type Job interface {
	MsgID() int64
	CollectionID() int64
	Context() context.Context
	// PreExecute does checks, DO NOT persists any thing within this stage,
	PreExecute() error
	// Execute processes the request
	Execute() error
	// PostExecute clears resources, it will be always processed
	PostExecute()
	Error() error
	SetError(err error)
	Done()
	Wait() error
}

type BaseJob struct {
	ctx          context.Context
	msgID        int64
	collectionID int64
	err          error
	doneCh       chan struct{}
}

func NewBaseJob(ctx context.Context, msgID, collectionID int64) *BaseJob {
	return &BaseJob{
		ctx:          ctx,
		msgID:        msgID,
		collectionID: collectionID,
		doneCh:       make(chan struct{}),
	}
}

func (job *BaseJob) MsgID() int64 {
	return job.msgID
}

func (job *BaseJob) CollectionID() int64 {
	return job.collectionID
}

func (job *BaseJob) Context() context.Context {
	return job.ctx
}

func (job *BaseJob) Error() error {
	return job.err
}

func (job *BaseJob) SetError(err error) {
	job.err = err
}

func (job *BaseJob) Done() {
	close(job.doneCh)
}

func (job *BaseJob) Wait() error {
	<-job.doneCh
	return job.err
}

func (job *BaseJob) PreExecute() error {
	return nil
}

func (job *BaseJob) PostExecute() {}

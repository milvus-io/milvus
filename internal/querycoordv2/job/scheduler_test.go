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
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

type blockingSchedulerJob struct {
	*BaseJob
	started chan struct{}
	finish  chan struct{}
}

func (job *blockingSchedulerJob) Execute() error {
	close(job.started)
	<-job.finish
	return nil
}

func TestWaitCollectionIdleTracksTerminalJobs(t *testing.T) {
	scheduler := NewScheduler()
	scheduler.Start()
	defer scheduler.Stop()

	job := &blockingSchedulerJob{
		BaseJob: NewBaseJob(context.Background(), 1, 100),
		started: make(chan struct{}),
		finish:  make(chan struct{}),
	}
	scheduler.Add(job)
	select {
	case <-job.started:
	case <-time.After(5 * time.Second):
		t.Fatal("job did not start")
	}

	waitCtx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	require.ErrorIs(t, scheduler.WaitCollectionIdle(waitCtx, 100), context.DeadlineExceeded)
	require.NoError(t, scheduler.WaitCollectionIdle(context.Background(), 200))

	close(job.finish)
	require.NoError(t, scheduler.WaitCollectionIdle(context.Background(), 100))
	require.NoError(t, job.Wait())
}

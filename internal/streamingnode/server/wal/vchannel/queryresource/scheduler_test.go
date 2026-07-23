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

package queryresource

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/pkg/v3/util/nodescheduler"
)

func TestResourceBuildTaskStoresResultBeforeHandleCompletion(t *testing.T) {
	scheduler := nodescheduler.New(1)
	defer scheduler.Close()

	want := NewQueryRuntime(&recordingModule{})
	task := newResourceBuildTask(func(context.Context) (*QueryRuntime, error) {
		return want, nil
	})
	scheduled := scheduleResourceBuild(scheduler, task)

	got, err := scheduled.Result()
	require.NoError(t, err)
	require.Same(t, want, got)
}

func TestResourceBuildTaskCanceledBeforeExecutionCompletesResult(t *testing.T) {
	scheduler := nodescheduler.New(1)
	defer scheduler.Close()

	started := make(chan struct{})
	release := make(chan struct{})
	blocking := scheduler.Submit(queryResourceTaskFunc(func(context.Context) error {
		close(started)
		<-release
		return nil
	}))
	<-started

	task := newResourceBuildTask(func(context.Context) (*QueryRuntime, error) {
		t.Fatal("canceled build must not execute")
		return nil, nil
	})
	scheduled := scheduleResourceBuild(scheduler, task)
	scheduled.Cancel()
	close(release)
	require.NoError(t, blocking.Wait(context.Background()))
	_, err := scheduled.Result()
	require.ErrorIs(t, err, context.Canceled)
}

type queryResourceTaskFunc func(context.Context) error

func (f queryResourceTaskFunc) Execute(ctx context.Context) error {
	return f(ctx)
}

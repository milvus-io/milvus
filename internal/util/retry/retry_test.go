// Copyright (C) 2019-2020 Zilliz. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software distributed under the License
// is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
// or implied. See the License for the specific language governing permissions and limitations under the License.

package retry

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/lingdor/stackerror"
	"github.com/milvus-io/milvus-proto/go-api/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/milvuspb"
	"github.com/stretchr/testify/assert"
)

func TestDo(t *testing.T) {
	ctx := context.Background()

	n := 0
	testFn := func() error {
		if n < 3 {
			n++
			return fmt.Errorf("some error")
		}
		return nil
	}

	err := Do(ctx, testFn)
	assert.Nil(t, err)
}

func TestAttempts(t *testing.T) {
	ctx := context.Background()

	testFn := func() error {
		return fmt.Errorf("some error")
	}

	err := Do(ctx, testFn, Attempts(1))
	assert.NotNil(t, err)
	fmt.Println(err)
}

func TestMaxSleepTime(t *testing.T) {
	ctx := context.Background()

	testFn := func() error {
		return fmt.Errorf("some error")
	}

	err := Do(ctx, testFn, Attempts(3), MaxSleepTime(200*time.Millisecond))
	assert.NotNil(t, err)
	fmt.Println(err)
}

func TestSleep(t *testing.T) {
	ctx := context.Background()

	testFn := func() error {
		return fmt.Errorf("some error")
	}

	err := Do(ctx, testFn, Attempts(3), Sleep(500*time.Millisecond))
	assert.NotNil(t, err)
	fmt.Println(err)
}

func TestFnTimeout(t *testing.T) {
	ctx := context.Background()

	testFn := func() error {
		time.Sleep(time.Second * 2)
		return fmt.Errorf("some error")
	}

	err := Do(ctx, testFn, Attempts(3), FnTimeout(time.Second*1))
	assert.Error(t, err)
	fmt.Println(err)
}

func TestTotalTimeout(t *testing.T) {
	ctx := context.Background()

	testFn := func() error {
		time.Sleep(time.Second * 2)
		return fmt.Errorf("some error")
	}

	err := Do(ctx, testFn, Attempts(10), TotalTimeout(time.Second*10))
	assert.Error(t, err)
	fmt.Println(err)
}

func TestTotalTimeoutSuccess(t *testing.T) {
	ctx := context.Background()

	testFn := func() error {
		return nil
	}

	err := Do(ctx, testFn, Attempts(10), TotalTimeout(time.Second*10))
	assert.NoError(t, err)
}

func TestAllError(t *testing.T) {
	ctx := context.Background()

	testFn := func() error {
		return stackerror.New("some error")
	}

	err := Do(ctx, testFn, Attempts(3))
	assert.NotNil(t, err)
	fmt.Println(err)
}

func TestUnRecoveryError(t *testing.T) {
	attempts := 0
	ctx := context.Background()

	testFn := func() error {
		attempts++
		return Unrecoverable(fmt.Errorf("some error"))
	}

	err := Do(ctx, testFn, Attempts(3))
	assert.NotNil(t, err)
	assert.Equal(t, attempts, 1)
}

func TestContextDeadline(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel()

	testFn := func() error {
		return fmt.Errorf("some error")
	}

	err := Do(ctx, testFn)
	assert.NotNil(t, err)
	fmt.Println(err)
}

func TestContextCancel(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())

	testFn := func() error {
		return fmt.Errorf("some error")
	}

	go func() {
		time.Sleep(100 * time.Millisecond)
		cancel()
	}()

	err := Do(ctx, testFn)
	assert.NotNil(t, err)
	fmt.Println(err)
}

func TestDoGrpc(t *testing.T) {
	var (
		result any
		err    error
		times  uint = 5
		ctx         = context.Background()
	)

	_, err = DoGrpc(ctx, times, func() (any, error) {
		return nil, errors.New("foo")
	})
	assert.Error(t, err)

	successStatus := &commonpb.Status{ErrorCode: commonpb.ErrorCode_Success}
	result, err = DoGrpc(ctx, times, func() (any, error) {
		return successStatus, nil
	})
	assert.Equal(t, successStatus, result)
	assert.NoError(t, err)
	unExpectStatus := &commonpb.Status{ErrorCode: commonpb.ErrorCode_UnexpectedError}
	result, err = DoGrpc(ctx, times, func() (any, error) {
		return unExpectStatus, nil
	})
	assert.Equal(t, unExpectStatus, result)
	assert.NoError(t, err)
	notReadyStatus := &commonpb.Status{ErrorCode: commonpb.ErrorCode_NotReadyServe, Reason: "fo"}
	result, err = DoGrpc(ctx, times, func() (any, error) {
		return notReadyStatus, nil
	})
	assert.Equal(t, notReadyStatus, result)
	assert.NoError(t, err)

	showCollectionsResp := &milvuspb.ShowCollectionsResponse{Status: successStatus}
	result, err = DoGrpc(ctx, times, func() (any, error) {
		return showCollectionsResp, nil
	})
	assert.Equal(t, showCollectionsResp, result)
	assert.NoError(t, err)

	ctx2, cancel := context.WithCancel(ctx)
	testDone := make(chan struct{})
	timer := time.NewTimer(5 * time.Second)
	defer timer.Stop()
	go func() {
		time.Sleep(time.Second)
		cancel()
	}()
	go func() {
		result, err = DoGrpc(ctx2, 100, func() (any, error) {
			return notReadyStatus, nil
		})
		assert.Equal(t, notReadyStatus, result)
		assert.NoError(t, err)
		testDone <- struct{}{}
	}()
	select {
	case <-testDone:
		return
	case <-timer.C:
		t.FailNow()
	}
}

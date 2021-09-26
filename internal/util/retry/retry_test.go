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
	"fmt"
	"testing"
	"time"

	"github.com/lingdor/stackerror"
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

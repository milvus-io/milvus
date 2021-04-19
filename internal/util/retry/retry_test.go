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
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestImpl(t *testing.T) {
	attempts := 10
	sleep := time.Millisecond * 1
	maxSleepTime := time.Millisecond * 16

	var err error

	naiveF := func() error {
		return nil
	}
	err = Impl(attempts, sleep, naiveF, maxSleepTime)
	assert.Equal(t, err, nil)

	errorF := func() error {
		return errors.New("errorF")
	}
	err = Impl(attempts, sleep, errorF, maxSleepTime)
	assert.NotEqual(t, err, nil)

	begin := 0
	stop := 2
	interruptF := func() error {
		if begin >= stop {
			return NoRetryError(errors.New("interrupt here"))
		}
		begin++
		return errors.New("interruptF")
	}
	err = Impl(attempts, sleep, interruptF, maxSleepTime)
	assert.NotEqual(t, err, nil)

	begin = 0
	stop = attempts / 2
	untilSucceedF := func() error {
		if begin >= stop {
			return nil
		}
		begin++
		return errors.New("untilSucceedF")
	}
	err = Impl(attempts, sleep, untilSucceedF, maxSleepTime)
	assert.Equal(t, err, nil)

	begin = 0
	stop = attempts * 2
	noRetryF := func() error {
		if begin >= stop {
			return nil
		}
		begin++
		return errors.New("noRetryF")
	}
	err = Impl(attempts, sleep, noRetryF, maxSleepTime)
	assert.NotEqual(t, err, nil)
}

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

package funcutil

import (
	"errors"
	"math/rand"
	"runtime"
	"strconv"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
)

func dummyFunc() {
}

func Test_GetFunctionName(t *testing.T) {
	name := GetFunctionName(dummyFunc)
	assert.True(t, strings.Contains(name, "dummyFunc"))
}

func Test_ProcessFuncParallel(t *testing.T) {
	total := 64
	s := make([]int, total)

	expectedS := make([]int, total)
	for i := range expectedS {
		expectedS[i] = i
	}

	naiveF := func(idx int) error {
		s[idx] = idx
		return nil
	}

	var err error

	err = ProcessFuncParallel(total, 0, naiveF, "naiveF") // maxParallel = 0
	assert.Equal(t, err, nil)

	err = ProcessFuncParallel(0, 1, naiveF, "naiveF") // total = 0
	assert.Equal(t, err, nil)

	err = ProcessFuncParallel(total, 1, naiveF, "naiveF") // serial
	assert.Equal(t, err, nil, "process function serially must be right")
	assert.Equal(t, s, expectedS, "process function serially must be right")

	err = ProcessFuncParallel(total, total, naiveF, "naiveF") // Totally Parallel
	assert.Equal(t, err, nil, "process function parallel must be right")
	assert.Equal(t, s, expectedS, "process function parallel must be right")

	err = ProcessFuncParallel(total, runtime.NumCPU(), naiveF, "naiveF") // Parallel by CPU
	assert.Equal(t, err, nil, "process function parallel must be right")
	assert.Equal(t, s, expectedS, "process function parallel must be right")

	oddErrorF := func(idx int) error {
		if idx%2 == 1 {
			return errors.New("odd location: " + strconv.Itoa(idx))
		}
		return nil
	}

	err = ProcessFuncParallel(total, 1, oddErrorF, "oddErrorF") // serial
	assert.NotEqual(t, err, nil, "process function serially must be right")

	err = ProcessFuncParallel(total, total, oddErrorF, "oddErrorF") // Totally Parallel
	assert.NotEqual(t, err, nil, "process function parallel must be right")

	err = ProcessFuncParallel(total, runtime.NumCPU(), oddErrorF, "oddErrorF") // Parallel by CPU
	assert.NotEqual(t, err, nil, "process function parallel must be right")

	evenErrorF := func(idx int) error {
		if idx%2 == 0 {
			return errors.New("even location: " + strconv.Itoa(idx))
		}
		return nil
	}

	err = ProcessFuncParallel(total, 1, evenErrorF, "evenErrorF") // serial
	assert.NotEqual(t, err, nil, "process function serially must be right")

	err = ProcessFuncParallel(total, total, evenErrorF, "evenErrorF") // Totally Parallel
	assert.NotEqual(t, err, nil, "process function parallel must be right")

	err = ProcessFuncParallel(total, runtime.NumCPU(), evenErrorF, "evenErrorF") // Parallel by CPU
	assert.NotEqual(t, err, nil, "process function parallel must be right")

	// rand.Int() may be always a even number
	randomErrorF := func(idx int) error {
		if rand.Int()%2 == 0 {
			return errors.New("random location: " + strconv.Itoa(idx))
		}
		return nil
	}

	ProcessFuncParallel(total, 1, randomErrorF, "randomErrorF") // serial

	ProcessFuncParallel(total, total, randomErrorF, "randomErrorF") // Totally Parallel

	ProcessFuncParallel(total, runtime.NumCPU(), randomErrorF, "randomErrorF") // Parallel by CPU
}

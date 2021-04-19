package funcutil

import (
	"errors"
	"math/rand"
	"runtime"
	"strconv"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestProcessFuncParallel(t *testing.T) {
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

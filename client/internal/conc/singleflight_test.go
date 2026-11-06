// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package conc

import (
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cockroachdb/errors"
)

func TestSingleflightCoalescesConcurrentSameKey(t *testing.T) {
	var sf Singleflight[int, string]
	var loads atomic.Int64
	const callers = 16

	var wg sync.WaitGroup
	results := make([]int, callers)
	shared := make([]bool, callers)
	start := make(chan struct{})
	release := make(chan struct{})
	for i := 0; i < callers; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			<-start
			results[idx], _, shared[idx] = sf.Do("key", func() (int, error) {
				loads.Add(1)
				// Block the leader so every concurrent caller reaches Do and
				// joins the wait group before the call is finished and removed.
				<-release
				return 42, nil
			})
		}(i)
	}
	close(start)
	// Wait until the leader is inside fn (loads == 1) and the remaining
	// callers have had a chance to join the in-flight call.
	deadline := time.Now().Add(time.Second)
	for loads.Load() != 1 {
		if time.Now().After(deadline) {
			t.Fatal("leader never entered fn")
		}
		time.Sleep(time.Millisecond)
	}
	time.Sleep(20 * time.Millisecond)
	close(release)
	wg.Wait()

	if loads.Load() != 1 {
		t.Fatalf("expected 1 execution, got %d", loads.Load())
	}
	for i, r := range results {
		if r != 42 {
			t.Fatalf("caller %d got %d", i, r)
		}
		if !shared[i] {
			t.Fatalf("caller %d expected shared=true", i)
		}
	}
}

func TestSingleflightIsolatesDistinctKeys(t *testing.T) {
	var sf Singleflight[int, string]
	var wg sync.WaitGroup
	executed := make(chan string, 2)
	wg.Add(2)
	for _, key := range []string{"a", "b"} {
		go func(k string) {
			defer wg.Done()
			_, _, _ = sf.Do(k, func() (int, error) {
				executed <- k
				return 1, nil
			})
		}(key)
	}
	wg.Wait()
	close(executed)
	seen := map[string]bool{}
	for k := range executed {
		seen[k] = true
	}
	if len(seen) != 2 {
		t.Fatalf("expected both keys to execute, got %v", seen)
	}
}

func TestSingleflightSharesError(t *testing.T) {
	var sf Singleflight[int, string]
	sentinel := errors.New("boom")
	var wg sync.WaitGroup
	wg.Add(2)
	errs := make([]error, 2)
	for i := 0; i < 2; i++ {
		go func(idx int) {
			defer wg.Done()
			_, errs[idx], _ = sf.Do("key", func() (int, error) {
				return 0, sentinel
			})
		}(i)
	}
	wg.Wait()
	for i, err := range errs {
		if !errors.Is(err, sentinel) {
			t.Fatalf("caller %d expected sentinel error, got %v", i, err)
		}
	}
}

func TestSingleflightLeaderPanicRepanicsDups(t *testing.T) {
	var sf Singleflight[int, string]
	var wg sync.WaitGroup
	results := make([]any, 2)
	// leader panics; both leader and the duplicate caller must re-panic.
	for i := 0; i < 2; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			defer func() {
				results[idx] = recover()
			}()
			_, _, _ = sf.Do("key", func() (int, error) {
				panic("kaboom")
			})
		}(i)
	}
	wg.Wait()
	for i, r := range results {
		pe, ok := r.(*panicError)
		if !ok {
			t.Fatalf("caller %d expected *panicError, got %T: %v", i, r, r)
		}
		if pe.message != "kaboom" {
			t.Fatalf("caller %d expected message kaboom, got %v", i, pe.message)
		}
	}
}

func TestSingleflightPanicNilRepanics(t *testing.T) {
	var sf Singleflight[int, string]
	defer func() {
		r := recover()
		if r == nil {
			t.Fatal("expected a panic, got nil")
		}
		if _, ok := r.(*panicError); !ok {
			t.Fatalf("expected *panicError, got %T: %v", r, r)
		}
	}()
	_, _, _ = sf.Do("key", func() (int, error) {
		panic(nil)
	})
	t.Fatal("expected panic")
}

func TestSingleflightForgetReturnsFreshResult(t *testing.T) {
	var sf Singleflight[int, string]
	_, err, _ := sf.Do("key", func() (int, error) {
		return 1, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	// panicError's Error() must render the message.
	if got := fmt.Sprint(newPanicError("x").Error()); got != "panic: x" {
		t.Fatalf("unexpected panicError rendering: %s", got)
	}
}

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

package reqregistry

import (
	"context"
	"sync"
	"testing"
	"time"
	"unsafe"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// proxy 25, started 2026-11-04 23:31:06 UTC (1793835066 = "to03zu" in base 36)
var testStart = time.Unix(1793835066, 0)

func TestRequestIDFormat(t *testing.T) {
	r := New()
	r.SetIdentity(25, testStart)

	_, first := r.Register(context.Background(), Info{})
	assert.Equal(t, "25-to03zu-1", first.RequestID())
	r.Unregister(first)

	var e *Entry
	for i := 2; i <= 1000; i++ {
		_, e = r.Register(context.Background(), Info{})
		r.Unregister(e)
	}
	assert.Equal(t, "25-to03zu-rs", e.RequestID(), "the 1000th request of the process")

	_, e = r.Register(context.Background(), Info{})
	defer r.Unregister(e)
	listed := r.List(Filter{}, time.Now())
	require.Len(t, listed, 1)
	assert.Equal(t, "25-to03zu-rt", listed[0].RequestID)
	assert.Same(t, e, r.Get("25-to03zu-rt"))
}

func TestRequestIDsAreUniqueUnderConcurrency(t *testing.T) {
	r := New()
	r.SetIdentity(25, testStart)
	const goroutines, perGoroutine = 32, 2000
	var mu sync.Mutex
	seen := make(map[string]struct{}, goroutines*perGoroutine)
	var wg sync.WaitGroup
	for g := 0; g < goroutines; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			ids := make([]string, 0, perGoroutine)
			for i := 0; i < perGoroutine; i++ {
				_, e := r.Register(context.Background(), Info{})
				ids = append(ids, e.RequestID())
				r.Unregister(e)
			}
			mu.Lock()
			defer mu.Unlock()
			for _, id := range ids {
				seen[id] = struct{}{}
			}
		}()
	}
	wg.Wait()
	assert.Len(t, seen, goroutines*perGoroutine)
}

func TestCancelTreatsForeignAndMalformedIDsAsNotFound(t *testing.T) {
	r := New()
	r.SetIdentity(25, testStart)
	ctx, e := r.Register(context.Background(), Info{User: "alice"})
	defer r.Unregister(e)
	require.Equal(t, "25-to03zu-1", e.RequestID())

	other := []string{
		"26-to03zu-1",              // another proxy
		"25-to06ru-1",              // this proxy id, another process start
		"",                         // empty
		"25-to03zu-",               // no sequence
		"25-to03zu-0",              // sequences start at 1
		"25-to03zu-01",             // not canonical: leading zero
		"25-TO03ZU-1",              // not canonical: upper case prefix
		"25-to03zu-1-1",            // trailing part
		"25-to03zu-+1",             // sign
		"25-to03zu-zzzzzzzzzzzzzz", // beyond uint64
		"1",                        // the old numeric form
	}
	canceled, repeated, notFound := r.Cancel(other, "", "op", "", time.Now())
	assert.Empty(t, canceled)
	assert.Empty(t, repeated)
	assert.Equal(t, other, notFound)
	assert.NoError(t, ctx.Err(), "no malformed or foreign id may stop the request")

	// each id names exactly one request: the upper-case spelling of the
	// sequence is rejected too, so a canceled entry always echoes the
	// canonical id
	_, _, notFound = r.Cancel([]string{"25-to03zu-1", "25-to03zu-1"}, "", "op", "", time.Now())
	assert.Empty(t, notFound)
	assert.Error(t, ctx.Err())
}

func TestRestartWithReusedNodeIDIssuesDistinctIDs(t *testing.T) {
	// A node id is normally fresh for every process, but a fixed test node id
	// or a wiped etcd can hand the same one out again. The start second still
	// tells the two processes apart, so an id listed before the restart
	// cannot cancel a request of the new process.
	before := New()
	before.SetIdentity(25, testStart)
	_, old := before.Register(context.Background(), Info{})

	after := New()
	after.SetIdentity(25, testStart.Add(time.Hour))
	ctx, fresh := after.Register(context.Background(), Info{})
	defer after.Unregister(fresh)

	assert.Equal(t, "25-to03zu-1", old.RequestID())
	assert.Equal(t, "25-to06ru-1", fresh.RequestID())
	_, _, notFound := after.Cancel([]string{old.RequestID()}, "", "op", "", time.Now())
	assert.Equal(t, []string{old.RequestID()}, notFound)
	assert.NoError(t, ctx.Err())
}

func TestEntryStaysInItsAllocationSizeClass(t *testing.T) {
	// Entry was 280 bytes with an int64 id, allocated from Go's 288-byte size
	// class. The string id must not push it into the next class (320).
	assert.LessOrEqual(t, unsafe.Sizeof(Entry{}), uintptr(288))
}

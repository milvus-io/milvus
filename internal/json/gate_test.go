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

package json

import (
	"bytes"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSonicAPIs(t *testing.T) {
	type nested struct {
		V int `json:"v"`
	}
	type obj struct {
		A string  `json:"a"`
		B int     `json:"b"`
		N *nested `json:"n,omitempty"`
	}

	o := obj{A: "x", B: 1, N: &nested{V: 2}}
	data, err := Marshal(o)
	require.NoError(t, err)

	var got obj
	require.NoError(t, Unmarshal(data, &got))
	assert.Equal(t, o, got)

	indented, err := MarshalIndent(o, "", "  ")
	require.NoError(t, err)
	assert.Contains(t, string(indented), "\n")

	require.True(t, Valid(data))
	require.False(t, Valid([]byte("{not json")))
}

func TestDecoderEncoder(t *testing.T) {
	type obj struct {
		A string  `json:"a"`
		B int     `json:"b"`
		C float64 `json:"c"`
	}

	input := `{"a":"x","b":1,"c":2.5}`

	dec := NewDecoder(bytes.NewBufferString(input))
	dec.UseNumber()
	var got obj
	require.NoError(t, dec.Decode(&got))
	assert.Equal(t, obj{A: "x", B: 1, C: 2.5}, got)

	var buf bytes.Buffer
	enc := NewEncoder(&buf)
	require.NoError(t, enc.Encode(obj{A: "y", B: 2, C: 3.5}))
	var decoded obj
	require.NoError(t, Unmarshal(buf.Bytes(), &decoded))
	assert.Equal(t, obj{A: "y", B: 2, C: 3.5}, decoded)
}

func TestBlockForPluginLoad_WaitsForInflightReader(t *testing.T) {
	acquireRead()

	blocked := make(chan struct{})
	go func() {
		BlockForPluginLoad()
		close(blocked)
	}()

	select {
	case <-blocked:
		t.Fatal("BlockForPluginLoad returned while a reader was still in flight")
	case <-time.After(50 * time.Millisecond):
	}

	releaseRead()
	select {
	case <-blocked:
	case <-time.After(time.Second):
		t.Fatal("BlockForPluginLoad did not return after the reader released")
	}

	UnblockForPluginLoad()
}

func TestBlockForPluginLoad_BlocksNewReaders(t *testing.T) {
	BlockForPluginLoad()

	readerDone := make(chan struct{})
	go func() {
		acquireRead()
		releaseRead()
		close(readerDone)
	}()

	select {
	case <-readerDone:
		t.Fatal("reader acquired while plugin load was holding the exclusive side")
	case <-time.After(50 * time.Millisecond):
	}

	UnblockForPluginLoad()
	select {
	case <-readerDone:
	case <-time.After(time.Second):
		t.Fatal("reader did not proceed after the plugin load released")
	}
}

func TestConcurrentReadersAllowed(t *testing.T) {
	BlockForPluginLoad()
	UnblockForPluginLoad()

	var wg sync.WaitGroup
	start := make(chan struct{})
	for i := 0; i < 16; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			for j := 0; j < 100; j++ {
				b, err := Marshal(map[string]int{"k": j})
				if err != nil {
					t.Error(err)
					return
				}
				if err := Unmarshal(b, new(map[string]int)); err != nil {
					t.Error(err)
					return
				}
			}
		}()
	}
	close(start)
	wg.Wait()
}

// TestGateWriterTogglesWithReaders stresses the handshake between concurrent
// readers and repeatedly toggling writers. It guards against deadlocks and
// missed wakeups (a reader's fast-path undo racing a writer's drain).
func TestGateWriterTogglesWithReaders(t *testing.T) {
	var wg sync.WaitGroup
	start := make(chan struct{})

	for i := 0; i < 4; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			for j := 0; j < 2000; j++ {
				acquireRead()
				b, err := json.Marshal(map[string]int{"k": j})
				if err != nil {
					t.Error(err)
					releaseRead()
					return
				}
				if err := json.Unmarshal(b, new(map[string]int)); err != nil {
					t.Error(err)
					releaseRead()
					return
				}
				releaseRead()
			}
		}()
	}

	wg.Add(1)
	go func() {
		defer wg.Done()
		<-start
		for j := 0; j < 500; j++ {
			BlockForPluginLoad()
			UnblockForPluginLoad()
		}
	}()

	close(start)
	wg.Wait()
}

// TestDisableGateSkipsFastPath verifies that once the gate is disabled a reader
// ignores the writer side entirely (steady state: zero blocking).
func TestDisableGateSkipsFastPath(t *testing.T) {
	DisableGate()
	defer func() { gateDisabled.Store(false) }()

	// Simulate a writer holding the exclusive side.
	gateMu.Lock()
	writeHeld.Store(true)
	gateMu.Unlock()

	acquireRead() // must return immediately even though a writer is active

	gateMu.Lock()
	writeHeld.Store(false)
	gateMu.Unlock()

	assert.Equal(t, int64(0), reading.Load(), "disabled gate must not touch the reader counter")
}

// TestBlockForPluginLoadRearmsAfterDisable verifies that a plugin load after
// DisableGate re-arms the gate so readers block again.
func TestBlockForPluginLoadRearmsAfterDisable(t *testing.T) {
	DisableGate()
	BlockForPluginLoad() // re-arms
	defer UnblockForPluginLoad()

	readerDone := make(chan struct{})
	go func() {
		acquireRead()
		releaseRead()
		close(readerDone)
	}()

	select {
	case <-readerDone:
		t.Fatal("reader acquired while a re-armed plugin load was holding the exclusive side")
	case <-time.After(50 * time.Millisecond):
	}
}

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

package storage

import (
	"runtime"
	"testing"
	"unsafe"

	"github.com/apache/arrow/go/v17/arrow"
	"github.com/apache/arrow/go/v17/arrow/array"
	"github.com/apache/arrow/go/v17/arrow/cdata"
	"github.com/apache/arrow/go/v17/arrow/memory"
)

func TestIssue52233ImportAllNullStringWithSentinelValuesPointer(t *testing.T) {
	const rowCount = 1000

	builder := array.NewStringBuilder(memory.DefaultAllocator)
	defer builder.Release()
	builder.AppendNulls(rowCount)
	source := builder.NewStringArray()
	defer source.Release()

	var exported cdata.CArrowArray
	cdata.ExportArrowArray(source, &exported, nil)
	defer cdata.ReleaseCArrowArray(&exported)

	// ArrowArray is part of the stable C Data Interface ABI. Treat the buffer
	// entries as uintptr values so the Go runtime never treats the 0x1 sentinel
	// as a Go pointer while constructing the fixture.
	type arrowArrayLayout struct {
		length    int64
		nullCount int64
		offset    int64
		nBuffers  int64
		nChildren int64
		buffers   unsafe.Pointer
	}
	layout := (*arrowArrayLayout)(unsafe.Pointer(&exported))
	if layout.nBuffers != 3 {
		t.Fatalf("C ArrowArray buffer count = %d, want 3", layout.nBuffers)
	}
	buffers := unsafe.Slice((*uintptr)(layout.buffers), int(layout.nBuffers))
	buffers[2] = 1

	imported, err := cdata.ImportCArrayWithType(&exported, arrow.BinaryTypes.String)
	if err != nil {
		t.Fatal(err)
	}
	defer imported.Release()

	values := imported.Data().Buffers()[2]
	if values == nil {
		t.Fatal("values buffer is nil")
	}
	bytes := values.Bytes()
	if got := len(bytes); got != 0 {
		t.Fatalf("values buffer length = %d, want 0", got)
	}
	if got := imported.Len(); got != rowCount {
		t.Fatalf("array length = %d, want %d", got, rowCount)
	}
	if got := imported.NullN(); got != rowCount {
		t.Fatalf("array null count = %d, want %d", got, rowCount)
	}

	var growStack func(int) int
	growStack = func(depth int) int {
		var padding [1024]byte
		padding[0] = byte(depth)
		if depth == 0 {
			return int(padding[0])
		}
		return int(padding[0]) + growStack(depth-1)
	}
	_ = growStack(128)
	runtime.KeepAlive(bytes)
}

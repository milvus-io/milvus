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

package packed

/*
#cgo pkg-config: milvus_core milvus-storage
#include <stdlib.h>
#include "milvus-storage/ffi_talon_c.h"
extern void milvusTalonDone(uintptr_t, LoonFFIResult, LoonTalonResult*);
static inline int64_t milvusTalonBytesRead(const LoonTalonResult* r) { return r->data.bytes_read; }
static inline uint64_t milvusTalonStatSize(const LoonTalonResult* r) { return r->data.stat.size; }
static inline const char* milvusTalonStatVersion(const LoonTalonResult* r) { return r->data.stat.version; }
static inline bool milvusTalonLoadFailed(const LoonTalonResult* r) { return r->data.load.failed; }
static inline size_t milvusTalonLoadCount(const LoonTalonResult* r) { return r->data.load.count; }
static inline const LoonTalonLoadFailure* milvusTalonLoadFailures(const LoonTalonResult* r) { return r->data.load.entries.failures; }
static inline const LoonTalonLoadResult* milvusTalonLoadResults(const LoonTalonResult* r) { return r->data.load.entries.results; }
*/
import "C"

import (
	"context"
	"runtime"
	"runtime/cgo"
	"strconv"
	"sync"
	"unsafe"

	"github.com/milvus-io/milvus/pkg/v3/proto/indexpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

// URI schemes understood by the raw Talon client. Azure uses "az" in the
// Talon protocol, independently of the cloud provider name.
const (
	TalonSchemeS3    = "s3"
	TalonSchemeAzure = "az"
)

// TalonReadRequest describes an object range. A nil ObjectSize asks Talon to
// resolve metadata; a pointer to zero means a known empty object. A nonempty
// Version requires ObjectSize and must identify the same object revision.
type TalonReadRequest struct {
	URI        string
	Offset     uint64
	Length     uint64
	ObjectSize *uint64
	Version    string
}

// TalonLoadRequest describes the same immutable object and cache identity used
// by a normal Talon reader. Size is mandatory; zero denotes an empty object.
type TalonLoadRequest struct {
	URI     string
	Size    uint64
	Version string // Empty skips version matching.
}

// TalonLoadResult reports the object extent and number of blocks covered by
// the load, including blocks already cached; these are not download counters.
type TalonLoadResult struct {
	Size   uint64
	Blocks uint64
}

// TalonLoadFailure retains the native request index and complete diagnostic.
type TalonLoadFailure struct {
	Index      uint64
	Uncertain  bool
	Diagnostic string
}

// TalonBatchLoadError preserves partial/uncertain outcomes and native status.
type TalonBatchLoadError struct {
	Failures []TalonLoadFailure
	Err      error
}

func (e *TalonBatchLoadError) Error() string { return e.Err.Error() }
func (e *TalonBatchLoadError) Unwrap() error { return e.Err }

// TalonClient owns a raw Talon handle and a caller-owned executor. Operations
// never fall back to origin storage. Close drains accepted callbacks.
type TalonClient struct {
	io        *ManifestIOContext
	native    C.LoonTalonClientHandle
	closeOnce sync.Once
}

func NewTalonClient(config *indexpb.StorageConfig, concurrency int) (*TalonClient, error) {
	if config == nil || config.GetTalonCoordinator() == "" || concurrency <= 0 {
		return nil, merr.WrapErrServiceInternalMsg("Talon client requires coordinator and positive concurrency")
	}
	// Raw Talon needs no filesystem, credentials, or filesystem-routing mode.
	keys := []string{"fs.talon.coordinator"}
	values := []string{config.GetTalonCoordinator()}
	for _, property := range []struct {
		key   string
		value uint32
	}{
		{"fs.talon.block_size", config.GetTalonBlockSize()},
		{"fs.talon.max_idle_per_addr", config.GetTalonMaxIdlePerAddr()},
	} {
		if property.value > 0 {
			keys = append(keys, property.key)
			values = append(values, strconv.FormatUint(uint64(property.value), 10))
		}
	}
	cKeys, cValues := make([]*C.char, len(keys)), make([]*C.char, len(keys))
	for i := range keys {
		cKeys[i], cValues[i] = C.CString(keys[i]), C.CString(values[i])
		defer C.free(unsafe.Pointer(cKeys[i]))
		defer C.free(unsafe.Pointer(cValues[i]))
	}
	var properties C.LoonProperties
	if err := HandleLoonFFIResult(C.loon_properties_create(&cKeys[0], &cValues[0], C.size_t(len(keys)), &properties)); err != nil {
		return nil, merr.Wrap(err, "create raw Talon properties")
	}
	defer C.loon_properties_free(&properties)
	client := &TalonClient{io: NewManifestIOContext(concurrency)}
	if err := HandleLoonFFIResult(C.loon_talon_client_create(&properties, &client.native)); err != nil {
		return nil, merr.Wrap(err, "create raw Talon client")
	}
	return client, nil
}

type talonResult struct {
	bytesRead int64
	data      []byte
	size      uint64
	version   string
	loads     []TalonLoadResult
	err       error
}

func takeTalonResult(status C.LoonFFIResult, payload *C.LoonTalonResult) (result talonResult) {
	defer C.loon_talon_result_free(payload)
	// Do not allow malformed native payload conversion to panic across cgo.
	defer func() {
		if value := recover(); value != nil {
			result.err = merr.WrapErrServiceInternalMsg("convert Talon result: %v", value)
		}
	}()
	result.err = handleManifestAsyncResult(status) // Also frees status on failure.
	if payload == nil {
		if result.err == nil {
			result.err = merr.WrapErrServiceInternalMsg("missing Talon result payload")
		}
		return
	}
	switch payload.operation {
	case C.LOON_TALON_READ:
		result.bytesRead = int64(C.milvusTalonBytesRead(payload))
	case C.LOON_TALON_STAT:
		result.size = uint64(C.milvusTalonStatSize(payload))
		result.version = C.GoString(C.milvusTalonStatVersion(payload))
	case C.LOON_TALON_LOAD, C.LOON_TALON_BATCH_LOAD:
		if C.milvusTalonLoadFailed(payload) {
			failures := unsafe.Slice(C.milvusTalonLoadFailures(payload), int(C.milvusTalonLoadCount(payload)))
			partial := &TalonBatchLoadError{Err: result.err, Failures: make([]TalonLoadFailure, len(failures))}
			if partial.Err == nil {
				partial.Err = merr.WrapErrServiceInternalMsg("Talon batch failed without native error")
			}
			for i, failure := range failures {
				partial.Failures[i] = TalonLoadFailure{uint64(failure.index), bool(failure.uncertain), C.GoString(failure.error)}
			}
			result.err = partial
		} else if result.err == nil {
			loads := unsafe.Slice(C.milvusTalonLoadResults(payload), int(C.milvusTalonLoadCount(payload)))
			result.loads = make([]TalonLoadResult, len(loads))
			for i, load := range loads {
				result.loads[i] = TalonLoadResult{Size: uint64(load.size), Blocks: uint64(load.blocks)}
			}
		}
	default:
		if result.err == nil {
			result.err = merr.WrapErrServiceInternalMsg("unexpected Talon result operation %d", payload.operation)
		}
	}
	return
}

//export milvusTalonDone
func milvusTalonDone(token C.uintptr_t, status C.LoonFFIResult, payload *C.LoonTalonResult) {
	h := cgo.Handle(token)
	complete := h.Value().(func(talonResult))
	h.Delete()
	complete(takeTalonResult(status, payload))
}

type talonRequest struct {
	operation C.LoonTalonOperation
	uri       string
	read      *TalonReadRequest
	loads     []TalonLoadRequest
}

// submit holds admission until the actual callback, including after a waiter
// cancels. The mutex excludes AfterFunc cancellation from handle publication and
// release; rejected admissions have no callback and delete their own token.
func (c *TalonClient) submit(ctx context.Context, request talonRequest) (<-chan talonResult, error) {
	if err := c.io.acquire(ctx); err != nil {
		return nil, err
	}
	accepted := false
	var pinned runtime.Pinner
	defer func() {
		if !accepted {
			pinned.Unpin()
			c.io.release()
		}
	}()
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	var cURI *C.char
	var cVersion *C.char
	var objectSize C.uint64_t
	var cObjectSize *C.uint64_t
	var data []byte
	var dst *C.uint8_t
	var entries []C.LoonTalonLoadRequest
	if request.operation == C.LOON_TALON_LOAD || request.operation == C.LOON_TALON_BATCH_LOAD {
		entries = make([]C.LoonTalonLoadRequest, len(request.loads))
		for i, load := range request.loads {
			entries[i].uri, entries[i].version = C.CString(load.URI), C.CString(load.Version)
			entries[i].size = C.uint64_t(load.Size)
			defer C.free(unsafe.Pointer(entries[i].uri))
			defer C.free(unsafe.Pointer(entries[i].version))
		}
	} else {
		cURI = C.CString(request.uri)
		defer C.free(unsafe.Pointer(cURI))
	}
	if request.operation == C.LOON_TALON_READ {
		data = make([]byte, int(request.read.Length))
		if len(data) > 0 {
			// Native I/O may outlive the Go waiter. Own and pin this buffer until
			// its actual callback; never expose it to a canceled caller.
			pinned.Pin(&data[0])
			dst = (*C.uint8_t)(unsafe.Pointer(&data[0]))
		}
		cVersion = C.CString(request.read.Version)
		defer C.free(unsafe.Pointer(cVersion))
		if request.read.ObjectSize != nil {
			objectSize = C.uint64_t(*request.read.ObjectSize)
			cObjectSize = &objectSize
		}
	}
	done := make(chan talonResult, 1)
	var mu sync.Mutex
	operation := manifestAsyncHandle(ctx)
	var stopCancel func() bool
	token := cgo.NewHandle(func(result talonResult) {
		defer c.io.release()
		defer pinned.Unpin()
		mu.Lock()
		stopCancel()
		C.loon_async_release(&operation)
		mu.Unlock()
		if request.operation == C.LOON_TALON_READ && result.err == nil {
			if result.bytesRead < 0 || uint64(result.bytesRead) > uint64(len(data)) {
				result.err = merr.WrapErrServiceInternalMsg("Talon returned invalid read length %d for buffer size %d", result.bytesRead, len(data))
			} else {
				result.data = data[:int(result.bytesRead)]
			}
		}
		done <- result
	})
	mu.Lock()
	var status C.LoonFFIResult
	switch request.operation {
	case C.LOON_TALON_READ:
		status = C.loon_talon_read_async(c.io.native, c.native, cURI, C.uint64_t(request.read.Offset), C.uint64_t(request.read.Length), cVersion, cObjectSize, dst, (C.LoonTalonCallback)(C.milvusTalonDone), C.uintptr_t(token), &operation)
	case C.LOON_TALON_LOAD:
		status = C.loon_talon_load_async(c.io.native, c.native, &entries[0], (C.LoonTalonCallback)(C.milvusTalonDone), C.uintptr_t(token), &operation)
	case C.LOON_TALON_BATCH_LOAD:
		status = C.loon_talon_batch_load_async(c.io.native, c.native, &entries[0], C.size_t(len(entries)), (C.LoonTalonCallback)(C.milvusTalonDone), C.uintptr_t(token), &operation)
	case C.LOON_TALON_STAT:
		status = C.loon_talon_stat_async(c.io.native, c.native, cURI, (C.LoonTalonCallback)(C.milvusTalonDone), C.uintptr_t(token), &operation)
	}
	if err := handleManifestAsyncResult(status); err != nil {
		mu.Unlock()
		token.Delete()
		return nil, merr.Wrap(err, "submit raw Talon operation")
	}
	stopCancel = context.AfterFunc(ctx, func() { mu.Lock(); C.loon_async_cancel(&operation); mu.Unlock() })
	accepted = true
	mu.Unlock()
	return done, nil
}

// Read allocates a private Length-byte buffer and returns up to Length bytes,
// with a short successful result at EOF.
// Cancellation stops waiting; accepted native work retains its private buffer
// until completion. No bytes are returned on error, and no origin fallback runs.
func (c *TalonClient) Read(ctx context.Context, request TalonReadRequest) ([]byte, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if request.Length > uint64(^uint(0)>>1) {
		return nil, merr.Wrapf(ErrLoonPermanent, "Talon read length exceeds Go buffer capacity: %d", request.Length)
	}
	done, err := c.submit(ctx, talonRequest{operation: C.LOON_TALON_READ, uri: request.URI, read: &request})
	if err != nil {
		return nil, err
	}
	select {
	case result := <-done:
		return result.data, result.err
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func (c *TalonClient) Stat(ctx context.Context, uri string) (uint64, string, error) {
	done, err := c.submit(ctx, talonRequest{operation: C.LOON_TALON_STAT, uri: uri})
	if err != nil {
		return 0, "", err
	}
	select {
	case result := <-done:
		return result.size, result.version, result.err
	case <-ctx.Done():
		return 0, "", ctx.Err()
	}
}

// Load prewarms one object and returns its size and block count.
func (c *TalonClient) Load(ctx context.Context, request TalonLoadRequest) (TalonLoadResult, error) {
	done, err := c.submit(ctx, talonRequest{operation: C.LOON_TALON_LOAD, loads: []TalonLoadRequest{request}})
	if err != nil {
		return TalonLoadResult{}, err
	}
	select {
	case result := <-done:
		if result.err != nil {
			return TalonLoadResult{}, result.err
		}
		if len(result.loads) != 1 {
			return TalonLoadResult{}, merr.WrapErrServiceInternalMsg("Talon load returned %d results, expected 1", len(result.loads))
		}
		return result.loads[0], nil
	case <-ctx.Done():
		return TalonLoadResult{}, ctx.Err()
	}
}

// BatchLoad returns successful results in request order. On partial failure it
// returns no success results, because native Talon cannot provide them; inspect
// TalonBatchLoadError for original request indexes and uncertain outcomes.
func (c *TalonClient) BatchLoad(ctx context.Context, requests []TalonLoadRequest) ([]TalonLoadResult, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if len(requests) == 0 {
		return nil, nil
	}
	done, err := c.submit(ctx, talonRequest{operation: C.LOON_TALON_BATCH_LOAD, loads: requests})
	if err != nil {
		return nil, err
	}
	select {
	case result := <-done:
		if result.err != nil {
			return nil, result.err
		}
		if len(result.loads) != len(requests) {
			return nil, merr.WrapErrServiceInternalMsg("Talon batch returned %d results, expected %d", len(result.loads), len(requests))
		}
		return result.loads, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func (c *TalonClient) Close() {
	c.closeOnce.Do(func() {
		c.io.Close()
		C.loon_talon_client_destroy(c.native)
	})
}

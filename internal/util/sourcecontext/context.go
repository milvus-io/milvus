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

// Package sourcecontext bridges trusted Go provider sessions to the native
// storage context ABI. Tokens exist only in callback-owned transient buffers.
package sourcecontext

/*
#cgo pkg-config: milvus-storage
#include <stdlib.h>
#include "bridge.h"
*/
import "C"

import (
	"context"
	"runtime/cgo"
	"sync"
	"time"
	"unsafe"

	"github.com/milvus-io/milvus/pkg/v3/sourceauth"
)

// Context owns a native handle during a synchronous Go operation. Close cancels
// source I/O before releasing it. Native-owned retained readers use the factory
// entry point below, whose lifetime is independent of the initiating Proxy RPC.
// Do not copy a Context or race Handle with Close.
type Context struct {
	mu     sync.Mutex
	handle C.LoonSourceReadContextHandle
	stop   func() bool
}

func Open(ctx context.Context, source, spec string) (*Context, error) {
	native, err := openNative(ctx, source, spec, 0)
	if err != nil || native == 0 {
		return nil, err
	}
	result := &Context{handle: native}
	result.stop = context.AfterFunc(ctx, func() { result.Cancel() })
	return result, nil
}
func (c *Context) Handle() uintptr {
	if c == nil {
		return 0
	}
	return uintptr(c.handle)
}
func (c *Context) Cancel() {
	if c == nil {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.handle != 0 {
		C.loon_source_read_context_cancel(c.handle)
	}
}
func (c *Context) Close() {
	if c == nil {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.stop != nil {
		c.stop()
	}
	if c.handle != 0 {
		C.loon_source_read_context_cancel(c.handle)
		C.loon_source_read_context_release(c.handle)
		c.handle = 0
	}
}

type callbackState struct {
	ctx     context.Context
	cancel  context.CancelFunc
	session sourceauth.Session
}

func openNative(ctx context.Context, source, spec string, authorityDeadline int64, onClose ...context.CancelFunc) (C.LoonSourceReadContextHandle, error) {
	session, err := sourceauth.Open(ctx, source, spec)
	if err != nil || session == nil {
		return 0, err
	}
	ctx, cancel := context.WithCancel(ctx)
	childCancel := cancel
	cancel = func() {
		childCancel()
		for _, close := range onClose {
			close()
		}
	}
	state := &callbackState{ctx: ctx, cancel: cancel, session: session}
	owner := cgo.NewHandle(state)
	adopted := false
	defer func() {
		if !adopted {
			owner.Delete()
			cancel()
			session.Close()
		}
	}()
	account := C.CString(session.AccountName())
	defer C.free(unsafe.Pointer(account))
	roots := session.AllowedRootURIs()
	array := C.calloc(C.size_t(len(roots)), C.size_t(unsafe.Sizeof(uintptr(0))))
	if array == nil {
		return 0, sourceauth.StatusError(sourceauth.CredentialInvalidResponse)
	}
	defer C.free(array)
	cRoots := unsafe.Slice((**C.char)(array), len(roots))
	for i, root := range roots {
		cRoots[i] = C.CString(root)
	}
	defer func() {
		for _, root := range cRoots {
			C.free(unsafe.Pointer(root))
		}
	}()
	// Execution cancellation is propagated by Context. Only an explicit native
	// authority deadline may constrain signed SAS lifetime.
	options := C.LoonSourceReadContextOptions{
		abi_version: 1, struct_size: C.sizeof_LoonSourceReadContextOptions,
		account_name: account, allowed_root_uris: (**C.char)(array), allowed_root_count: C.size_t(len(roots)),
		operation_deadline_unix_ms: C.int64_t(authorityDeadline), request_timeout_ms: 15000,
	}
	var native C.LoonSourceReadContextHandle
	result := C.MilvusSourceContextCreate(C.uintptr_t(owner), &options, &native)
	defer C.loon_ffi_free_result(&result)
	if C.loon_ffi_is_success(&result) == 0 {
		if err, ok := sourceauth.ErrorFromNativeCode(int(result.err_code)); ok {
			return 0, err
		}
		return 0, sourceauth.StatusError(sourceauth.CredentialInvalidResponse)
	}
	adopted = true
	return native, nil
}

//export MilvusOpenSourceReadContext
func MilvusOpenSourceReadContext(source, spec *C.char, deadline C.int64_t, output *C.uintptr_t) (status C.int) {
	status = C.int(sourceauth.CredentialInvalidResponse)
	if output == nil {
		return
	}
	*output = 0
	defer func() {
		if recover() != nil {
			status = C.int(sourceauth.CredentialInvalidResponse)
		}
	}()
	ctx := context.Background()
	// Native retained readers normally pass zero. A nonzero value explicitly
	// bounds both reader lifetime and signed authority. Ordinary execution
	// timeouts use Context.Cancel, never this credential authority bound.
	if deadline != 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithDeadline(ctx, time.UnixMilli(int64(deadline)))
		// The session owns the derived context, so cancel when the callback
		// owner is destroyed, not as soon as this constructor returns.
		native, err := openNative(ctx, C.GoString(source), C.GoString(spec), int64(deadline), cancel)
		if err != nil || native == 0 {
			cancel()
			return C.int(sourceauth.StatusFromError(err))
		}
		// The deadline timer has a bounded lifetime even if the native reader
		// remains retained after its operation deadline.
		*output = C.uintptr_t(native)
		return C.int(sourceauth.CredentialOK)
	}
	native, err := openNative(ctx, C.GoString(source), C.GoString(spec), 0)
	if err != nil {
		return C.int(sourceauth.StatusFromError(err))
	}
	*output = C.uintptr_t(native)
	return C.int(sourceauth.CredentialOK)
}

//export MilvusSourceFetch
func MilvusSourceFetch(owner C.uintptr_t, request *C.LoonSourceCredentialRequest, output *C.LoonSourceCredentialResult) {
	if output == nil {
		return
	}
	output.abi_version = 1
	output.struct_size = C.sizeof_LoonSourceCredentialResult
	output.status = C.LOON_SOURCE_CREDENTIAL_INVALID_RESPONSE
	defer func() {
		if recover() != nil {
			C.MilvusSourceResultRelease(output)
			output.status = C.LOON_SOURCE_CREDENTIAL_INVALID_RESPONSE
		}
	}()
	if request == nil || request.abi_version != 1 || request.struct_size < C.sizeof_LoonSourceCredentialRequest ||
		request.resource_uri == nil || request.scope_root_uri == nil ||
		request.resource_uri_length == 0 || request.resource_uri_length > 65536 ||
		request.scope_root_uri_length == 0 || request.scope_root_uri_length > 65536 ||
		(request.operation != C.LOON_SOURCE_READ && request.operation != C.LOON_SOURCE_LIST) ||
		request.request_deadline_unix_ms <= 0 {
		return
	}
	state := cgo.Handle(owner).Value().(*callbackState)
	deadline := time.UnixMilli(int64(request.request_deadline_unix_ms))
	ctx, cancel := context.WithDeadline(state.ctx, deadline)
	defer cancel()
	req := sourceauth.FetchRequest{
		ResourceURI:  C.GoStringN(request.resource_uri, C.int(request.resource_uri_length)),
		ScopeRootURI: C.GoStringN(request.scope_root_uri, C.int(request.scope_root_uri_length)),
		Operation:    sourceauth.ReadOperation(request.operation), RequestDeadline: deadline,
	}
	if request.operation_deadline_unix_ms != 0 {
		req.OperationDeadline = time.UnixMilli(int64(request.operation_deadline_unix_ms))
	}
	if ctx.Err() != nil {
		output.status = C.LoonSourceCredentialStatus(sourceauth.StatusFromError(ctx.Err()))
		return
	}
	credential := state.session.Fetch(ctx, req)
	if ctx.Err() != nil {
		credential = sourceauth.Credential{Status: sourceauth.StatusFromError(ctx.Err())}
	}
	if credential.Status > sourceauth.CredentialInvalidResponse {
		return
	}
	output.status = C.LoonSourceCredentialStatus(credential.Status)
	if credential.Status != sourceauth.CredentialOK {
		return
	}
	if credential.AccountName == "" || credential.ScopeRootURI == "" || credential.SASToken == "" ||
		len(credential.AccountName) > 128 || len(credential.ScopeRootURI) > 65536 || len(credential.SASToken) > 16384 ||
		!credential.ExpiresAt.After(time.Now()) {
		output.status = C.LOON_SOURCE_CREDENTIAL_INVALID_RESPONSE
		return
	}
	output.credential_kind = C.LOON_SOURCE_CREDENTIAL_KIND_AZURE_SAS
	output.account_name = C.CString(credential.AccountName)
	output.account_name_length = C.size_t(len(credential.AccountName))
	output.scope_root_uri = C.CString(credential.ScopeRootURI)
	output.scope_root_uri_length = C.size_t(len(credential.ScopeRootURI))
	output.sas_token = C.CString(credential.SASToken)
	output.sas_token_length = C.size_t(len(credential.SASToken))
	output.expires_at_unix_ms = C.int64_t(credential.ExpiresAt.UnixMilli())
}

//export MilvusSourceDestroy
func MilvusSourceDestroy(owner C.uintptr_t) {
	// A provider panic must not unwind across the C ABI. The native ownership
	// contract calls this exactly once after references and callbacks drain.
	defer func() { _ = recover() }()
	handle := cgo.Handle(owner)
	state := handle.Value().(*callbackState)
	defer handle.Delete()
	state.cancel()
	state.session.Close()
}

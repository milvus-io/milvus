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

package sourceauth

import (
	"context"
	"sync/atomic"

	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

// RequestHook captures transient source credentials in an Authorizer and removes
// them from transport metadata before any coordinator RPC. It does not replace
// Milvus authentication or the installed request hook. Network authorization is
// deferred to Prepare, after the effective external source has been resolved.
type RequestHook func(context.Context, any, string) (context.Context, error)

type runtime struct {
	providers *Registry
	request   RequestHook
}

var installed atomic.Pointer[runtime]

// Install configures the distribution in EVERY process before Milvus starts.
// The immutable installation cannot be removed while readers still hold leases.
// Stock binaries do not install a runtime and retain their existing behavior.
func Install(providers *Registry, request RequestHook) error {
	if providers == nil || request == nil || !installed.CompareAndSwap(nil, &runtime{providers, request}) {
		return merr.WrapErrServiceInternalMsg("invalid or duplicate source authorization runtime")
	}
	return nil
}

func PrepareRequest(ctx context.Context, req any, method string) (context.Context, error) {
	if r := installed.Load(); r != nil {
		return r.request(ctx, req, method)
	}
	return ctx, nil
}

// Open creates a per-reader session from trusted coordinator metadata. A bound
// source without an installed runtime is always refused, including on workers.
func Open(ctx context.Context, source, spec string) (Session, error) {
	if r := installed.Load(); r != nil {
		return r.providers.Open(ctx, source, spec)
	}
	var empty Registry
	return empty.Open(ctx, source, spec)
}

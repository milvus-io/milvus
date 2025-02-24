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

package contextutil

import (
	"context"
	"fmt"
	"strings"
	"time"

	"google.golang.org/grpc/metadata"

	"github.com/milvus-io/milvus/pkg/v2/util"
	"github.com/milvus-io/milvus/pkg/v2/util/crypto"
)

type ctxTenantKey struct{}

// WithTenantID creates a new context that has tenantID injected.
func WithTenantID(ctx context.Context, tenantID string) context.Context {
	if ctx == nil {
		ctx = context.Background()
	}
	return context.WithValue(ctx, ctxTenantKey{}, tenantID)
}

// TenantID tries to retrieve tenantID from the given context.
// If it doesn't exist, an empty string is returned.
func TenantID(ctx context.Context) string {
	if requestID, ok := ctx.Value(ctxTenantKey{}).(string); ok {
		return requestID
	}

	return ""
}

func AppendToIncomingContext(ctx context.Context, kv ...string) context.Context {
	if len(kv)%2 == 1 {
		panic(fmt.Sprintf("metadata: AppendToOutgoingContext got an odd number of input pairs for metadata: %d", len(kv)))
	}
	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		md = metadata.New(make(map[string]string, len(kv)/2))
	}
	for i, s := range kv {
		if i%2 == 0 {
			md.Append(s, kv[i+1])
		}
	}
	return metadata.NewIncomingContext(ctx, md)
}

func GetCurUserFromContext(ctx context.Context) (string, error) {
	username, _, err := GetAuthInfoFromContext(ctx)
	return username, err
}

func GetAuthInfoFromContext(ctx context.Context) (string, string, error) {
	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return "", "", fmt.Errorf("fail to get md from the context")
	}
	authorization, ok := md[strings.ToLower(util.HeaderAuthorize)]
	if !ok || len(authorization) < 1 {
		return "", "", fmt.Errorf("fail to get authorization from the md, %s:[token]", strings.ToLower(util.HeaderAuthorize))
	}
	token := authorization[0]
	rawToken, err := crypto.Base64Decode(token)
	if err != nil {
		return "", "", fmt.Errorf("fail to decode the token, token: %s", token)
	}
	secrets := strings.SplitN(rawToken, util.CredentialSeperator, 2)
	if len(secrets) < 2 {
		return "", "", fmt.Errorf("fail to get user info from the raw token, raw token: %s", rawToken)
	}
	// username: secrets[0]
	// password: secrets[1]
	return secrets[0], secrets[1], nil
}

// TODO: use context.WithTimeoutCause instead in go 1.21.0, then deprecated this function
// !!! We cannot keep same implementation with context.WithDeadlineCause.
// if cancel happens, context.WithTimeoutCause will return context.Err() == context.Timeout and context.Cause(ctx) == err.
// if cancel happens, WithTimeoutCause will return context.Err() == context.Canceled and context.Cause(ctx) == err.
func WithTimeoutCause(parent context.Context, timeout time.Duration, err error) (context.Context, context.CancelFunc) {
	return WithDeadlineCause(parent, time.Now().Add(timeout), err)
}

// TODO: use context.WithDeadlineCause instead in go 1.21.0, then deprecated this function
// !!! We cannot keep same implementation with context.WithDeadlineCause.
// if cancel happens, context.WithDeadlineCause will return context.Err() == context.DeadlineExceeded and context.Cause(ctx) == err.
// if cancel happens, WithDeadlineCause will return context.Err() == context.Canceled and context.Cause(ctx) == err.
func WithDeadlineCause(parent context.Context, deadline time.Time, err error) (context.Context, context.CancelFunc) {
	if parent == nil {
		panic("cannot create context from nil parent")
	}
	if parentDeadline, ok := parent.Deadline(); ok && parentDeadline.Before(deadline) {
		// The current deadline is already sooner than the new one.
		return context.WithCancel(parent)
	}
	ctx, cancel := context.WithCancelCause(parent)
	time.AfterFunc(time.Until(deadline), func() {
		cancel(err)
	})

	return ctx, func() {
		cancel(context.Canceled)
	}
}

// MergeContext create a cancellation context that cancels when any of the given contexts are canceled.
func MergeContext(ctx1 context.Context, ctx2 context.Context) (context.Context, context.CancelFunc) {
	ctx, cancel := context.WithCancelCause(ctx1)
	stop := context.AfterFunc(ctx2, func() {
		cancel(context.Cause(ctx2))
	})
	return ctx, func() {
		stop()
		cancel(context.Canceled)
	}
}

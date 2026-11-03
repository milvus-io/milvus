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

package grpcclient

import (
	"context"

	"github.com/cockroachdb/errors"
)

type failFastOnConnectionErrorKey struct{}

// WithFailFastOnConnectionError lets a read caller with an alternative route
// handle a connection failure before reconnecting/retrying the same endpoint.
// It does not impose an RPC deadline or change application-status retries.
func WithFailFastOnConnectionError(ctx context.Context) context.Context {
	return context.WithValue(ctx, failFastOnConnectionErrorKey{}, true)
}

type connectionFailure struct {
	error
}

func (e connectionFailure) Unwrap() error {
	return e.error
}

// IsConnectionFailure identifies a connection failure observed while the
// caller's context was still live. The component boundary must translate this
// local classification into its wire error; it is not a node-death verdict.
func IsConnectionFailure(err error) bool {
	var failure connectionFailure
	return errors.As(err, &failure)
}

func isDialFailure(err error) bool {
	var dialErr errConnect
	return errors.As(err, &dialErr) && !errors.Is(dialErr.err, context.Canceled)
}

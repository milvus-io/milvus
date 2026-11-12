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

package taskmodel

import "context"

// ReadRetry retries only a read, including collection readiness checks and
// backoff. The callback must not perform any mutation.
type ReadRetry func(context.Context, func(context.Context) error) error

// DMLReadRetry runs a read retry loop without retaining a DML execution slot
// during readiness waits. Each read callback and the caller's continuation run
// with a slot. This is a serial, task-local boundary, not a goroutine executor.
type DMLReadRetry func(context.Context, ReadRetry, func(context.Context) error) error

type dmlReadRetryKey struct{}

func WithDMLReadRetry(ctx context.Context, run DMLReadRetry) context.Context {
	return context.WithValue(ctx, dmlReadRetryKey{}, run)
}

func RunDMLReadRetry(ctx context.Context, retry ReadRetry, read func(context.Context) error) error {
	if run, ok := ctx.Value(dmlReadRetryKey{}).(DMLReadRetry); ok {
		// Child query tasks must not borrow or release their parent's DML slot.
		ctx = context.WithValue(ctx, dmlReadRetryKey{}, nil)
		return run(ctx, retry, read)
	}
	return retry(ctx, read)
}

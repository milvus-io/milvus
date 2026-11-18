// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
package shardclient

import (
	"context"
	"time"

	"github.com/cockroachdb/errors"

	"github.com/milvus-io/milvus/pkg/v2/util/merr"
)

type attemptOutcome int

const (
	// attemptIgnored says nothing about the replica.
	attemptIgnored attemptOutcome = iota
	attemptSucceeded
	// attemptFailed counts against the replica, within the attempt's request
	// class, once several attempts in a row failed (see replicaStats.fail).
	attemptFailed
)

const (
	// timeoutWithoutDeadline is the timeout assumed for a request without
	// deadline. The gRPC entry of the proxy sets none by default (the REST
	// entry does, proxy.http.requestTimeoutMs).
	timeoutWithoutDeadline = time.Minute
	// maxAttemptPenalty bounds the penalty of one attempt.
	maxAttemptPenalty = time.Hour
)

// classifyAttempt tells what the outcome of one attempt says about the replica
// that served it, and the latency or penalty to record:
//
//   - success, or ErrAttemptSuperseded (it succeeded after another attempt of
//     the channel had already published): succeeded, with its elapsed time.
//   - an input error: ignored, the request itself is at fault.
//   - the attempt's context was canceled: ignored. The caller stopped
//     waiting, or the request returned with another attempt's result.
//   - any other error, a timeout of the request included: failed, with a
//     penalty of the attempt's timeout (the request deadline minus the attempt
//     start, or timeoutWithoutDeadline): a failed attempt counts as if it had
//     taken as long as the request was willing to wait, so that a replica that
//     fails fast does not look fast.
//
// Errors are not told apart further on purpose: the statistics are kept per
// request class, so an error every replica returns for a class (for example
// ErrChannelTSafeStalled while the WAL lags, which hits Strong reads on every
// replica) raises every replica alike and leaves the shares unchanged, while
// the same error code returned by one replica only (its own consumer is
// stuck) moves the traffic away from it. The error code alone cannot tell
// these two cases apart.
func classifyAttempt(ctx context.Context, start, now time.Time, err error) (attemptOutcome, time.Duration) {
	elapsed := now.Sub(start)
	if err == nil || errors.Is(err, ErrAttemptSuperseded) {
		return attemptSucceeded, elapsed
	}
	if merr.GetErrorType(err) == merr.InputError {
		return attemptIgnored, 0
	}
	if errors.Is(ctx.Err(), context.Canceled) {
		return attemptIgnored, 0
	}
	return attemptFailed, attemptTimeout(ctx, start, elapsed)
}

// attemptTimeout returns how long the request was willing to wait for an
// attempt that started at start and has run for elapsed: until the request
// deadline, or timeoutWithoutDeadline without one.
func attemptTimeout(ctx context.Context, start time.Time, elapsed time.Duration) time.Duration {
	timeout := timeoutWithoutDeadline
	if deadline, ok := ctx.Deadline(); ok {
		timeout = deadline.Sub(start)
	}
	if timeout < elapsed {
		timeout = elapsed
	}
	if timeout <= 0 {
		timeout = timeoutWithoutDeadline
	}
	if timeout > maxAttemptPenalty {
		timeout = maxAttemptPenalty
	}
	return timeout
}

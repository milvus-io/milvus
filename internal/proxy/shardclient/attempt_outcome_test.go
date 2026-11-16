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
	"testing"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/assert"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

func TestClassifyAttempt(t *testing.T) {
	start := time.Now()
	now := start.Add(2 * time.Millisecond)
	withDeadline, cancel := context.WithDeadline(context.Background(), start.Add(3*time.Second))
	defer cancel()
	canceled, cancelNow := context.WithCancel(context.Background())
	cancelNow()
	expired, cancelExpired := context.WithDeadline(context.Background(), start.Add(-time.Millisecond))
	defer cancelExpired()

	cases := []struct {
		name    string
		ctx     context.Context
		now     time.Time
		err     error
		outcome attemptOutcome
		latency time.Duration
	}{
		{"success", withDeadline, now, nil, attemptSucceeded, 2 * time.Millisecond},
		{"superseded is a success", withDeadline, now, ErrAttemptSuperseded, attemptSucceeded, 2 * time.Millisecond},
		{"wrapped superseded", withDeadline, now, errors.Wrap(ErrAttemptSuperseded, "search"), attemptSucceeded, 2 * time.Millisecond},
		{"input error", withDeadline, now, merr.WrapErrParameterInvalidMsg("bad"), attemptIgnored, 0},
		{"input error through a status", withDeadline, now, merr.Error(merr.Status(merr.WrapErrParameterInvalidMsg("bad"))), attemptIgnored, 0},
		{"caller canceled", canceled, now, context.Canceled, attemptIgnored, 0},
		{"caller canceled mid rpc", canceled, now, status.Error(codes.Canceled, "context canceled"), attemptIgnored, 0},
		// a failure counts as the attempt's timeout
		{"fast node error", withDeadline, now, merr.ErrServiceUnavailable, attemptFailed, 3 * time.Second},
		{"unreachable node", withDeadline, now, status.Error(codes.Unavailable, "connection refused"), attemptFailed, 3 * time.Second},
		{"dial timeout inside the attempt", withDeadline, now, errors.Wrap(context.DeadlineExceeded, "failed to connect"), attemptFailed, 3 * time.Second},
		{"tsafe lag", withDeadline, now, merr.Error(merr.Status(errors.Wrap(merr.ErrChannelTSafeStalled, "lag(3s) max(3s)"))), attemptFailed, 3 * time.Second},
		{"node error through a status", withDeadline, now, merr.Error(merr.Status(merr.WrapErrChannelNotAvailable("ch"))), attemptFailed, 3 * time.Second},
		{"query node serialized timeout", withDeadline, now, merr.Error(merr.Status(context.DeadlineExceeded)), attemptFailed, 3 * time.Second},
		// a timeout of the request counts with its timeout too
		{"request timed out", expired, start.Add(3 * time.Second), status.Error(codes.DeadlineExceeded, "deadline"), attemptFailed, 3 * time.Second},
		{"started after its deadline", expired, now, context.DeadlineExceeded, attemptFailed, 2 * time.Millisecond},
		{"any error once the request timed out", expired, start.Add(time.Second), merr.ErrServiceUnavailable, attemptFailed, time.Second},
		{"without deadline", context.Background(), now, errors.New("boom"), attemptFailed, time.Minute},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			outcome, latency := classifyAttempt(tc.ctx, start, tc.now, tc.err)
			assert.Equal(t, tc.outcome, outcome)
			assert.Equal(t, tc.latency, latency)
		})
	}

	// the penalty is bounded
	_, latency := classifyAttempt(context.Background(), start, start.Add(1000*time.Hour), merr.ErrServiceUnavailable)
	assert.Equal(t, maxAttemptPenalty, latency)
}

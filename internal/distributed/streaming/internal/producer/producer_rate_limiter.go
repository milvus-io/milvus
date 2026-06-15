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

package producer

import (
	"context"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"golang.org/x/time/rate"

	"github.com/milvus-io/milvus/pkg/v2/metrics"
	"github.com/milvus-io/milvus/pkg/v2/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v2/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v2/streaming/util/ratelimit"
	"github.com/milvus-io/milvus/pkg/v2/util/merr"
	"github.com/milvus-io/milvus/pkg/v2/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v2/util/syncutil"
)

var ErrSlowDown = merr.WrapErrServiceRateLimit(0, "reach the limit of request, please slowdown and retry later")

// getDefaultBurst returns the default burst size from configuration.
func getDefaultBurst() int {
	return int(paramtable.Get().StreamingCfg.WALRateLimitDefaultBurst.GetAsSize())
}

// newProduceRateLimiter creates a new rate limiter.
func newProduceRateLimiter(channel string) *produceRateLimiter {
	defaultBurst := getDefaultBurst()
	return &produceRateLimiter{
		limiter: rate.NewLimiter(rate.Inf, defaultBurst),
		cond:    syncutil.NewContextCond(&sync.Mutex{}),
		state:   ratelimit.NewNormalRateLimitState(),
		channel: channel,
	}
}

// produceRateLimiter is an adaptive rate limiter that can adjust the rate limit based on the load.
type produceRateLimiter struct {
	limiter *rate.Limiter

	cond    *syncutil.ContextCond
	state   ratelimit.RateLimitState
	channel string
}

// RequestReservation requests a reservation for the message.
func (arl *produceRateLimiter) RequestReservation(ctx context.Context, msgs ...message.MutableMessage) (*rate.Reservation, error) {
	msgSize := 0
	for _, msg := range msgs {
		msgSize += msg.EstimateSize()
	}
	r := arl.limiter.ReserveN(time.Now(), msgSize)
	if !r.OK() {
		return nil, ErrSlowDown
	}
	return r, nil
}

// UpdateRateLimitState updates the rate limit state.
func (arl *produceRateLimiter) UpdateRateLimitState(state ratelimit.RateLimitState) {
	arl.cond.L.Lock()
	defer arl.cond.L.Unlock()

	if arl.state == state {
		return
	}

	arl.cond.UnsafeBroadcast()
	defaultBurst := getDefaultBurst()
	switch state.State {
	case streamingpb.WALRateLimitState_WAL_RATE_LIMIT_STATE_SLOWDOWN:
		arl.limiter.SetLimit(rate.Limit(state.Rate))
		arl.limiter.SetBurst(defaultBurst)
	case streamingpb.WALRateLimitState_WAL_RATE_LIMIT_STATE_REJECT:
		arl.limiter.SetLimit(0)
		arl.limiter.SetBurst(0)
	case streamingpb.WALRateLimitState_WAL_RATE_LIMIT_STATE_NORMAL:
		arl.limiter.SetLimit(rate.Inf)
		arl.limiter.SetBurst(defaultBurst)
	}
	arl.state = state
	metrics.StreamingServiceClientRateLimitState.DeletePartialMatch(prometheus.Labels{
		metrics.WALChannelLabelName: arl.channel,
	})
	metrics.StreamingServiceClientRateLimitState.WithLabelValues(
		paramtable.GetStringNodeID(),
		arl.channel,
		state.State.String(),
	).Set(float64(state.Rate))
}

// WaitUntilAvailable waits until the rate limit is available.
func (arl *produceRateLimiter) WaitUntilAvailable(ctx context.Context) error {
	arl.cond.L.Lock()
	for arl.state.State == streamingpb.WALRateLimitState_WAL_RATE_LIMIT_STATE_REJECT {
		if err := arl.cond.Wait(ctx); err != nil {
			return err
		}
	}
	arl.cond.L.Unlock()
	return nil
}

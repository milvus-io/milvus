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

package compaction

import (
	"context"
	"errors"
	"math"
	"strconv"
	"sync"
	"time"

	"golang.org/x/time/rate"

	"github.com/milvus-io/milvus/internal/datacoord/meta"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

// ErrAdmissionLimited is handled inside DataCoord. Recovery of already
// accepted tasks enters the queue separately from new work.
var ErrAdmissionLimited = errors.New("compaction task admission limited")

type Producer = meta.CompactionProducer
type AdmissionOptions = meta.CompactionAdmissionOptions

const (
	ProducerLegacyTrigger  = meta.ProducerLegacyTrigger
	ProducerTriggerManager = meta.ProducerTriggerManager
	ProducerTBC            = meta.ProducerTBC
	ProducerImport         = meta.ProducerImport
	ProducerRestore        = meta.ProducerRestore
)

const (
	admissionRateLimit     = "rate"
	admissionShareLimit    = "producer_share"
	admissionCapacityLimit = "capacity"
)

type admissionConfig struct {
	capacity int
	interval time.Duration
}

func readAdmissionConfig() (admissionConfig, error) {
	tokens := &Params.DataCoordCfg.CompactionRateLimitTokens
	burst, err := strconv.ParseInt(tokens.GetValue(), 10, 32)
	if err != nil || burst <= 0 {
		return admissionConfig{}, merr.WrapErrParameterInvalidMsg("%s must be a positive integer", tokens.Key)
	}
	interval := &Params.DataCoordCfg.CompactionRateLimitInterval
	seconds, err := strconv.ParseFloat(interval.GetValue(), 64)
	nanos := seconds * float64(time.Second)
	if err != nil || math.IsNaN(nanos) || nanos < 1 || nanos >= float64(math.MaxInt64) {
		return admissionConfig{}, merr.WrapErrParameterInvalidMsg("%s must be a positive number of seconds representable as a duration", interval.Key)
	}
	return admissionConfig{capacity: int(burst), interval: time.Duration(nanos)}, nil
}

type producerService struct {
	deferredAt time.Time
	admitted   int64
	pending    int64
}

// Adapted from the shared bucket and cumulative producer service in #53979.
// Demand is inferred from recent deferrals. A sole producer may use the full
// balance. While producers contend, each may get ahead of the least served by
// at most its share of the burst, so partial refills need not be split.
type compactionAdmission struct {
	mu         sync.Mutex
	config     admissionConfig
	tokens     float64
	refilledAt time.Time
	service    [meta.CompactionProducerCount]producerService
	now        func() time.Time
}

func newCompactionAdmission(config admissionConfig) *compactionAdmission {
	return &compactionAdmission{
		config: config, tokens: float64(config.capacity),
		refilledAt: time.Now(), now: time.Now,
	}
}

func (a *compactionAdmission) refill(now time.Time) {
	a.tokens = min(float64(a.config.capacity), a.tokens+
		float64(a.config.capacity)*now.Sub(a.refilledAt).Seconds()/a.config.interval.Seconds())
	a.refilledAt = now
}

func (a *compactionAdmission) refresh(ctx context.Context, now time.Time) {
	a.refill(now)
	config, err := readAdmissionConfig()
	if err != nil {
		mlog.RatedWarn(ctx, rate.Limit(1), "invalid compaction admission update, keeping the last valid settings", mlog.Err(err))
		return
	}
	a.config = config
	a.tokens = min(a.tokens, float64(config.capacity))
}

func (a *compactionAdmission) available(now time.Time, producer Producer, capacity int) (int, string) {
	if capacity == 0 {
		return 0, admissionCapacityLimit
	}

	stale := 2 * Params.DataCoordCfg.MixCompactionTriggerInterval.GetAsDuration(time.Second)
	lowest := int64(math.MaxInt64)
	others := 0
	contested := false
	for i := range a.service {
		service := &a.service[i]
		if service.deferredAt.IsZero() || now.Sub(service.deferredAt) > stale {
			continue
		}
		contested = true
		if Producer(i) != producer {
			lowest = min(lowest, service.admitted+service.pending)
			others++
		}
	}
	if !contested {
		// Keep at most a burst of prior service so the first producer to
		// ask after a depletion cannot claim every partial refill.
		for i := range a.service {
			a.service[i].admitted = min(a.service[i].admitted, int64(a.config.capacity))
		}
	} else if others > 0 {
		// A source without recent demand rejoins at the current service
		// floor. It cannot claim an idle source's share in one burst.
		for i := range a.service {
			service := &a.service[i]
			if service.deferredAt.IsZero() || now.Sub(service.deferredAt) > stale {
				service.admitted = max(service.admitted, lowest-service.pending)
			}
		}
	}
	tokens := int(a.tokens)
	if others > 0 {
		current := &a.service[producer]
		lead := (int64(a.config.capacity) + int64(others)) / int64(others+1)
		share := max(int64(0), lowest+lead-current.admitted-current.pending)
		if share < int64(min(tokens, capacity)) {
			return int(share), admissionShareLimit
		}
	}
	if tokens < capacity {
		return tokens, admissionRateLimit
	}
	return capacity, admissionCapacityLimit
}

func (a *compactionAdmission) attemptLimit(ctx context.Context, producer Producer, capacity, plans int) (int, string) {
	a.mu.Lock()
	defer a.mu.Unlock()
	now := a.now()
	a.refresh(ctx, now)
	if plans == 0 {
		a.service[producer].deferredAt = time.Time{}
		return 0, ""
	}
	limit, reason := a.available(now, producer, capacity)
	if plans > limit && reason != admissionCapacityLimit {
		a.service[producer].deferredAt = now
	}
	return min(plans, limit), reason
}

func (a *compactionAdmission) acquire(ctx context.Context, producer Producer, capacity int) (*admissionPermit, string) {
	a.mu.Lock()
	defer a.mu.Unlock()
	now := a.now()
	a.refresh(ctx, now)
	limit, reason := a.available(now, producer, capacity)
	if limit == 0 {
		if reason != admissionCapacityLimit {
			a.service[producer].deferredAt = now
		}
		return nil, reason
	}
	a.tokens--
	a.service[producer].pending++
	return &admissionPermit{bucket: a, producer: producer}, ""
}

type admissionPermit struct {
	bucket   *compactionAdmission
	producer Producer
}

// Called once on exit from Inspector.EnqueueCompaction. Persisting a task
// without getting it into the queue still returns its admission token.
func (p *admissionPermit) settle(queued bool) {
	a := p.bucket
	a.mu.Lock()
	defer a.mu.Unlock()
	service := &a.service[p.producer]
	service.pending--
	if queued {
		service.admitted++
		return
	}
	a.refill(a.now())
	a.tokens = min(a.tokens+1, float64(a.config.capacity))
}

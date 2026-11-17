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

package credentialcache

import (
	"strings"
	"sync"
	"time"

	"github.com/minio/minio-go/v7/pkg/credentials"

	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

// Provider keeps only validated credential tuples. Unlike minio's outer cache,
// a failed early refresh can fall back to the tuple until its actual expiration.
// Each instance belongs to one selected identity; it never selects a fallback
// identity or invents a lifetime for credentials whose expiration is unknown.
type Provider struct {
	mu                  sync.Mutex
	fetch               func() (credentials.Value, error)
	cached              credentials.Value
	refreshWindow       time.Duration
	requireSessionToken bool
	allowStatic         bool
	now                 func() time.Time
}

func New(fetch func() (credentials.Value, error), refreshWindow time.Duration, requireSessionToken, allowStatic bool) *Provider {
	return &Provider{
		fetch: fetch, refreshWindow: refreshWindow, requireSessionToken: requireSessionToken,
		allowStatic: allowStatic, now: time.Now,
	}
}

func nonempty(value string) bool {
	return strings.TrimSpace(value) != ""
}

func (p *Provider) valid(value credentials.Value, now time.Time) bool {
	if !nonempty(value.AccessKeyID) || !nonempty(value.SecretAccessKey) ||
		(p.requireSessionToken && !nonempty(value.SessionToken)) {
		return false
	}
	if value.Expiration.IsZero() {
		return p.allowStatic
	}
	return value.Expiration.After(now)
}

func (p *Provider) Retrieve() (credentials.Value, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	now := p.now()
	if p.valid(p.cached, now) && (p.cached.Expiration.IsZero() || p.cached.Expiration.Sub(now) > p.refreshWindow) {
		return p.cached, nil
	}

	candidate, err := p.fetch()
	// Fetching may take long enough for either tuple to expire. Validate against
	// completion time, not the time at which the request started.
	now = p.now()
	if err == nil && p.valid(candidate, now) {
		candidate.SignerType = credentials.SignatureV4
		p.cached = candidate
		return candidate, nil
	}
	if p.valid(p.cached, now) {
		return p.cached, nil
	}
	if err != nil {
		return credentials.Value{}, merr.Wrap(err, "refresh object storage credentials")
	}
	return credentials.Value{}, merr.WrapErrServiceUnavailableMsg("credential refresh returned an incomplete or expired tuple")
}

// Minio must enter Retrieve on every request so a shared provider can enforce
// real expiration. Retrieve's cache-hit path does not perform a network call.
func (p *Provider) IsExpired() bool {
	return true
}

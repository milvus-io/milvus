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

package requestbudget

import (
	"context"
	"strconv"
	"time"

	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

// Policy is a snapshot taken at the beginning of a request. Its deadlines are
// absolute, so later stages cannot renew a spent budget. The header guard
// runs independently before the post-header budget starts.
type Policy struct {
	OverallTimeoutBudget      time.Duration
	ReadHeaderTimeout         time.Duration
	MaxConnectionIdleInterval time.Duration
}

// Resolve returns the earliest absolute deadline among the server budget,
// client request budget, and inherited context deadline.
func (p Policy) Resolve(parent context.Context, startedAt time.Time, requestTimeout string) (time.Time, error) {
	if p.OverallTimeoutBudget <= 0 || p.ReadHeaderTimeout <= 0 || p.MaxConnectionIdleInterval < 0 {
		return time.Time{}, merr.WrapErrServiceInternalMsg("invalid REST timeout policy: overall and header budgets must be positive; connection idle interval must be nonnegative")
	}
	deadline := startedAt.Add(p.OverallTimeoutBudget)
	if requestTimeout != "" {
		seconds, err := strconv.ParseInt(requestTimeout, 10, 64)
		const maxSeconds = int64((1<<63 - 1) / time.Second)
		if err != nil || seconds <= 0 || seconds > maxSeconds {
			// Preserve the offending value in the REST error, as the legacy
			// endpoint did, without reflecting an arbitrarily large header.
			shown := requestTimeout
			if len(shown) > 64 {
				shown = shown[:64] + "..."
			}
			return time.Time{}, merr.WrapErrParameterInvalidMsg("Request-Timeout %q must be a positive integer number of seconds no greater than %d", shown, maxSeconds)
		}
		clientDeadline := startedAt.Add(time.Duration(seconds) * time.Second)
		if clientDeadline.Before(deadline) {
			deadline = clientDeadline
		}
	}
	if inherited, ok := parent.Deadline(); ok && inherited.Before(deadline) {
		deadline = inherited
	}
	return deadline, nil
}

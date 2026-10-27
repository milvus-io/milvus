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

package store

import (
	"context"

	"golang.org/x/sync/errgroup"

	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

// RunBounded lets existing task pipelines share their read/write permits with
// object copies. Permits are acquired before starting goroutines, so concurrent
// plans do not each create another pool of blocked workers.
func RunBounded[T any](ctx context.Context, items []T, permits chan struct{}, operation func(context.Context, T) error) error {
	if cap(permits) <= 0 || operation == nil {
		return merr.WrapErrServiceInternalMsg("invalid copy executor configuration")
	}
	group, copyCtx := errgroup.WithContext(ctx)
loop:
	for _, item := range items {
		select {
		case permits <- struct{}{}:
		case <-copyCtx.Done():
			break loop
		}
		if copyCtx.Err() != nil {
			<-permits
			break
		}
		group.Go(func() error {
			defer func() { <-permits }()
			return operation(copyCtx, item)
		})
	}
	if err := group.Wait(); err != nil {
		return err
	}
	return ctx.Err()
}

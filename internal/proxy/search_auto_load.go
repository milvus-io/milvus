// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package proxy

import (
	"context"
	"strconv"
	"time"

	"github.com/cockroachdb/errors"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus/internal/proxy/accesslog"
	"github.com/milvus-io/milvus/internal/proxy/taskmodel"
	"github.com/milvus-io/milvus/pkg/v3/proto/querypb"
	"github.com/milvus-io/milvus/pkg/v3/util/funcutil"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/retry"
)

// retryDQL replans the entire request when its read view has
// retired, including schema lookup and all shard results. This also applies to
// explicitly loaded collections. Iterators retain their original read binding.
func (node *Proxy) retryDQL(ctx context.Context, dbName, collectionName string, params []*commonpb.KeyValuePair, execute func(context.Context) (bool, error)) error {
	iteratorValue, _ := funcutil.GetAttrByKeyFromRepeatedKV(IteratorField, params)
	isIterator, _ := strconv.ParseBool(iteratorValue)
	refreshes := 0
	autoLoad := Params.ProxyCfg.EnableAutoLoad.GetAsBool()
	accesslog.SetColdSearch(ctx, false)
	var attemptErr error
	err := retry.Handle(ctx, func() (again bool, err error) {
		defer func() { attemptErr = err }()
		if err := ctx.Err(); err != nil {
			return false, context.Cause(ctx)
		}

		if autoLoad {
			shouldLoad, err := node.shouldLoadCollection(ctx, dbName, collectionName, attemptErr)
			if err != nil {
				return false, err
			}
			if shouldLoad {
				accesslog.SetColdSearch(ctx, true)
				if err := node.ensureCollectionReady(ctx, dbName, collectionName); err != nil {
					return false, err
				}
			}
		}

		again, err = execute(ctx)
		if autoLoad && ctx.Err() == nil && errors.Is(err, merr.ErrCollectionNotLoaded) {
			return true, err
		}
		if !isIterator && shouldRefreshDQLView(ctx, err, refreshes) {
			refreshes++
			node.GetMetaCache().RemoveCollection(ctx, dbName, collectionName)
			again = true
		}
		return again, err
	})
	// retry.Handle may return its previous error when canceled during backoff.
	if ctx.Err() != nil {
		return context.Cause(ctx)
	}
	// retry.Handle also returns its previous error when a later attempt stops on
	// a child-context cancellation or timeout. Preserve that attempt's terminal
	// error while the parent request context is still valid.
	if attemptErr != nil {
		return attemptErr
	}
	return err
}

func (node *Proxy) shouldLoadCollection(ctx context.Context, dbName, collectionName string, lastReadErr error) (bool, error) {
	// Watch can still advertise old replicas after a read confirms release.
	if errors.Is(lastReadErr, merr.ErrCollectionNotLoaded) {
		return true, nil
	}
	if node.viewQueryClient == nil {
		return false, nil
	}
	if err := merr.CheckHealthy(node.GetStateCode()); err != nil {
		return false, err
	}
	collection, err := node.GetMetaCache().GetCollectionInfo(ctx, dbName, collectionName, 0)
	if err != nil {
		return false, err
	}
	available, err := node.viewQueryClient.Legacy().HasAvailableReplicas(ctx, collection.VChannels)
	return !available, err
}

func shouldRefreshDQLView(ctx context.Context, err error, refreshes int) bool {
	return refreshes < 3 && ctx.Err() == nil && errors.Is(err, merr.ErrQueryViewNotReady) &&
		!merr.IsCanceledOrTimeout(err) && merr.GetErrorType(err) != merr.InputError
}

// RetryRead implements taskmodel.QueryRunner.
func (node *Proxy) RetryRead(ctx context.Context, dbName, collectionName string, read func(context.Context) error) error {
	return taskmodel.RunDMLReadRetry(ctx, func(ctx context.Context, read func(context.Context) error) error {
		refreshes := 0
		return node.retryDMLRead(ctx, dbName, collectionName, func(ctx context.Context) (bool, error) {
			err := read(ctx)
			if shouldRefreshDQLView(ctx, err, refreshes) {
				refreshes++
				node.GetMetaCache().RemoveCollection(ctx, dbName, collectionName)
				return true, err
			}
			return false, err
		})
	}, read)
}

func (node *Proxy) retryDMLRead(ctx context.Context, dbName, collectionName string, read func(context.Context) (bool, error)) error {
	var lastReadErr error
	var terminalErr error
	err := retry.Handle(ctx, func() (bool, error) {
		terminalErr = nil
		if err := ctx.Err(); err != nil {
			return false, context.Cause(ctx)
		}
		if Params.ProxyCfg.EnableAutoLoad.GetAsBool() {
			shouldLoad, err := node.shouldLoadCollection(ctx, dbName, collectionName, lastReadErr)
			if err != nil {
				terminalErr = err
				return false, err
			}
			if shouldLoad {
				if err := node.ensureCollectionReady(ctx, dbName, collectionName); err != nil {
					if node.shouldRetryDMLLoad(err) {
						return true, err
					}
					terminalErr = err
					return false, err
				}
			}
		} else if err := merr.CheckHealthy(node.GetStateCode()); err != nil {
			terminalErr = err
			return false, err
		}
		again, err := read(ctx)
		lastReadErr = err
		if again || (errors.Is(err, merr.ErrCollectionNotLoaded) && node.shouldRetryDMLLoad(err)) {
			return true, err
		}
		terminalErr = err
		return false, err
	})
	// retry.Handle may return its previous error when canceled during backoff.
	if ctx.Err() != nil {
		return context.Cause(ctx)
	}
	// retry.Handle also returns its previous error when a later attempt stops on
	// a child-context cancellation or timeout. Preserve that attempt's terminal
	// error while the parent request context is still valid.
	if terminalErr != nil {
		return terminalErr
	}
	return err
}

func (node *Proxy) shouldRetryDMLLoad(err error) bool {
	if err == nil || !Params.ProxyCfg.EnableAutoLoad.GetAsBool() ||
		merr.IsCanceledOrTimeout(err) || merr.GetErrorType(err) == merr.InputError {
		return false
	}
	if errors.Is(err, merr.ErrCollectionNotLoaded) {
		return true
	}
	return errors.Is(err, merr.ErrQueryViewNotReady)
}

// ensureCollectionReady sends the load-and-wait RPC before executing a cold read.
// QueryCoord owns load state, concurrent load submission and readiness waiting.
func (node *Proxy) ensureCollectionReady(ctx context.Context, dbName, collectionName string) error {
	if err := merr.CheckHealthy(node.GetStateCode()); err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(ctx, Params.QueryCoordCfg.LoadTimeoutSeconds.GetAsDuration(time.Second))
	defer cancel()

	cache := node.GetMetaCache()
	collection, err := cache.GetCollectionInfo(ctx, dbName, collectionName, 0)
	if err != nil {
		return err
	}

	status, err := node.mixCoord.EnsureCollectionReady(ctx, &querypb.EnsureCollectionReadyRequest{
		CollectionID:      collection.CollID,
		ExpectedVchannels: collection.VChannels,
	})
	if ctx.Err() != nil {
		return context.Cause(ctx)
	}
	return merr.CheckRPCCall(status, err)
}

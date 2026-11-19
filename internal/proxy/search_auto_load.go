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
	var terminalErr error
	err := retry.Handle(ctx, func() (bool, error) {
		terminalErr = nil
		if err := ctx.Err(); err != nil {
			return false, context.Cause(ctx)
		}
		again, err := execute(ctx)
		if autoLoad && ctx.Err() == nil && errors.Is(err, merr.ErrCollectionNotLoaded) {
			if loadErr := node.ensureCollectionReady(ctx, dbName, collectionName); loadErr != nil {
				terminalErr = loadErr
				return false, loadErr
			}
			if ctx.Err() != nil {
				return false, context.Cause(ctx)
			}
			again, err = execute(ctx)
			if ctx.Err() == nil && errors.Is(err, merr.ErrCollectionNotLoaded) {
				return true, err
			}
		}
		if err != nil && !isIterator && ctx.Err() == nil && !merr.IsCanceledOrTimeout(err) &&
			merr.GetErrorType(err) != merr.InputError && refreshes < 3 && errors.Is(err, merr.ErrQueryViewNotReady) {
			refreshes++
			node.GetMetaCache().RemoveCollection(ctx, dbName, collectionName)
			again = true
		}
		if again {
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

// ensureCollectionReady sends the load-and-wait RPC before retrying a cold DQL.
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

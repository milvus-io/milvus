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

package querycoordv2

import (
	"context"
	"time"

	"github.com/cockroachdb/errors"

	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/retry"
)

const leftoverCollectionReleaseMaxSleep = time.Minute

// releaseLeftoverLoadedCollections releases the load meta of collections that are
// recovered as loaded but no longer exist in RootCoord.
//
// Such leftovers are never released by anything else: the drop path that should have
// released them has already finished, and they block every load-config replica change,
// because their replicas can never become serviceable.
// Only a confirmed ErrCollectionNotFound triggers the release; any other error skips
// the collection.
func (s *Server) releaseLeftoverLoadedCollections(ctx context.Context) {
	for _, collectionID := range s.meta.GetAll(ctx) {
		_, err := s.broker.DescribeCollection(ctx, collectionID)
		if err == nil || !errors.Is(err, merr.ErrCollectionNotFound) {
			continue
		}
		mlog.Warn(ctx, "loaded collection not found in RootCoord, release its leftover load meta",
			mlog.FieldCollectionID(collectionID))
		err = retry.Do(ctx, func() error {
			err := s.broadcastDropLoadConfigForLeftoverCollection(ctx, collectionID)
			if errors.Is(err, errReleaseCollectionNotLoaded) {
				return nil
			}
			return err
		}, retry.AttemptAlways(), retry.MaxSleepTime(leftoverCollectionReleaseMaxSleep))
		if err != nil {
			mlog.Warn(ctx, "failed to release leftover loaded collection",
				mlog.FieldCollectionID(collectionID), mlog.Err(err))
		}
	}
}

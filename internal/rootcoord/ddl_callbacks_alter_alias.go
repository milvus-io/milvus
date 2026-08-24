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

package rootcoord

import (
	"context"
	"strings"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
	"github.com/milvus-io/milvus/internal/distributed/streaming"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message/ce"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

// aliasNoOldTarget marks an AlterAlias-family broadcast that provably has no
// pre-alter target to evict (a CreateAlias). It is deliberately distinct from
// the zero value: an old rootcoord (produced before old_collection_id existed)
// leaves the field unset, and a new AlterAlias that cannot resolve its old
// target also leaves it 0 -- both of those MUST fall back to the proxy holder
// scan. So OldCollectionId==0 means "unknown old target, scan to be safe" and
// this sentinel means "definitely nothing to evict, do NOT scan". Keeping the
// zero value as the safe-scan path is what makes a replayed old-rootcoord
// AlterAlias still close its ghost after a rolling upgrade.
const aliasNoOldTarget = int64(-1)

func (c *Core) broadcastCreateAlias(ctx context.Context, req *milvuspb.CreateAliasRequest) error {
	req.DbName = strings.TrimSpace(req.DbName)
	req.Alias = strings.TrimSpace(req.Alias)
	req.CollectionName = strings.TrimSpace(req.CollectionName)
	broadcaster, err := startBroadcastWithDatabaseLock(ctx, req.GetDbName())
	if err != nil {
		return err
	}
	defer broadcaster.Close()

	if err := c.meta.CheckIfAliasCreatable(ctx, req.GetDbName(), req.GetAlias(), req.GetCollectionName()); err != nil {
		return err
	}

	db, err := c.meta.GetDatabaseByName(ctx, req.GetDbName(), typeutil.MaxTimestamp)
	if err != nil {
		return err
	}
	collection, err := c.meta.GetCollectionByName(ctx, req.GetDbName(), req.GetCollectionName(), typeutil.MaxTimestamp, false)
	if err != nil {
		return err
	}

	msg := message.NewAlterAliasMessageBuilderV2().
		WithHeader(&message.AlterAliasMessageHeader{
			DbId:           db.ID,
			DbName:         req.GetDbName(),
			CollectionId:   collection.CollectionID,
			Alias:          req.GetAlias(),
			CollectionName: req.GetCollectionName(),
			// CreateAlias has no pre-existing target: mark it explicitly so the
			// ack callback does NOT run the O(N) holder scan on every create.
			OldCollectionId: aliasNoOldTarget,
		}).
		WithBody(&message.AlterAliasMessageBody{}).
		WithBroadcast([]string{streaming.WAL().ControlChannel()}).
		MustBuildBroadcast()
	_, err = broadcaster.Broadcast(ctx, msg)
	return err
}

func (c *Core) broadcastAlterAlias(ctx context.Context, req *milvuspb.AlterAliasRequest) error {
	req.DbName = strings.TrimSpace(req.DbName)
	req.Alias = strings.TrimSpace(req.Alias)
	req.CollectionName = strings.TrimSpace(req.CollectionName)
	broadcaster, err := startBroadcastWithDatabaseLock(ctx, req.GetDbName())
	if err != nil {
		return err
	}
	defer broadcaster.Close()

	if err := c.meta.CheckIfAliasAlterable(ctx, req.GetDbName(), req.GetAlias(), req.GetCollectionName()); err != nil {
		return err
	}

	db, err := c.meta.GetDatabaseByName(ctx, req.GetDbName(), typeutil.MaxTimestamp)
	if err != nil {
		return err
	}
	collection, err := c.meta.GetCollectionByName(ctx, req.GetDbName(), req.GetCollectionName(), typeutil.MaxTimestamp, false)
	if err != nil {
		return err
	}

	// Resolve the alias's CURRENT (pre-alter) target under the same database
	// lock and carry it in the header, so the cache expiration can evict the old
	// target by id. The proxy cannot always resolve the old target itself: a
	// concurrent describe of the new target may re-point its alias resolution
	// before the expiration arrives, which would leave the old target's cached
	// Aliases list permanently stale (the proxy's holder-scan fallback only runs
	// for old-rootcoord broadcasts with CollectionID==0, so a partial message
	// with a nonzero new id and a zero old id silently disables the fix).
	//
	// GetCollectionByName resolves an alias to its target authoritatively (it
	// consults the alias map), allowUnavailable=true so a target already being
	// dropped still yields its id. CheckIfAliasAlterable above guarantees the
	// alias exists, so this normally resolves to a real (>0) id and the ack
	// callback evicts the old target by id -- no scan. If it genuinely cannot (a
	// meta inconsistency), DON'T fail the alter: leave the old target UNKNOWN
	// (zero) so the ack callback falls back to the proxy holder scan. Zero (not
	// a sentinel) is deliberately the safe-scan path -- it is also what an old
	// rootcoord (before old_collection_id existed) leaves in a replayed
	// AlterAlias, so both cases converge on the scan and neither reopens a
	// ghost. Only CreateAlias, which provably has no old target, sets the
	// aliasNoOldTarget sentinel to skip the scan. Computed at broadcast time so
	// message replay stays deterministic.
	oldCollectionID := int64(0)
	if oldColl, err := c.meta.GetCollectionByName(ctx, req.GetDbName(), req.GetAlias(), typeutil.MaxTimestamp, true); err == nil {
		oldCollectionID = oldColl.CollectionID
	} else {
		mlog.Warn(ctx, "could not resolve pre-alter alias target; proxy will holder-scan for it",
			mlog.String("db", req.GetDbName()), mlog.String("alias", req.GetAlias()), mlog.Err(err))
	}

	msg := message.NewAlterAliasMessageBuilderV2().
		WithHeader(&message.AlterAliasMessageHeader{
			DbId:            db.ID,
			DbName:          req.GetDbName(),
			CollectionId:    collection.CollectionID,
			Alias:           req.GetAlias(),
			CollectionName:  req.GetCollectionName(),
			OldCollectionId: oldCollectionID,
		}).
		WithBody(&message.AlterAliasMessageBody{}).
		WithBroadcast([]string{streaming.WAL().ControlChannel()}).
		MustBuildBroadcast()
	_, err = broadcaster.Broadcast(ctx, msg)
	return err
}

func (c *DDLCallback) alterAliasV2AckCallback(ctx context.Context, result message.BroadcastResultAlterAliasMessageV2) error {
	if err := c.meta.AlterAlias(ctx, result); err != nil {
		return err
	}
	header := result.Message.Header()
	builder := ce.NewBuilder().WithLegacyProxyCollectionMetaCache(
		ce.OptLPCMDBName(header.DbName),
		ce.OptLPCMCollectionName(header.Alias),
		// Forward the new target's collection id so the proxy also evicts the
		// canonical entry (and id index) of the collection now gaining the
		// alias, which the proxy cannot resolve from the alias name (the alias
		// did not point at it when it was cached). Without this, an id-only
		// Describe of the new target would serve a stale Aliases list.
		ce.OptLPCMCollectionID(header.CollectionId),
		ce.OptLPCMMsgType(commonpb.MsgType_AlterAlias))
	switch {
	case header.OldCollectionId > 0 && header.OldCollectionId != header.CollectionId:
		// Old target known and distinct from the new one: evict it by id. A
		// second expiration entry with no collection name -- the proxy's handler
		// then only runs the id eviction, no scan.
		builder = builder.WithLegacyProxyCollectionMetaCache(
			ce.OptLPCMDBName(header.DbName),
			ce.OptLPCMCollectionID(header.OldCollectionId),
			ce.OptLPCMMsgType(commonpb.MsgType_AlterAlias))
	case header.OldCollectionId == 0:
		// Old target UNKNOWN. Two ways to land here, both must scan: (a) a new
		// AlterAlias whose broadcast could not resolve the pre-alter target (meta
		// inconsistency), and (b) a replayed old-rootcoord AlterAlias that
		// predates old_collection_id (field defaults to 0). Emit a
		// CollectionID==0 alias-name entry so the proxy runs its holder-scan
		// fallback and evicts any cached collection still declaring the alias.
		//
		// CreateAlias sets the aliasNoOldTarget (<0) sentinel and takes neither
		// branch: it provably has no old target, so it must never trigger the
		// O(N) holder scan.
		builder = builder.WithLegacyProxyCollectionMetaCache(
			ce.OptLPCMDBName(header.DbName),
			ce.OptLPCMCollectionName(header.Alias),
			ce.OptLPCMCollectionID(0),
			ce.OptLPCMMsgType(commonpb.MsgType_AlterAlias))
	}
	return c.ExpireCaches(ctx, builder)
}

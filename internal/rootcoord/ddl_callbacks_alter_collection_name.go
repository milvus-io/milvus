package rootcoord

import (
	"context"
	"fmt"

	"google.golang.org/protobuf/types/known/fieldmaskpb"

	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
	"github.com/milvus-io/milvus/internal/distributed/streaming"
	"github.com/milvus-io/milvus/internal/streamingcoord/server/broadcaster/broadcast"
	"github.com/milvus-io/milvus/internal/util/hookutil"
	"github.com/milvus-io/milvus/pkg/v2/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v2/util"
	"github.com/milvus-io/milvus/pkg/v2/util/merr"
	"github.com/milvus-io/milvus/pkg/v2/util/typeutil"
)

func (c *Core) broadcastAlterCollectionForRenameCollection(ctx context.Context, req *milvuspb.RenameCollectionRequest) error {
	if req.DbName == "" {
		req.DbName = util.DefaultDBName
	}
	if req.NewDBName == "" {
		req.NewDBName = req.DbName
	}
	if req.NewName == "" {
		return merr.WrapErrParameterInvalidMsg("new collection name should not be empty")
	}
	if req.OldName == "" {
		return merr.WrapErrParameterInvalidMsg("old collection name should not be empty")
	}
	if req.DbName == req.NewDBName && req.OldName == req.NewName {
		// no-op here.
		return merr.WrapErrParameterInvalidMsg("collection name or database name should be different")
	}

	// StartBroadcastWithResourceKeys will deduplicate the resource keys itself, so it's safe to add all the resource keys here.
	rks := []message.ResourceKey{
		message.NewExclusiveDBNameResourceKey(req.GetNewDBName()),
		message.NewExclusiveDBNameResourceKey(req.GetDbName()),
	}
	broadcaster, err := broadcast.StartBroadcastWithResourceKeys(ctx, rks...)
	if err != nil {
		return err
	}
	defer broadcaster.Close()

	if err := c.validateEncryption(ctx, req.GetDbName(), req.GetNewDBName()); err != nil {
		return err
	}

	if err := c.meta.CheckIfCollectionRenamable(ctx, req.GetDbName(), req.GetOldName(), req.GetNewDBName(), req.GetNewName()); err != nil {
		return err
	}

	newDB, err := c.meta.GetDatabaseByName(ctx, req.GetNewDBName(), typeutil.MaxTimestamp)
	if err != nil {
		return err
	}
	coll, err := c.meta.GetCollectionByName(ctx, req.GetDbName(), req.GetOldName(), typeutil.MaxTimestamp)
	if err != nil {
		return err
	}

	updateMask := &fieldmaskpb.FieldMask{
		Paths: []string{},
	}
	updates := &message.AlterCollectionMessageUpdates{}

	if req.GetNewDBName() != req.GetDbName() {
		updates.DbName = newDB.Name
		updates.DbId = newDB.ID
		updateMask.Paths = append(updateMask.Paths, message.FieldMaskDB)
	}
	if req.GetNewName() != req.GetOldName() {
		updates.CollectionName = req.GetNewName()
		updateMask.Paths = append(updateMask.Paths, message.FieldMaskCollectionName)
	}

	channels := make([]string, 0, len(coll.VirtualChannelNames)+1)
	channels = append(channels, streaming.WAL().ControlChannel())
	channels = append(channels, coll.VirtualChannelNames...)
	cacheExpirations, err := c.getCacheExpireForCollection(ctx, req.GetDbName(), req.GetOldName())
	if err != nil {
		return err
	}

	msg := message.NewAlterCollectionMessageBuilderV2().
		WithHeader(&message.AlterCollectionMessageHeader{
			DbId:             coll.DBID,
			CollectionId:     coll.CollectionID,
			UpdateMask:       updateMask,
			CacheExpirations: cacheExpirations,
		}).
		WithBody(&message.AlterCollectionMessageBody{
			Updates: updates,
		}).
		WithBroadcast(channels).
		MustBuildBroadcast()
	_, err = broadcaster.Broadcast(ctx, msg)
	return err
}

func (c *Core) validateEncryption(ctx context.Context, oldDBName string, newDBName string) error {
	if oldDBName == newDBName {
		return nil
	}
	// Check if renaming across databases with encryption enabled

	// old and new DB names are filled in Prepare, shouldn't be empty here
	originalDB, err := c.meta.GetDatabaseByName(ctx, oldDBName, typeutil.MaxTimestamp)
	if err != nil {
		return fmt.Errorf("failed to get original database: %w", err)
	}

	targetDB, err := c.meta.GetDatabaseByName(ctx, newDBName, typeutil.MaxTimestamp)
	if err != nil {
		return fmt.Errorf("target database %s not found: %w", newDBName, err)
	}

	// Check if either database has encryption enabled
	if hookutil.IsDBEncryptionEnabled(originalDB.Properties) || hookutil.IsDBEncryptionEnabled(targetDB.Properties) {
		return fmt.Errorf("deny to change collection databases due to at least one database enabled encryption, original DB: %s, target DB: %s", oldDBName, newDBName)
	}

	return nil
}

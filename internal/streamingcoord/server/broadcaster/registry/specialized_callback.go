package registry

import (
	"github.com/milvus-io/milvus-proto/go-api/v2/msgpb"
	"github.com/milvus-io/milvus/pkg/v2/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v2/util/syncutil"
)

// init the message ack callbacks
func init() {
	resetMessageAckOnceCallbacks()
	resetMessageAckCallbacks()
	resetMessageCheckCallbacks()
}

var RegisterImportV1CheckCallback = registerMessageCheckCallback[*message.ImportMessageHeader, *msgpb.ImportMsg]

// resetMessageCheckCallbacks resets the message check callbacks.
func resetMessageCheckCallbacks() {
	messageCheckCallbacks = map[message.MessageTypeWithVersion]*syncutil.Future[messageInnerCheckCallback]{
		message.MessageTypeImportV1: syncutil.NewFuture[messageInnerCheckCallback](),
	}
}

var (
	RegisterImportV1AckCallback              = registerMessageAckCallback[*message.ImportMessageHeader, *msgpb.ImportMsg]
	RegisterBatchUpdateManifestV2AckCallback = registerMessageAckCallback[*message.BatchUpdateManifestMessageHeader, *message.BatchUpdateManifestMessageBody]

	// Cluster
	RegisterAlterReplicateConfigV2AckCallback = registerMessageAckCallback[*message.AlterReplicateConfigMessageHeader, *message.AlterReplicateConfigMessageBody]
	RegisterFlushAllV2AckCallback             = registerMessageAckCallback[*message.FlushAllMessageHeader, *message.FlushAllMessageBody]
	RegisterAlterWALV2AckCallback             = registerMessageAckCallback[*message.AlterWALMessageHeader, *message.AlterWALMessageBody]

	// Collection
	RegisterAlterCollectionV2AckCallback    = registerMessageAckCallback[*message.AlterCollectionMessageHeader, *message.AlterCollectionMessageBody]
	RegisterCreateCollectionV1AckCallback   = registerMessageAckCallback[*message.CreateCollectionMessageHeader, *message.CreateCollectionRequest]
	RegisterDropCollectionV1AckCallback     = registerMessageAckCallback[*message.DropCollectionMessageHeader, *message.DropCollectionRequest]
	RegisterTruncateCollectionV2AckCallback = registerMessageAckCallback[*message.TruncateCollectionMessageHeader, *message.TruncateCollectionMessageBody]
	RegisterAlterLoadConfigV2AckCallback    = registerMessageAckCallback[*message.AlterLoadConfigMessageHeader, *message.AlterLoadConfigMessageBody]
	RegisterDropLoadConfigV2AckCallback     = registerMessageAckCallback[*message.DropLoadConfigMessageHeader, *message.DropLoadConfigMessageBody]

	// Partition
	RegisterCreatePartitionV1AckCallback = registerMessageAckCallback[*message.CreatePartitionMessageHeader, *message.CreatePartitionRequest]
	RegisterDropPartitionV1AckCallback   = registerMessageAckCallback[*message.DropPartitionMessageHeader, *message.DropPartitionRequest]

	// Database
	RegisterCreateDatabaseV2AckCallback = registerMessageAckCallback[*message.CreateDatabaseMessageHeader, *message.CreateDatabaseMessageBody]
	RegisterAlterDatabaseV2AckCallback  = registerMessageAckCallback[*message.AlterDatabaseMessageHeader, *message.AlterDatabaseMessageBody]
	RegisterDropDatabaseV2AckCallback   = registerMessageAckCallback[*message.DropDatabaseMessageHeader, *message.DropDatabaseMessageBody]

	// Alias
	RegisterAlterAliasV2AckCallback = registerMessageAckCallback[*message.AlterAliasMessageHeader, *message.AlterAliasMessageBody]
	RegisterDropAliasV2AckCallback  = registerMessageAckCallback[*message.DropAliasMessageHeader, *message.DropAliasMessageBody]

	// Index
	RegisterCreateIndexV2AckCallback = registerMessageAckCallback[*message.CreateIndexMessageHeader, *message.CreateIndexMessageBody]
	RegisterAlterIndexV2AckCallback  = registerMessageAckCallback[*message.AlterIndexMessageHeader, *message.AlterIndexMessageBody]
	RegisterDropIndexV2AckCallback   = registerMessageAckCallback[*message.DropIndexMessageHeader, *message.DropIndexMessageBody]

	// RBAC
	RegisterAlterUserV2AckCallback           = registerMessageAckCallback[*message.AlterUserMessageHeader, *message.AlterUserMessageBody]
	RegisterDropUserV2AckCallback            = registerMessageAckCallback[*message.DropUserMessageHeader, *message.DropUserMessageBody]
	RegisterAlterRoleV2AckCallback           = registerMessageAckCallback[*message.AlterRoleMessageHeader, *message.AlterRoleMessageBody]
	RegisterDropRoleV2AckCallback            = registerMessageAckCallback[*message.DropRoleMessageHeader, *message.DropRoleMessageBody]
	RegisterAlterUserRoleV2AckCallback       = registerMessageAckCallback[*message.AlterUserRoleMessageHeader, *message.AlterUserRoleMessageBody]
	RegisterDropUserRoleV2AckCallback        = registerMessageAckCallback[*message.DropUserRoleMessageHeader, *message.DropUserRoleMessageBody]
	RegisterAlterPrivilegeV2AckCallback      = registerMessageAckCallback[*message.AlterPrivilegeMessageHeader, *message.AlterPrivilegeMessageBody]
	RegisterDropPrivilegeV2AckCallback       = registerMessageAckCallback[*message.DropPrivilegeMessageHeader, *message.DropPrivilegeMessageBody]
	RegisterAlterPrivilegeGroupV2AckCallback = registerMessageAckCallback[*message.AlterPrivilegeGroupMessageHeader, *message.AlterPrivilegeGroupMessageBody]
	RegisterDropPrivilegeGroupV2AckCallback  = registerMessageAckCallback[*message.DropPrivilegeGroupMessageHeader, *message.DropPrivilegeGroupMessageBody]
	RegisterRestoreRBACV2AckCallback         = registerMessageAckCallback[*message.RestoreRBACMessageHeader, *message.RestoreRBACMessageBody]

	// Resource Group
	RegisterAlterResourceGroupV2AckCallback = registerMessageAckCallback[*message.AlterResourceGroupMessageHeader, *message.AlterResourceGroupMessageBody]
	RegisterDropResourceGroupV2AckCallback  = registerMessageAckCallback[*message.DropResourceGroupMessageHeader, *message.DropResourceGroupMessageBody]

	// Snapshot
	RegisterCreateSnapshotV2AckCallback  = registerMessageAckCallback[*message.CreateSnapshotMessageHeader, *message.CreateSnapshotMessageBody]
	RegisterDropSnapshotV2AckCallback    = registerMessageAckCallback[*message.DropSnapshotMessageHeader, *message.DropSnapshotMessageBody]
	RegisterRestoreSnapshotV2AckCallback = registerMessageAckCallback[*message.RestoreSnapshotMessageHeader, *message.RestoreSnapshotMessageBody]
)

// resetMessageAckCallbacks resets the message ack callbacks.
func resetMessageAckCallbacks() {
	messageAckCallbacks = map[message.MessageTypeWithVersion]*syncutil.Future[messageInnerAckCallback]{
		message.MessageTypeImportV1:              syncutil.NewFuture[messageInnerAckCallback](),
		message.MessageTypeBatchUpdateManifestV2: syncutil.NewFuture[messageInnerAckCallback](),

		// Cluster
		message.MessageTypeAlterReplicateConfigV2: syncutil.NewFuture[messageInnerAckCallback](),
		message.MessageTypeFlushAllV2:             syncutil.NewFuture[messageInnerAckCallback](),
		message.MessageTypeAlterWALV2:             syncutil.NewFuture[messageInnerAckCallback](),

		// Collection
		message.MessageTypeAlterCollectionV2:    syncutil.NewFuture[messageInnerAckCallback](),
		message.MessageTypeCreateCollectionV1:   syncutil.NewFuture[messageInnerAckCallback](),
		message.MessageTypeDropCollectionV1:     syncutil.NewFuture[messageInnerAckCallback](),
		message.MessageTypeTruncateCollectionV2: syncutil.NewFuture[messageInnerAckCallback](),
		message.MessageTypeAlterLoadConfigV2:    syncutil.NewFuture[messageInnerAckCallback](),
		message.MessageTypeDropLoadConfigV2:     syncutil.NewFuture[messageInnerAckCallback](),

		// Partition
		message.MessageTypeCreatePartitionV1: syncutil.NewFuture[messageInnerAckCallback](),
		message.MessageTypeDropPartitionV1:   syncutil.NewFuture[messageInnerAckCallback](),

		// Database
		message.MessageTypeCreateDatabaseV2: syncutil.NewFuture[messageInnerAckCallback](),
		message.MessageTypeAlterDatabaseV2:  syncutil.NewFuture[messageInnerAckCallback](),
		message.MessageTypeDropDatabaseV2:   syncutil.NewFuture[messageInnerAckCallback](),

		// Alias
		message.MessageTypeAlterAliasV2: syncutil.NewFuture[messageInnerAckCallback](),
		message.MessageTypeDropAliasV2:  syncutil.NewFuture[messageInnerAckCallback](),

		// Index
		message.MessageTypeCreateIndexV2: syncutil.NewFuture[messageInnerAckCallback](),
		message.MessageTypeAlterIndexV2:  syncutil.NewFuture[messageInnerAckCallback](),
		message.MessageTypeDropIndexV2:   syncutil.NewFuture[messageInnerAckCallback](),

		// RBAC
		message.MessageTypeAlterUserV2:           syncutil.NewFuture[messageInnerAckCallback](),
		message.MessageTypeDropUserV2:            syncutil.NewFuture[messageInnerAckCallback](),
		message.MessageTypeAlterRoleV2:           syncutil.NewFuture[messageInnerAckCallback](),
		message.MessageTypeDropRoleV2:            syncutil.NewFuture[messageInnerAckCallback](),
		message.MessageTypeAlterUserRoleV2:       syncutil.NewFuture[messageInnerAckCallback](),
		message.MessageTypeDropUserRoleV2:        syncutil.NewFuture[messageInnerAckCallback](),
		message.MessageTypeAlterPrivilegeV2:      syncutil.NewFuture[messageInnerAckCallback](),
		message.MessageTypeDropPrivilegeV2:       syncutil.NewFuture[messageInnerAckCallback](),
		message.MessageTypeAlterPrivilegeGroupV2: syncutil.NewFuture[messageInnerAckCallback](),
		message.MessageTypeDropPrivilegeGroupV2:  syncutil.NewFuture[messageInnerAckCallback](),
		message.MessageTypeRestoreRBACV2:         syncutil.NewFuture[messageInnerAckCallback](),

		// Resource Group
		message.MessageTypeAlterResourceGroupV2: syncutil.NewFuture[messageInnerAckCallback](),
		message.MessageTypeDropResourceGroupV2:  syncutil.NewFuture[messageInnerAckCallback](),

		// Snapshot
		message.MessageTypeCreateSnapshotV2:  syncutil.NewFuture[messageInnerAckCallback](),
		message.MessageTypeDropSnapshotV2:    syncutil.NewFuture[messageInnerAckCallback](),
		message.MessageTypeRestoreSnapshotV2: syncutil.NewFuture[messageInnerAckCallback](),
	}
}

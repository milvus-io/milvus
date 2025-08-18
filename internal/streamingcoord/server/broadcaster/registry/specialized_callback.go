package registry

import (
	"github.com/milvus-io/milvus-proto/go-api/v2/msgpb"
	"github.com/milvus-io/milvus/pkg/v2/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v2/util/syncutil"
)

// init the message ack callbacks
func init() {
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
	RegisterImportV1AckCallback = registerMessageAckCallback[*message.ImportMessageHeader, *msgpb.ImportMsg]

	// Cluster
	RegisterPutReplicateConfigV2AckCallback = registerMessageAckCallback[*message.PutReplicateConfigMessageHeader, *message.PutReplicateConfigMessageBody]

	// Collection
	RegisterPutCollectionV2AckCallback    = registerMessageAckCallback[*message.PutCollectionMessageHeader, *message.PutCollectionMessageBody]
	RegisterCreateCollectionV1AckCallback = registerMessageAckCallback[*message.CreateCollectionMessageHeader, *message.CreateCollectionRequest]
	RegisterDropCollectionV1AckCallback   = registerMessageAckCallback[*message.DropCollectionMessageHeader, *message.DropCollectionRequest]
	RegisterPutLoadConfigV2AckCallback    = registerMessageAckCallback[*message.PutLoadConfigMessageHeader, *message.PutLoadConfigMessageBody]
	RegisterDropLoadConfigV2AckCallback   = registerMessageAckCallback[*message.DropLoadConfigMessageHeader, *message.DropLoadConfigMessageBody]

	// Partition
	RegisterCreatePartitionV1AckCallback = registerMessageAckCallback[*message.CreatePartitionMessageHeader, *message.CreatePartitionRequest]
	RegisterDropPartitionV1AckCallback   = registerMessageAckCallback[*message.DropPartitionMessageHeader, *message.DropPartitionRequest]

	// Database
	RegisterCreateDatabaseV2AckCallback = registerMessageAckCallback[*message.CreateDatabaseMessageHeader, *message.CreateDatabaseMessageBody]
	RegisterPutDatabaseV2AckCallback    = registerMessageAckCallback[*message.PutDatabaseMessageHeader, *message.PutDatabaseMessageBody]
	RegisterDropDatabaseV2AckCallback   = registerMessageAckCallback[*message.DropDatabaseMessageHeader, *message.DropDatabaseMessageBody]

	// Alias
	RegisterPutAliasV2AckCallback  = registerMessageAckCallback[*message.PutAliasMessageHeader, *message.PutAliasMessageBody]
	RegisterDropAliasV2AckCallback = registerMessageAckCallback[*message.DropAliasMessageHeader, *message.DropAliasMessageBody]

	// Index
	RegisterCreateIndexV2AckCallback = registerMessageAckCallback[*message.CreateIndexMessageHeader, *message.CreateIndexMessageBody]
	RegisterAlterIndexV2AckCallback  = registerMessageAckCallback[*message.AlterIndexMessageHeader, *message.AlterIndexMessageBody]
	RegisterDropIndexV2AckCallback   = registerMessageAckCallback[*message.DropIndexMessageHeader, *message.DropIndexMessageBody]

	// RBAC
	RegisterPutUserV2AckCallback            = registerMessageAckCallback[*message.PutUserMessageHeader, *message.PutUserMessageBody]
	RegisterDropUserV2AckCallback           = registerMessageAckCallback[*message.DropUserMessageHeader, *message.DropUserMessageBody]
	RegisterPutRoleV2AckCallback            = registerMessageAckCallback[*message.PutRoleMessageHeader, *message.PutRoleMessageBody]
	RegisterDropRoleV2AckCallback           = registerMessageAckCallback[*message.DropRoleMessageHeader, *message.DropRoleMessageBody]
	RegisterPutUserRoleV2AckCallback        = registerMessageAckCallback[*message.PutUserRoleMessageHeader, *message.PutUserRoleMessageBody]
	RegisterDropUserRoleV2AckCallback       = registerMessageAckCallback[*message.DropUserRoleMessageHeader, *message.DropUserRoleMessageBody]
	RegisterGrantPrivilegeV2AckCallback     = registerMessageAckCallback[*message.GrantPrivilegeMessageHeader, *message.GrantPrivilegeMessageBody]
	RegisterRevokePrivilegeV2AckCallback    = registerMessageAckCallback[*message.RevokePrivilegeMessageHeader, *message.RevokePrivilegeMessageBody]
	RegisterPutPrivilegeGroupV2AckCallback  = registerMessageAckCallback[*message.PutPrivilegeGroupMessageHeader, *message.PutPrivilegeGroupMessageBody]
	RegisterDropPrivilegeGroupV2AckCallback = registerMessageAckCallback[*message.DropPrivilegeGroupMessageHeader, *message.DropPrivilegeGroupMessageBody]
	RegisterRestoreRBACV2AckCallback        = registerMessageAckCallback[*message.RestoreRBACMessageHeader, *message.RestoreRBACMessageBody]

	// Resource Group
	RegisterPutResourceGroupV2AckCallback  = registerMessageAckCallback[*message.PutResourceGroupMessageHeader, *message.PutResourceGroupMessageBody]
	RegisterDropResourceGroupV2AckCallback = registerMessageAckCallback[*message.DropResourceGroupMessageHeader, *message.DropResourceGroupMessageBody]
)

// resetMessageAckCallbacks resets the message ack callbacks.
func resetMessageAckCallbacks() {
	messageAckCallbacks = map[message.MessageTypeWithVersion]*syncutil.Future[messageInnerAckCallback]{
		message.MessageTypeImportV1: syncutil.NewFuture[messageInnerAckCallback](),

		// Cluster
		message.MessageTypePutReplicateConfigV2: syncutil.NewFuture[messageInnerAckCallback](),

		// Collection
		message.MessageTypePutCollectionV2:    syncutil.NewFuture[messageInnerAckCallback](),
		message.MessageTypeCreateCollectionV1: syncutil.NewFuture[messageInnerAckCallback](),
		message.MessageTypeDropCollectionV1:   syncutil.NewFuture[messageInnerAckCallback](),
		message.MessageTypePutLoadConfigV2:    syncutil.NewFuture[messageInnerAckCallback](),
		message.MessageTypeDropLoadConfigV2:   syncutil.NewFuture[messageInnerAckCallback](),

		// Partition
		message.MessageTypeCreatePartitionV1: syncutil.NewFuture[messageInnerAckCallback](),
		message.MessageTypeDropPartitionV1:   syncutil.NewFuture[messageInnerAckCallback](),

		// Database
		message.MessageTypeCreateDatabaseV2: syncutil.NewFuture[messageInnerAckCallback](),
		message.MessageTypePutDatabaseV2:    syncutil.NewFuture[messageInnerAckCallback](),
		message.MessageTypeDropDatabaseV2:   syncutil.NewFuture[messageInnerAckCallback](),

		// Alias
		message.MessageTypePutAliasV2:  syncutil.NewFuture[messageInnerAckCallback](),
		message.MessageTypeDropAliasV2: syncutil.NewFuture[messageInnerAckCallback](),

		// Index
		message.MessageTypeCreateIndexV2: syncutil.NewFuture[messageInnerAckCallback](),
		message.MessageTypeAlterIndexV2:  syncutil.NewFuture[messageInnerAckCallback](),
		message.MessageTypeDropIndexV2:   syncutil.NewFuture[messageInnerAckCallback](),

		// RBAC
		message.MessageTypePutUserV2:            syncutil.NewFuture[messageInnerAckCallback](),
		message.MessageTypeDropUserV2:           syncutil.NewFuture[messageInnerAckCallback](),
		message.MessageTypePutRoleV2:            syncutil.NewFuture[messageInnerAckCallback](),
		message.MessageTypeDropRoleV2:           syncutil.NewFuture[messageInnerAckCallback](),
		message.MessageTypePutUserRoleV2:        syncutil.NewFuture[messageInnerAckCallback](),
		message.MessageTypeDropUserRoleV2:       syncutil.NewFuture[messageInnerAckCallback](),
		message.MessageTypeGrantPrivilegeV2:     syncutil.NewFuture[messageInnerAckCallback](),
		message.MessageTypeRevokePrivilegeV2:    syncutil.NewFuture[messageInnerAckCallback](),
		message.MessageTypePutPrivilegeGroupV2:  syncutil.NewFuture[messageInnerAckCallback](),
		message.MessageTypeDropPrivilegeGroupV2: syncutil.NewFuture[messageInnerAckCallback](),
		message.MessageTypeRestoreRBACV2:        syncutil.NewFuture[messageInnerAckCallback](),

		// Resource Group
		message.MessageTypePutResourceGroupV2:  syncutil.NewFuture[messageInnerAckCallback](),
		message.MessageTypeDropResourceGroupV2: syncutil.NewFuture[messageInnerAckCallback](),
	}
}

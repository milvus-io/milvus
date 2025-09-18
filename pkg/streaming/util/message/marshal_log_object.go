package message

import (
	"fmt"
	"reflect"
	"strconv"
	"strings"

	"go.uber.org/zap/zapcore"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v2/rgpb"
)

// MarshalLogObject encodes the message into zap log object.
func (m *messageImpl) MarshalLogObject(enc zapcore.ObjectEncoder) error {
	if m == nil {
		return nil
	}
	enc.AddString("type", m.MessageType().String())
	if m.properties.Exist(messageVChannel) {
		enc.AddString("vchannel", m.VChannel())
	}
	if m.properties.Exist(messageTimeTick) {
		enc.AddUint64("timetick", m.TimeTick())
	}
	if txn := m.TxnContext(); txn != nil {
		enc.AddInt64("txnID", int64(txn.TxnID))
	}
	if broadcast := m.BroadcastHeader(); broadcast != nil {
		enc.AddInt64("broadcastID", int64(broadcast.BroadcastID))
		enc.AddString("broadcastVChannels", strings.Join(broadcast.VChannels, ","))
	}
	if replicate := m.ReplicateHeader(); replicate != nil {
		enc.AddString("rClusterID", replicate.ClusterID)
		enc.AddString("rMessageID", replicate.MessageID.String())
		enc.AddString("rLastConfirmedMessageID", replicate.LastConfirmedMessageID.String())
		enc.AddUint64("rTimeTick", replicate.TimeTick)
		enc.AddString("rVchannel", replicate.VChannel)
	}
	enc.AddInt("size", len(m.payload))
	marshalSpecializedHeader(m.MessageType(), m.Version(), m.properties[messageHeader], enc)
	return nil
}

// MarshalLogObject encodes the immutable message into zap log object.
func (m *immutableMessageImpl) MarshalLogObject(enc zapcore.ObjectEncoder) error {
	if m == nil {
		return nil
	}
	enc.AddString("type", m.MessageType().String())
	enc.AddString("vchannel", m.VChannel())
	enc.AddUint64("timetick", m.TimeTick())
	enc.AddString("messageID", m.MessageID().String())
	enc.AddString("lastConfirmed", m.LastConfirmedMessageID().String())
	if txn := m.TxnContext(); txn != nil {
		enc.AddInt64("txnID", int64(txn.TxnID))
	}
	if broadcast := m.BroadcastHeader(); broadcast != nil {
		enc.AddInt64("broadcastID", int64(broadcast.BroadcastID))
	}
	enc.AddInt("size", len(m.payload))
	marshalSpecializedHeader(m.MessageType(), m.Version(), m.properties[messageHeader], enc)
	return nil
}

func (m *immutableTxnMessageImpl) MarshalLogObject(enc zapcore.ObjectEncoder) error {
	if m == nil {
		return nil
	}
	enc.AddArray("txn", zapcore.ArrayMarshalerFunc(func(enc zapcore.ArrayEncoder) error {
		txnMessage := AsImmutableTxnMessage(m)
		txnMessage.RangeOver(func(im ImmutableMessage) error {
			enc.AppendObject(im)
			return nil
		})
		return nil
	}))
	return nil
}

// marshalSpecializedHeader marshals the specialized header of the message.
func marshalSpecializedHeader(t MessageType, v Version, h string, enc zapcore.ObjectEncoder) {
	typ, ok := GetSerializeType(NewMessageTypeWithVersion(t, v))
	if !ok {
		enc.AddString("headerDecodeError", fmt.Sprintf("message type %s version %d not found", t, v))
		return
	}
	// must be a proto type.
	header := reflect.New(typ.HeaderType.Elem()).Interface().(proto.Message)
	if err := DecodeProto(h, header); err != nil {
		enc.AddString("headerDecodeError", err.Error())
		return
	}
	switch header := header.(type) {
	case *InsertMessageHeader:
		enc.AddInt64("collectionID", header.GetCollectionId())
		segmentIDs := make([]string, 0, len(header.GetPartitions()))
		rows := make([]string, 0)
		for _, partition := range header.GetPartitions() {
			segmentIDs = append(segmentIDs, strconv.FormatInt(partition.GetSegmentAssignment().GetSegmentId(), 10))
			rows = append(rows, strconv.FormatUint(partition.Rows, 10))
		}
		enc.AddString("segmentIDs", strings.Join(segmentIDs, "|"))
		enc.AddString("rows", strings.Join(rows, "|"))
	case *DeleteMessageHeader:
		enc.AddInt64("collectionID", header.GetCollectionId())
		enc.AddUint64("rows", header.GetRows())
	case *CreateCollectionMessageHeader:
		enc.AddInt64("collectionID", header.GetCollectionId())
	case *DropCollectionMessageHeader:
		enc.AddInt64("collectionID", header.GetCollectionId())
	case *CreatePartitionMessageHeader:
		enc.AddInt64("collectionID", header.GetCollectionId())
		enc.AddInt64("partitionID", header.GetPartitionId())
	case *DropPartitionMessageHeader:
		enc.AddInt64("collectionID", header.GetCollectionId())
		enc.AddInt64("partitionID", header.GetPartitionId())
	case *CreateSegmentMessageHeader:
		enc.AddInt64("collectionID", header.GetCollectionId())
		enc.AddInt64("segmentID", header.GetSegmentId())
		enc.AddInt64("lv", int64(header.GetLevel()))
	case *FlushMessageHeader:
		enc.AddInt64("collectionID", header.GetCollectionId())
		enc.AddInt64("segmentID", header.GetSegmentId())
	case *ManualFlushMessageHeader:
		enc.AddInt64("collectionID", header.GetCollectionId())
		encodeIDs(header.GetSegmentIds(), enc)
	case *SchemaChangeMessageHeader:
		enc.AddInt64("collectionID", header.GetCollectionId())
		encodeIDs(header.GetFlushedSegmentIds(), enc)
	case *AlterCollectionMessageHeader:
		enc.AddInt64("collectionID", header.GetCollectionId())
		enc.AddString("udpateMasks", strings.Join(header.UpdateMask.GetPaths(), "|"))
		encodeIDs(header.GetFlushedSegmentIds(), enc)
	case *AlterLoadConfigMessageHeader:
		enc.AddInt64("collectionID", header.GetCollectionId())
		enc.AddInt64("replicaNumber", int64(len(header.GetReplicas())))
	case *DropLoadConfigMessageHeader:
		enc.AddInt64("collectionID", header.GetCollectionId())
	case *CreateDatabaseMessageHeader:
		enc.AddString("dbName", header.GetDbName())
		enc.AddInt64("dbID", header.GetDbId())
	case *AlterDatabaseMessageHeader:
		enc.AddString("dbName", header.GetDbName())
		enc.AddInt64("dbID", header.GetDbId())
	case *DropDatabaseMessageHeader:
		enc.AddString("dbName", header.GetDbName())
		enc.AddInt64("dbID", header.GetDbId())
	case *AlterAliasMessageHeader:
		enc.AddString("dbName", header.GetDbName())
		enc.AddInt64("dbID", header.GetDbId())
		enc.AddInt64("collectionID", header.GetCollectionId())
		enc.AddString("collectionName", header.GetCollectionName())
		enc.AddString("alias", header.GetAlias())
	case *DropAliasMessageHeader:
		enc.AddString("dbName", header.GetDbName())
		enc.AddInt64("dbID", header.GetDbId())
		enc.AddString("alias", header.GetAlias())
	case *AlterUserMessageHeader:
		enc.AddString("user", header.GetUserEntity().GetName())
	case *DropUserMessageHeader:
		enc.AddString("user", header.GetUserName())
	case *AlterRoleMessageHeader:
		enc.AddString("role", header.GetRoleEntity().GetName())
	case *DropRoleMessageHeader:
		enc.AddString("role", header.GetRoleName())
	case *AlterUserRoleMessageHeader:
		enc.AddString("user", header.GetRoleBinding().GetUserEntity().GetName())
		enc.AddString("role", header.GetRoleBinding().GetRoleEntity().GetName())
	case *DropUserRoleMessageHeader:
		enc.AddString("user", header.GetRoleBinding().GetUserEntity().GetName())
		enc.AddString("role", header.GetRoleBinding().GetRoleEntity().GetName())
	case *AlterPrivilegeMessageHeader:
	case *DropPrivilegeMessageHeader:
	case *AlterPrivilegeGroupMessageHeader:
	case *DropPrivilegeGroupMessageHeader:
	case *CreateIndexMessageHeader:
		enc.AddInt64("collectionID", header.GetCollectionId())
		enc.AddInt64("fieldID", header.GetFieldId())
		enc.AddInt64("indexID", header.GetIndexId())
		enc.AddString("indexName", header.GetIndexName())
	case *AlterIndexMessageHeader:
		enc.AddInt64("collectionID", header.GetCollectionId())
		encodeIDs(header.GetIndexIds(), enc)
	case *DropIndexMessageHeader:
		enc.AddInt64("collectionID", header.GetCollectionId())
		encodeIDs(header.GetIndexIds(), enc)
	case *ImportMessageHeader:
	case *AlterResourceGroupMessageHeader:
		encodeResourceGroupConfigs(header.GetResourceGroupConfigs(), enc)
	case *DropResourceGroupMessageHeader:
		enc.AddString("rg", header.GetResourceGroupName())
	case *AlterReplicateConfigMessageHeader:
		for idx, topology := range header.GetReplicateConfiguration().GetCrossClusterTopology() {
			enc.AddString(fmt.Sprintf("topo_%d", idx), fmt.Sprintf("%s->%s", topology.GetSourceClusterId(), topology.GetTargetClusterId()))
		}
	}
}

func encodeIDs(targetIDs []int64, enc zapcore.ObjectEncoder) {
	ids := make([]string, 0, len(targetIDs))
	for _, id := range targetIDs {
		ids = append(ids, strconv.FormatInt(id, 10))
	}
	enc.AddString("segmentIDs", strings.Join(ids, "|"))
}

func encodeResourceGroupConfigs(configs map[string]*rgpb.ResourceGroupConfig, enc zapcore.ObjectEncoder) {
	strs := make([]string, 0, len(configs))
	for name, config := range configs {
		strs = append(strs, fmt.Sprintf(
			"%s:rn%dln%d", name, config.GetRequests().GetNodeNum(), config.GetLimits().GetNodeNum()),
		)
	}
	enc.AddString("rgs", strings.Join(strs, "|"))
}

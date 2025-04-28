package message

import (
	"reflect"
	"strconv"
	"strings"

	"go.uber.org/zap/zapcore"
	"google.golang.org/protobuf/proto"
)

// MarshalLogObject encodes the message into zap log object.
func (m *messageImpl) MarshalLogObject(enc zapcore.ObjectEncoder) error {
	if m == nil {
		return nil
	}
	enc.AddString("type", m.MessageType().String())
	enc.AddString("vchannel", m.VChannel())
	if m.properties.Exist(messageTimeTick) {
		enc.AddUint64("timetick", m.TimeTick())
	}
	if txn := m.TxnContext(); txn != nil {
		enc.AddInt64("txnID", int64(txn.TxnID))
	}
	if broadcast := m.BroadcastHeader(); broadcast != nil {
		enc.AddInt64("broadcastID", int64(broadcast.BroadcastID))
	}
	enc.AddInt("size", len(m.payload))
	marshalSpecializedHeader(m.MessageType(), m.properties[messageHeader], enc)
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
	marshalSpecializedHeader(m.MessageType(), m.properties[messageHeader], enc)
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
func marshalSpecializedHeader(t MessageType, h string, enc zapcore.ObjectEncoder) {
	typ := messageTypeToCustomHeaderMap[t]
	// must be a proto type.
	header := reflect.New(typ.Elem()).Interface().(proto.Message)
	if err := DecodeProto(h, header); err != nil {
		enc.AddString("headerDecodeError", err.Error())
		return
	}
	switch header := header.(type) {
	case *InsertMessageHeader:
		enc.AddInt64("collectionID", header.GetCollectionId())
		segmentIDs := make([]string, 0, len(header.GetPartitions()))
		for _, partition := range header.GetPartitions() {
			segmentIDs = append(segmentIDs, strconv.FormatInt(partition.GetSegmentAssignment().GetSegmentId(), 10))
		}
		enc.AddString("segmentIDs", strings.Join(segmentIDs, "|"))
	case *DeleteMessageHeader:
		enc.AddInt64("collectionID", header.GetCollectionId())
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
		segmentIDs := make([]string, 0, len(header.GetSegmentIds()))
		for _, segmentID := range header.GetSegmentIds() {
			segmentIDs = append(segmentIDs, strconv.FormatInt(segmentID, 10))
		}
		enc.AddString("segmentIDs", strings.Join(segmentIDs, "|"))
	case *FlushMessageHeader:
		enc.AddInt64("collectionID", header.GetCollectionId())
		segmentIDs := make([]string, 0, len(header.GetSegmentIds()))
		for _, segmentID := range header.GetSegmentIds() {
			segmentIDs = append(segmentIDs, strconv.FormatInt(segmentID, 10))
		}
		enc.AddString("segmentIDs", strings.Join(segmentIDs, "|"))
	case *ManualFlushMessageHeader:
		enc.AddInt64("collectionID", header.GetCollectionId())
	case *SchemaChangeMessageHeader:
	case *ImportMessageHeader:
	}
}

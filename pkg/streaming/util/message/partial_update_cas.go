package message

import (
	"context"

	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/msgpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/messagespb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

// setProperty lets package helpers update ordinary and specialized mutable
// messages without expanding the public MutableMessage interface.
func (m *messageImpl) setProperty(key, value string) {
	m.properties.Set(key, value)
}

// AddPartialUpdateCAS stores CAS metadata in an insert builder before its body
// is encrypted and marks the resulting message for transactional production.
func (b *mutableMesasgeBuilder[H, B]) AddPartialUpdateCAS(meta *messagespb.PartialUpdateCAS) error {
	encoded, err := encodePartialUpdateCAS(meta)
	if err != nil {
		return err
	}
	body, ok := any(b.body).(*InsertRequest)
	if !ok || body == nil {
		return merr.WrapErrServiceInternalMsg("partial update CAS metadata requires an insert message builder")
	}
	setPartialUpdateCASInsertBody(body, encoded)
	b.properties.Set(messagePartialUpdateCAS, "")
	return nil
}

// EncodePartialUpdateCASIntoInsertTemplate validates meta and stores it into
// the template InsertRequest a body encoder serializes from, so the encoded
// body carries the same Base.Properties entry AddPartialUpdateCAS writes into
// a materialized body. The template must be prepared before the encoder plans
// its sizes; pair it with MarkPartialUpdateCASForBodyEncoder on each builder
// producing a message from that template.
func EncodePartialUpdateCASIntoInsertTemplate(meta *messagespb.PartialUpdateCAS, template *InsertRequest) error {
	encoded, err := encodePartialUpdateCAS(meta)
	if err != nil {
		return err
	}
	if template == nil {
		return merr.WrapErrServiceInternalMsg("partial update CAS metadata requires an insert template")
	}
	setPartialUpdateCASInsertBody(template, encoded)
	return nil
}

// MarkPartialUpdateCASForBodyEncoder marks an insert builder whose
// encoder-produced body already carries CAS metadata written by
// EncodePartialUpdateCASIntoInsertTemplate. It is the counterpart of the
// marking half of AddPartialUpdateCAS when no materialized body is attached to
// the builder.
func (b *mutableMesasgeBuilder[H, B]) MarkPartialUpdateCASForBodyEncoder() error {
	messageType := MustGetMessageTypeWithVersion[H, B]()
	if messageType.MessageType != MessageTypeInsert {
		return merr.WrapErrServiceInternalMsg("partial update CAS metadata requires an insert message builder")
	}
	if isNilBodyEncoder(b.bodyEncoder) {
		return merr.WrapErrServiceInternalMsg("partial update CAS marker requires a body encoder")
	}
	b.properties.Set(messagePartialUpdateCAS, "")
	return nil
}

// MarkPartialUpdateCASCommit marks a locally-created CommitTxn so the WAL can
// serialize its CAS admission without exposing the proof metadata in headers.
func MarkPartialUpdateCASCommit(msg MutableMessage) error {
	if msg == nil || msg.MessageType() != MessageTypeCommitTxn {
		return merr.WrapErrServiceInternalMsg("partial update CAS commit marker requires a commit transaction message")
	}
	setter, ok := msg.(interface {
		setProperty(key, value string)
	})
	if !ok {
		return merr.WrapErrServiceInternalMsg("mutable message does not support properties")
	}
	setter.setProperty(messagePartialUpdateCAS, "")
	return nil
}

func encodePartialUpdateCAS(meta *messagespb.PartialUpdateCAS) (string, error) {
	if err := validatePartialUpdateCAS(meta); err != nil {
		return "", err
	}
	encoded, err := EncodeProto(meta)
	if err != nil {
		return "", merr.WrapErrServiceInternalErr(err, "encode partial update CAS metadata")
	}
	return encoded, nil
}

func setPartialUpdateCASInsertBody(body *InsertRequest, encoded string) {
	if body.Base == nil {
		body.Base = &commonpb.MsgBase{}
	}
	if body.Base.Properties == nil {
		body.Base.Properties = make(map[string]string)
	}
	body.Base.Properties[messagePartialUpdateCAS] = encoded
}

// HasPartialUpdateCAS returns true when the message is marked for partial update CAS.
func HasPartialUpdateCAS(msg BasicMessage) bool {
	return msg.Properties().Exist(messagePartialUpdateCAS)
}

// ExtractPartialUpdateCAS decodes partial update CAS metadata from the DML body.
func ExtractPartialUpdateCAS(msg BasicMessage) (*messagespb.PartialUpdateCAS, error) {
	if !HasPartialUpdateCAS(msg) {
		return nil, nil
	}
	encoded, err := extractPartialUpdateCASBody(msg)
	if err != nil {
		return nil, err
	}

	meta := &messagespb.PartialUpdateCAS{}
	if err := DecodeProto(encoded, meta); err != nil {
		return nil, merr.WrapErrServiceInternalErr(err, "decode partial update CAS metadata")
	}
	if err := validatePartialUpdateCAS(meta); err != nil {
		return nil, err
	}
	return meta, nil
}

// DecodePartialUpdateCASMetadata decodes the serialized CAS metadata stored in
// the Insert body properties map.
func DecodePartialUpdateCASMetadata(encoded string) (*messagespb.PartialUpdateCAS, error) {
	meta := &messagespb.PartialUpdateCAS{}
	if err := DecodeProto(encoded, meta); err != nil {
		return nil, merr.WrapErrServiceInternalErr(err, "decode partial update CAS metadata")
	}
	if err := validatePartialUpdateCAS(meta); err != nil {
		return nil, err
	}
	return meta, nil
}

func extractPartialUpdateCASBody(msg BasicMessage) (string, error) {
	if msg.MessageType() != MessageTypeInsert {
		return "", merr.WrapErrServiceInternalMsg("partial update CAS marker requires an insert message")
	}
	payload, err := DecodePayload(context.Background(), msg)
	if err != nil {
		return "", merr.WrapErrServiceInternalErr(err, "decode partial update insert body")
	}
	return extractPartialUpdateCASPayload(payload)
}

func extractPartialUpdateCASPayload(payload []byte) (string, error) {
	body := &msgpb.InsertRequest{}
	if err := proto.Unmarshal(payload, body); err != nil {
		return "", merr.WrapErrServiceInternalErr(err, "decode partial update insert body")
	}
	properties := body.GetBase().GetProperties()
	encoded, ok := properties[messagePartialUpdateCAS]
	if !ok || encoded == "" {
		return "", merr.WrapErrServiceInternalMsg("partial update CAS body metadata is missing")
	}
	return encoded, nil
}

func validatePartialUpdateCAS(meta *messagespb.PartialUpdateCAS) error {
	switch {
	case meta == nil:
		return merr.WrapErrServiceInternalMsg("partial update CAS metadata is nil")
	case meta.GetReadTs() == 0:
		return merr.WrapErrServiceInternalMsg("partial update CAS read_ts is empty")
	case meta.GetObservedPchannelTerm() <= 0:
		return merr.WrapErrServiceInternalMsg("partial update CAS observed_pchannel_term is empty")
	default:
	}
	return nil
}

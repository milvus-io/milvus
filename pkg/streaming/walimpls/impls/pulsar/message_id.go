package pulsar

import (
	"encoding/base64"
	"fmt"

	"github.com/apache/pulsar-client-go/pulsar"
	"github.com/cockroachdb/errors"

	"github.com/milvus-io/milvus/pkg/streaming/util/message"
)

var _ message.MessageID = pulsarID{}

// NewPulsarID creates a new pulsarID
// TODO: remove in future.
func NewPulsarID(id pulsar.MessageID) message.MessageID {
	return pulsarID{id}
}

func UnmarshalMessageID(data string) (message.MessageID, error) {
	id, err := unmarshalMessageID(data)
	if err != nil {
		return nil, err
	}
	return id, nil
}

func unmarshalMessageID(data string) (pulsarID, error) {
	val, err := base64.StdEncoding.DecodeString(data)
	if err != nil {
		return pulsarID{nil}, errors.Wrapf(message.ErrInvalidMessageID, "decode pulsar fail when decode base64 with err: %s, id: %s", err.Error(), data)
	}
	msgID, err := pulsar.DeserializeMessageID(val)
	if err != nil {
		return pulsarID{nil}, errors.Wrapf(message.ErrInvalidMessageID, "decode pulsar fail when deserialize with err: %s, id: %s", err.Error(), data)
	}
	return pulsarID{msgID}, nil
}

type pulsarID struct {
	pulsar.MessageID
}

// PulsarID returns the pulsar message id.
// Don't delete this function until conversion logic removed.
// TODO: remove in future.
func (id pulsarID) PulsarID() pulsar.MessageID {
	return id.MessageID
}

func (id pulsarID) WALName() string {
	return walName
}

func (id pulsarID) LT(other message.MessageID) bool {
	id2 := other.(pulsarID)
	if id.LedgerID() != id2.LedgerID() {
		return id.LedgerID() < id2.LedgerID()
	}
	if id.EntryID() != id2.EntryID() {
		return id.EntryID() < id2.EntryID()
	}
	return id.BatchIdx() < id2.BatchIdx()
}

func (id pulsarID) LTE(other message.MessageID) bool {
	id2 := other.(pulsarID)
	if id.LedgerID() != id2.LedgerID() {
		return id.LedgerID() < id2.LedgerID()
	}
	if id.EntryID() != id2.EntryID() {
		return id.EntryID() < id2.EntryID()
	}
	return id.BatchIdx() <= id2.BatchIdx()
}

func (id pulsarID) EQ(other message.MessageID) bool {
	id2 := other.(pulsarID)
	return id.LedgerID() == id2.LedgerID() &&
		id.EntryID() == id2.EntryID() &&
		id.BatchIdx() == id2.BatchIdx()
}

func (id pulsarID) Marshal() string {
	return base64.StdEncoding.EncodeToString(id.Serialize())
}

func (id pulsarID) String() string {
	return fmt.Sprintf("%d/%d/%d", id.LedgerID(), id.EntryID(), id.BatchIdx())
}

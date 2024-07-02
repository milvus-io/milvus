package pulsar

import (
	"github.com/apache/pulsar-client-go/pulsar"

	"github.com/milvus-io/milvus/pkg/streaming/util/message"
)

var _ message.MessageID = pulsarID{}

func UnmarshalMessageID(data []byte) (message.MessageID, error) {
	id, err := unmarshalMessageID(data)
	if err != nil {
		return nil, err
	}
	return id, nil
}

func unmarshalMessageID(data []byte) (pulsarID, error) {
	msgID, err := pulsar.DeserializeMessageID(data)
	if err != nil {
		return pulsarID{nil}, err
	}
	return pulsarID{msgID}, nil
}

type pulsarID struct {
	pulsar.MessageID
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

func (id pulsarID) Marshal() []byte {
	return id.Serialize()
}

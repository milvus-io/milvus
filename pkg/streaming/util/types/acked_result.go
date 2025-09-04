package types

import (
	"github.com/milvus-io/milvus/pkg/v2/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v2/streaming/util/message"
)

// NewAckedPendings creates a new AckedPendings.
func NewAckedPendings(channels []string) *AckedResult {
	return &AckedResult{
		AckedResult: &streamingpb.AckedResult{
			Channels:         channels,
			AckedCheckpoints: make([]*streamingpb.AckedCheckpoint, len(channels)),
		},
	}
}

// NewAckedPendingsFromProto creates a new AckedPendings from proto.
func NewAckedPendingsFromProto(pendings *streamingpb.AckedResult) *AckedResult {
	if pendings == nil {
		return nil
	}
	return &AckedResult{
		AckedResult: pendings,
	}
}

// AckedResult is the pendings of the ack.
type AckedResult struct {
	*streamingpb.AckedResult
}

type AckedCheckpoint struct {
	Channel                string
	MessageID              message.MessageID
	LastConfirmedMessageID message.MessageID
	TimeTick               uint64
}

// Ack marks the item as acked.
// panic if the channel is not found.
// return true if the channel is acked, false if the channel is already acked.
func (a *AckedResult) Ack(cp AckedCheckpoint) bool {
	for idx, v := range a.Channels {
		if v == cp.Channel {
			if a.AckedCheckpoints[idx] == nil {
				a.AckedCheckpoints[idx] = &streamingpb.AckedCheckpoint{
					MessageId:              cp.MessageID.IntoProto(),
					LastConfirmedMessageId: cp.LastConfirmedMessageID.IntoProto(),
					TimeTick:               cp.TimeTick,
				}
				return true
			}
			return false
		}
	}
	panic("channel not found")
}

// IsAllAcked returns true if all the channels are acked.
func (a *AckedResult) IsAllAcked() bool {
	for _, v := range a.AckedCheckpoints {
		if v == nil {
			return false
		}
	}
	return true
}

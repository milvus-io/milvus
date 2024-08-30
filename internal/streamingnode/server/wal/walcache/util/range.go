package util

import (
	"fmt"

	"github.com/milvus-io/milvus/pkg/streaming/util/message"
)

// MessageIDRange represents a range of message IDs.
// The range is inclusive, i.e., both the begin and end are included.
type MessageIDRange struct {
	Begin message.MessageID // The Begin is always not nil.
	End   message.MessageID // The End may be nil if the range is not closed, [Begin, +∞).]
}

// In checks if the given message ID is in the range.
func (r MessageIDRange) In(msgID message.MessageID) bool {
	return r.Begin.LTE(msgID) && (r.End == nil || msgID.LTE(r.End))
}

// String returns the string representation of the range.
func (r MessageIDRange) String() string {
	if r.End == nil {
		return fmt.Sprintf("[%s, +∞)", r.Begin.String())
	}
	return fmt.Sprintf("(%s,%s)", r.Begin.String(), r.End.String())
}

package block

import "github.com/milvus-io/milvus/pkg/streaming/util/message"

// lowerboundOfMessageList returns the lowerbound of the message list.
func lowerboundOfMessageList(data []message.ImmutableMessage, target message.MessageID) int {
	// perform a lowerbound search here.
	left, right, mid := 0, len(data)-1, 0
	for left <= right {
		mid = (left + right) / 2
		if target.LTE(data[mid].MessageID()) {
			right = mid - 1
		} else {
			left = mid + 1
		}
	}
	return left
}

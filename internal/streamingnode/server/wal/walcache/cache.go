package walcache

import (
	"context"
	"errors"

	"github.com/milvus-io/milvus/pkg/streaming/util/message"
)

// ErrNotFound is the error when the message id is not found in cache.
var (
	// ErrEvicted is the error when the block is evicted.
	ErrEvicted = errors.New("the block is evicted")
	// the message can never be found at current cache unit.
	// returned by MessageReader.
	ErrNotFound = errors.New("message id is not found")
	// the message can not be reached by current scanner.
	// scanner scans the range [0, 99], but the target message id is 100.
	// returned by scanner.
	ErrNotReach = errors.New("message id is not reach")
)

// MessageReader is the interface to read messages from block.
type MessageReader interface {
	// Read creates a scanner to read messages from block.
	// return `ErrNotFound` if the message id is not in cache.
	// return `ErrEvicted` if the block is evicted.
	// TODO: Read operation should receive the read options, to support the message filtering inside.
	Read(started message.MessageID) (MessageScanner, error)
}

// MessageScanner is a scanner that scans messages from cache.
type MessageScanner interface {
	// Scan iterates the next message.
	// Return io.EOF if there is no more message.
	// Return ErrNotReach if the message id cannot be reached by the scanner.
	// Return nil continue to scan.
	// Return other error if any error occurs, the scanner is invalid after that.
	Scan(ctx context.Context) error

	// Message returns the current message.
	// Can only be called after Scan returns nil.
	Message() message.ImmutableMessage
}

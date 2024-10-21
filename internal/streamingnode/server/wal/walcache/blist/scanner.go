package blist

import (
	"context"
	"io"

	"github.com/cockroachdb/errors"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/walcache"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/walcache/block"
	"github.com/milvus-io/milvus/pkg/streaming/util/message"
)

// blockScanner is an interface used to scan blocks in the list.
type blockScanner interface {
	// Scan iterates the next block.
	// Return io.EOF if there is no more block.
	// Return nil continue to scan.
	Scan(ctx context.Context) error

	// Block returns the current block.
	// Can only be called after Scan returns nil.
	Block() block.Block
}

// newBlockListScanner creates a new scanner.
func newBlockListScanner(bs blockScanner, startMessageID message.MessageID) walcache.MessageScanner {
	return &blockListScanner{
		blockScanner:   bs,
		scanner:        nil,
		startMessageID: startMessageID,
		err:            nil,
	}
}

// blockListScanner is a scanner that scans messages in a immutableContinousBLockList.
type blockListScanner struct {
	blockScanner   blockScanner
	scanner        walcache.MessageScanner
	startMessageID message.MessageID
	err            error
}

// Scan scans the next message.
func (s *blockListScanner) Scan(ctx context.Context) error {
	if s.err != nil {
		return s.err
	}
	for {
		// update the next block scanner if needed.
		if err := s.updateScanner(ctx); err != nil {
			return err
		}

		err := s.scanner.Scan(ctx)
		// timeout error should be returned directly.
		if errors.IsAny(err, context.DeadlineExceeded, context.Canceled) {
			return err
		}
		// if the scanner return the ErrNotReach, the startMessageID is not consumed, we need to keep it.
		if errors.Is(err, walcache.ErrNotReach) {
			s.scanner = nil
			continue
		} else {
			// otherwise, the startMessageID is consumed, we can remove it.
			s.startMessageID = nil
		}

		if errors.Is(err, io.EOF) {
			s.scanner = nil
			continue
		}
		if err != nil {
			s.err = errors.Wrap(err, "by message scanner")
			return s.err
		}
		return nil
	}
}

// updateScanner updates the scanner.
func (s *blockListScanner) updateScanner(ctx context.Context) error {
	if s.scanner != nil {
		return nil
	}

	for {
		err := s.blockScanner.Scan(ctx)
		if errors.IsAny(err, context.DeadlineExceeded, context.Canceled, io.EOF, walcache.ErrEvicted) {
			return err
		}
		if err != nil {
			s.err = errors.Wrap(err, "by block scanner")
			return s.err
		}
		if s.startMessageID != nil {
			// if the start message id is set, we need to start at the start message id.
			if s.scanner, err = s.blockScanner.Block().Read(s.startMessageID); errors.Is(err, walcache.ErrNotFound) {
				continue
			}
		} else {
			// otherwise the full block should be read.
			s.scanner, err = s.blockScanner.Block().Read(s.blockScanner.Block().Range().Begin)
		}

		if err != nil {
			s.err = errors.Wrap(err, "by message reader")
			return s.err
		}
		return nil
	}
}

// Message returns the current message.
func (s *blockListScanner) Message() message.ImmutableMessage {
	return s.scanner.Message()
}

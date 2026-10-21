// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package reduce

import (
	"context"
	"errors"
	"io"
	"sync"

	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

// ChildStream is the portion of a ReduceStream needed to receive and close
// child streams. Close must unblock an active Recv so shutdown can join it.
type ChildStream[T any] interface {
	Recv() (T, error)
	Close() error
}

type childRecvCompletion[T any] struct {
	childIndex int
	chunk      T
	err        error
}

// ChildStreams manages one pending Recv per child stream. Its methods must be
// called serially by the owning ReduceStream, except Cancel, which may run
// concurrently to wake a pending receive wait.
type ChildStreams[T any] struct {
	streams     []ChildStream[T]
	receiving   []bool
	drained     []bool
	completions chan childRecvCompletion[T]
	done        chan struct{}
	cancelOnce  sync.Once
	pending     sync.WaitGroup

	closed   bool
	closeErr error
}

// NewChildStreams creates the shared child receive state for one ReduceStream.
func NewChildStreams[T any, S ChildStream[T]](streams []S) *ChildStreams[T] {
	children := make([]ChildStream[T], len(streams))
	for i := range streams {
		children[i] = streams[i]
	}
	return &ChildStreams[T]{
		streams:     children,
		receiving:   make([]bool, len(children)),
		drained:     make([]bool, len(children)),
		completions: make(chan childRecvCompletion[T], max(1, len(children))),
		done:        make(chan struct{}),
	}
}

// ReceiveUntilReady receives until each active child's Buffer has a Unit.
func (s *ChildStreams[T]) ReceiveUntilReady(
	hasUnit func(childIndex int) bool,
	acceptChunk func(childIndex int, chunk T) error,
) error {
	for {
		select {
		case <-s.done:
			return context.Canceled
		default:
		}

		allReady := true
		for i, stream := range s.streams {
			if s.drained[i] || hasUnit(i) {
				continue
			}

			allReady = false
			if s.receiving[i] {
				continue
			}

			s.receiving[i] = true
			s.pending.Add(1)
			go func(childIndex int, childStream ChildStream[T]) {
				defer s.pending.Done()
				chunk, err := childStream.Recv()
				s.completions <- childRecvCompletion[T]{
					childIndex: childIndex,
					chunk:      chunk,
					err:        err,
				}
			}(i, stream)
		}

		if allReady {
			return nil
		}

		var received childRecvCompletion[T]
		select {
		case <-s.done:
			return context.Canceled
		case received = <-s.completions:
		}
		s.receiving[received.childIndex] = false
		if errors.Is(received.err, io.EOF) {
			s.drained[received.childIndex] = true
			continue
		}
		if received.err != nil {
			return merr.Wrapf(received.err, "child stream %d Recv failed", received.childIndex)
		}
		if err := acceptChunk(received.childIndex, received.chunk); err != nil {
			return err
		}
	}
}

// ReceiveAnyReady receives until one active child's Buffer has a Unit.
func (s *ChildStreams[T]) ReceiveAnyReady(
	hasUnit func(childIndex int) bool,
	acceptChunk func(childIndex int, chunk T) error,
) error {
	for {
		select {
		case <-s.done:
			return context.Canceled
		default:
		}

		active := false
		for i, stream := range s.streams {
			if s.drained[i] {
				continue
			}
			active = true
			if hasUnit(i) {
				return nil
			}
			if s.receiving[i] {
				continue
			}

			s.receiving[i] = true
			s.pending.Add(1)
			go func(childIndex int, childStream ChildStream[T]) {
				defer s.pending.Done()
				chunk, err := childStream.Recv()
				s.completions <- childRecvCompletion[T]{
					childIndex: childIndex,
					chunk:      chunk,
					err:        err,
				}
			}(i, stream)
		}
		if !active {
			return nil
		}

		var received childRecvCompletion[T]
		select {
		case <-s.done:
			return context.Canceled
		case received = <-s.completions:
		}
		s.receiving[received.childIndex] = false
		if errors.Is(received.err, io.EOF) {
			s.drained[received.childIndex] = true
			continue
		}
		if received.err != nil {
			return merr.Wrapf(received.err, "child stream %d Recv failed", received.childIndex)
		}
		if err := acceptChunk(received.childIndex, received.chunk); err != nil {
			return err
		}
	}
}

// IsDrained reports whether a child stream returned EOF.
func (s *ChildStreams[T]) IsDrained(childIndex int) bool {
	return s.drained[childIndex]
}

// Cancel wakes receive waits without changing the owner's Buffers or closing
// child streams. Close performs that cleanup after the owner's Recv unwinds.
func (s *ChildStreams[T]) Cancel() {
	s.cancelOnce.Do(func() { close(s.done) })
}

// Close idempotently closes every child stream and joins pending receives.
// The owner must serialize it with receive scheduling, so no Add races Wait.
func (s *ChildStreams[T]) Close() error {
	if s.closed {
		return s.closeErr
	}
	s.closed = true
	s.Cancel()

	closeErrors := make([]error, 0, len(s.streams))
	for i, stream := range s.streams {
		if err := stream.Close(); err != nil {
			closeErrors = append(closeErrors, merr.Wrapf(err, "close child stream %d", i))
		}
	}
	s.pending.Wait()
	// A completion can retain a Chunk that was never accepted after an error.
	for len(s.completions) > 0 {
		<-s.completions
	}
	clear(s.receiving)
	s.closeErr = errors.Join(closeErrors...)
	return s.closeErr
}

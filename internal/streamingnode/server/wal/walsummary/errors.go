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

package walsummary

import "github.com/cockroachdb/errors"

var (
	// ErrStoreCorrupted marks a summary store object (chunk or manifest) that
	// cannot be decoded or is internally inconsistent. It is terminal: the
	// manifest is the only index into the chunk set, so a damaged one means
	// recovery cannot know what it is missing.
	ErrStoreCorrupted = errors.New("walsummary store corrupted")
	// ErrStoreFenced marks a write refused because the durable store already
	// carries a newer WAL assignment term: this owner is stale (split-brain)
	// and must stop persisting rather than overwrite the current owner's
	// summary state. Terminal, never retried.
	ErrStoreFenced = errors.New("walsummary store fenced by a newer term")
)

type markedStoreError struct {
	err    error
	target error
}

func (e *markedStoreError) Error() string {
	return e.err.Error()
}

func (e *markedStoreError) Unwrap() error {
	return e.err
}

func (e *markedStoreError) Is(target error) bool {
	return target == e.target
}

func markStoreCorrupted(err error) error {
	if err == nil {
		return nil
	}
	return &markedStoreError{
		err:    err,
		target: ErrStoreCorrupted,
	}
}

func storeCorruptedf(format string, args ...any) error {
	return markStoreCorrupted(errors.Errorf(format, args...))
}

func storeFencedf(format string, args ...any) error {
	return &markedStoreError{
		err:    errors.Errorf(format, args...),
		target: ErrStoreFenced,
	}
}

// isTerminalSummaryFlushError reports whether a flush error is terminal: a
// corrupted or fenced summary store cannot be repaired by rewriting the same
// chunk, so the flush task must fail loudly (and be dropped) instead of being
// retried forever.
func isTerminalSummaryFlushError(err error) bool {
	return errors.IsAny(err, ErrStoreCorrupted, ErrStoreFenced)
}

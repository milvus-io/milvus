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

package txn

import (
	"context"

	"github.com/cockroachdb/errors"

	"github.com/milvus-io/milvus/internal/util/streamingutil/status"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/util/funcutil"
)

// ValidateCommit checks the frozen successful prefix, including recovered bodies.
// Legacy transactions retain their existing validation contract.
func (s *TxnSession) ValidateCommit(ctx context.Context, msg message.MutableMessage) error {
	if !s.HasUpsert() {
		return nil
	}
	invalid := func() error {
		return status.NewUnrecoverableError("transaction containing Upsert has invalid scope or body count")
	}
	_, collectionID, _, err := funcutil.ParseVChannel(s.VChannel())
	if err != nil || collectionID <= 0 || funcutil.IsControlChannel(s.VChannel()) || s.HasInvalidScope() {
		return invalid()
	}
	validScope := func(member message.BasicMessage, vchannel string) bool {
		return message.ValidateUpsertEnvelope(member) == nil && vchannel == s.VChannel() && member.TxnContext() != nil && member.TxnContext().TxnID == s.TxnContext().TxnID
	}
	begin := s.BeginMessage()
	if begin == nil || !validScope(begin, begin.VChannel()) || !validScope(msg, msg.VChannel()) {
		return invalid()
	}

	commit, err := message.AsMutableCommitTxnMessageV2(msg)
	if err != nil || commit.Header().GetBodyCnt() == 0 {
		return invalid()
	}
	bodies := s.SuccessfulBodies()
	for _, body := range bodies {
		if !validScope(body, body.VChannel()) {
			return invalid()
		}
		if err := validateBodyCollection(ctx, body, collectionID); err != nil {
			return err
		}
	}
	if commit.Header().GetBodyCnt() != uint64(len(bodies)) {
		return status.NewTransactionExpired("txn %d body count mismatch: expected %d, appended %d", s.TxnContext().TxnID, commit.Header().GetBodyCnt(), len(bodies))
	}
	return nil
}

func validateBodyCollection(ctx context.Context, msg message.ImmutableMessage, collectionID int64) error {
	invalid := func() error { return status.NewUnrecoverableError("transaction body collection identity is invalid") }
	decodeError := func(err error) error {
		if errors.Is(err, message.ErrMalformedBody) {
			return status.NewUnrecoverableError("malformed Upsert transaction body: %v", err)
		}
		return err
	}
	switch msg.MessageType() {
	case message.MessageTypeUpsert:
		typed, err := message.AsImmutableUpsertMessageV2(msg)
		if err != nil {
			return invalid()
		}
		body, err := typed.Body(ctx)
		if err != nil {
			return decodeError(err)
		}
		if typed.Header().GetCollectionId() != collectionID || body.GetInsert() == nil || body.GetDelete() == nil || body.GetInsert().GetCollectionID() != collectionID || body.GetDelete().GetCollectionID() != collectionID {
			return invalid()
		}
	case message.MessageTypeInsert:
		typed, err := message.AsImmutableInsertMessageV1(msg)
		if err != nil {
			return invalid()
		}
		body, err := typed.Body(ctx)
		if err != nil {
			return decodeError(err)
		}
		if typed.Header().GetCollectionId() != collectionID || body == nil || body.GetCollectionID() != collectionID {
			return invalid()
		}
	case message.MessageTypeDelete:
		typed, err := message.AsImmutableDeleteMessageV1(msg)
		if err != nil {
			return invalid()
		}
		body, err := typed.Body(ctx)
		if err != nil {
			return decodeError(err)
		}
		if typed.Header().GetCollectionId() != collectionID || body == nil || body.GetCollectionID() != collectionID {
			return invalid()
		}
	default:
		return status.NewUnrecoverableError("transaction containing Upsert has an unsupported body type")
	}
	return nil
}

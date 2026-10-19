package message

import (
	"maps"

	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

// WithTxnBodyCount decorates a Commit copy without mutating the source WAL
// message retained by CDC for subsequent reconnects.
func WithTxnBodyCount(commit ImmutableMessage, count uint64) ImmutableMessage {
	header := proto.Clone(MustAsImmutableCommitTxnMessageV2(commit).Header()).(*CommitTxnMessageHeader)
	header.BodyCnt = count
	encoded, err := EncodeProto(header)
	if err != nil {
		panic(err)
	}
	properties := maps.Clone(commit.Properties().ToRawMap())
	properties[messageHeader] = encoded
	return NewImmutableMesasge(commit.MessageID(), commit.Payload(), properties)
}

// CheckTxnBodyCount validates logical bodies after physical WAL deduplication.
// Zero is the legacy unchecked format, including empty transactions.
func CheckTxnBodyCount(expected uint64, actual int) error {
	if expected != 0 && expected != uint64(actual) {
		return merr.WrapErrDataIntegrityMsg("transaction body count mismatch: expected %d, received %d", expected, actual)
	}
	return nil
}

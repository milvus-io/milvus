package utils

import (
	"github.com/milvus-io/milvus/pkg/v3/proto/messagespb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
)

type txnInsertResultBuilder struct {
	insertResults   []*messagespb.IdempotentInsertResult
	expiredTimeTick uint64
}

func (b *txnInsertResultBuilder) add(result *messagespb.IdempotentInsertResult, expiredTimeTick uint64) {
	b.insertResults = append(b.insertResults, result)
	b.keepalive(expiredTimeTick)
}

func (b *txnInsertResultBuilder) keepalive(expiredTimeTick uint64) {
	if expiredTimeTick > 0 {
		b.expiredTimeTick = expiredTimeTick
	}
}

func (b *txnInsertResultBuilder) build() *messagespb.IdempotentInsertResult {
	// On a malformed/mixed-type merge (err) or no payload (!hadAny) there is no
	// usable idempotent result; the homogeneous-type check inside the merge keeps
	// a corrupt set from producing a wrong (silently mismatched) result.
	merged, hadAny, err := message.MergeIdempotentInsertResults(b.insertResults...)
	if err != nil || !hadAny {
		return nil
	}
	return merged
}

func (b *txnInsertResultBuilder) expiredTimeTickValue() uint64 {
	return b.expiredTimeTick
}

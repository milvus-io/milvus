package proxy

import (
	"context"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/util/hookutil"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/proto/messagespb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/types"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

// prepareIdempotencyKey enables request-level deduplication only for an explicit key.
func (it *insertTask) prepareIdempotencyKey(collectionProperties []*commonpb.KeyValuePair) error {
	it.idempotencyEnabled = it.idempotencyKey != ""
	if !it.idempotencyEnabled {
		return nil
	}
	// The key and duplicate result are plaintext message properties; collection
	// encryption currently protects only the body.
	if hookutil.IsClusterEncryptionEnabled() &&
		hookutil.GetEzByCollProperties(collectionProperties, it.collectionID).AsMessageConfig() != nil {
		return merr.WrapErrParameterInvalidMsg(
			"idempotent write is not supported for an encrypted collection: the duplicate result and the key would be stored unencrypted")
	}
	if limit := Params.StreamingCfg.IdempotencyMaxKeyLength.GetAsInt(); limit > 0 && len(it.idempotencyKey) > limit {
		return merr.WrapErrParameterInvalidMsg("idempotency key length %d exceeds limit %d", len(it.idempotencyKey), limit)
	}
	return nil
}

func (it *insertTask) reassignAutoIDForIdempotencyIfNeeded(ctx context.Context, excludeAutoIDPrimary bool, primaryFieldSchema *schemapb.FieldSchema) error {
	if !it.idempotencyEnabled || !excludeAutoIDPrimary {
		return nil
	}
	// Namespace partition-key mode routes the WAL shard by namespace, not by the
	// generated primary key, so PK-hash-stable auto IDs buy nothing here.
	if namespacePartitionKeyModeEnabled(it.schema) && it.insertMsg.Namespace != nil {
		return nil
	}

	log := mlog.With(mlog.String("collectionName", it.insertMsg.GetCollectionName()))
	channelNames, err := it.chMgr.GetVChannels(it.collectionID)
	if err != nil {
		log.Warn(ctx, "get vChannels for idempotent autoID assignment failed",
			mlog.Int64("collectionID", it.collectionID),
			mlog.Err(err))
		return err
	}
	it.vChannels = channelNames
	if err := it.reassignAutoIDForStableIdempotency(primaryFieldSchema, channelNames); err != nil {
		log.Warn(ctx, "stabilize idempotent autoID assignment failed", mlog.Err(err))
		return err
	}
	return nil
}

// insertIdempotencyDecoration single-sources idempotency marking for inserts in
// the proxy. It pairs the two things that together make a write unit idempotent:
//   - the idempotency key, which travels in the `_ik` message property rather
//     than in the insert header (so every message type is read the same way);
//   - the per-write-unit insert result, which is stamped onto the insert header.
//
// They are carried in one value so the idempotency interceptor's "insert result
// requires key" invariant cannot be broken by applying one without the other. A
// nil decoration means idempotency is disabled for this insert and every method
// below is a no-op.
type insertIdempotencyDecoration struct {
	key            string
	decorateHeader func(*message.InsertMessageHeader, []int) error
}

// idempotentInsertDecoration returns the idempotency decoration for this insert,
// or nil when idempotency is disabled for it. The transaction commit message,
// synthesized later in the producer, gets the key applied there.
func (it *insertTask) idempotentInsertDecoration() *insertIdempotencyDecoration {
	if !it.idempotencyEnabled {
		return nil
	}
	ids := it.result.GetIDs()
	return &insertIdempotencyDecoration{
		key: it.idempotencyKey,
		decorateHeader: func(header *message.InsertMessageHeader, rowOffsets []int) error {
			result, err := buildInsertWriteUnitIdempotentInsertResult(ids, rowOffsets)
			if err != nil {
				return err
			}
			message.SetInsertHeaderIdempotentInsertResult(header, result)
			return nil
		},
	}
}

// enabled reports whether this insert is an idempotent write.
func (d *insertIdempotencyDecoration) enabled() bool {
	return d != nil
}

// idempotencyKey returns the key to stamp on the message property, or "" when
// the insert is not idempotent (WithIdempotencyKey then no-ops).
//
// The key is unscoped: the dedup window is per vchannel, and a vchannel name
// already carries the collection id.
func (d *insertIdempotencyDecoration) idempotencyKey() message.IdempotencyKey {
	if d == nil {
		return ""
	}
	return message.IdempotencyKey(d.key)
}

// decorate stamps the per-write-unit insert result onto the insert header.
func (d *insertIdempotencyDecoration) decorate(header *message.InsertMessageHeader, rowOffsets []int) error {
	if d == nil {
		return nil
	}
	return d.decorateHeader(header, rowOffsets)
}

func buildInsertWriteUnitIdempotentInsertResult(ids *schemapb.IDs, rowOffsets []int) (*messagespb.IdempotentInsertResult, error) {
	writeUnitIDs, err := idsByOffsets(ids, rowOffsets)
	if err != nil {
		return nil, err
	}
	offsets := make([]uint32, 0, len(rowOffsets))
	for _, offset := range rowOffsets {
		if offset < 0 {
			return nil, merr.WrapErrServiceInternalMsg("negative row offset %d", offset)
		}
		offsets = append(offsets, uint32(offset))
	}
	return message.NewIdempotentInsertResult(offsets, writeUnitIDs), nil
}

func idsByOffsets(ids *schemapb.IDs, rowOffsets []int) (*schemapb.IDs, error) {
	if ids == nil {
		return nil, merr.WrapErrServiceInternalMsg("missing mutation result ids")
	}
	if intIDs := ids.GetIntId(); intIDs != nil {
		data := intIDs.GetData()
		selected := make([]int64, 0, len(rowOffsets))
		for _, offset := range rowOffsets {
			if offset < 0 || offset >= len(data) {
				return nil, merr.WrapErrServiceInternalMsg("row offset %d out of int id range %d", offset, len(data))
			}
			selected = append(selected, data[offset])
		}
		return &schemapb.IDs{
			IdField: &schemapb.IDs_IntId{IntId: &schemapb.LongArray{Data: selected}},
		}, nil
	}
	if strIDs := ids.GetStrId(); strIDs != nil {
		data := strIDs.GetData()
		selected := make([]string, 0, len(rowOffsets))
		for _, offset := range rowOffsets {
			if offset < 0 || offset >= len(data) {
				return nil, merr.WrapErrServiceInternalMsg("row offset %d out of string id range %d", offset, len(data))
			}
			selected = append(selected, data[offset])
		}
		return &schemapb.IDs{
			IdField: &schemapb.IDs_StrId{StrId: &schemapb.StringArray{Data: selected}},
		}, nil
	}
	return nil, merr.WrapErrServiceInternalMsg("unsupported mutation result ids type")
}

// warnOnPartialIdempotentDuplicate makes a fan-out that was deduplicated on some
// write units but appended fresh on others visible in the log instead of merging
// silently into a successful response.
//
// This is a diagnostic, not a rejection: the mix is the EXPECTED outcome of a
// retry after an attempt that only reached part of the fan-out (the missing
// shards must be written, the landed ones must not be written twice), and the
// proxy cannot tell that apart from the pathological case where a shard's window
// forgot a key its siblings still hold — the retry then re-appends rows that are
// already in the WAL. Failing here would break the legitimate case, so the mix is
// only reported. The idempotency window prevents minEntries from extending a
// retained entry past TTL, but the hard maxBytes cap may still shorten
// retention independently per vchannel under skewed load; this warning is the
// visibility hook for either accepted partial-retry progress or cap pressure.
func warnOnPartialIdempotentDuplicate(ctx context.Context, key string, resp types.AppendResponses) {
	duplicates := 0
	total := 0
	for _, appendResp := range resp.Responses {
		if appendResp.Error != nil || appendResp.AppendResult == nil {
			continue
		}
		total++
		if appendResp.AppendResult.Extra != nil {
			duplicates++
		}
	}
	if duplicates == 0 || duplicates == total {
		return
	}
	mlog.RatedWarn(ctx, 1, "idempotent insert was deduplicated on part of its write units only; the fresh ones were appended again",
		mlog.String("idempotencyKeyHash", message.IdempotencyKeyFingerprint(key)),
		mlog.Int("idempotencyKeyLength", len(key)),
		mlog.Int("duplicateWriteUnits", duplicates),
		mlog.Int("totalWriteUnits", total))
}

func mergeDuplicateInsertResults(result *milvuspb.MutationResult, resp types.AppendResponses) error {
	if result == nil || result.GetIDs() == nil {
		return nil
	}
	for _, appendResp := range resp.Responses {
		if appendResp.Error != nil || appendResp.AppendResult == nil || appendResp.AppendResult.Extra == nil {
			continue
		}
		extra := &messagespb.IdempotentInsertResult{}
		if !appendResp.AppendResult.Extra.MessageIs(extra) {
			continue
		}
		if err := appendResp.AppendResult.GetExtra(extra); err != nil {
			return err
		}
		if err := mergeInsertIDsByOffsets(result.GetIDs(), extra.GetIds(), extra.GetRowOffsets()); err != nil {
			return err
		}
	}
	return nil
}

func mergeInsertIDsByOffsets(dst *schemapb.IDs, src *schemapb.IDs, rowOffsets []uint32) error {
	if dst == nil || src == nil {
		return merr.WrapErrServiceInternalMsg("missing ids for idempotent insert result merge")
	}
	if srcIntIDs := src.GetIntId(); srcIntIDs != nil {
		dstIntIDs := dst.GetIntId()
		if dstIntIDs == nil {
			return merr.WrapErrServiceInternalMsg("id type mismatch for idempotent insert result merge")
		}
		if len(rowOffsets) != len(srcIntIDs.GetData()) {
			return merr.WrapErrServiceInternalMsg("row offsets length %d mismatches int ids length %d", len(rowOffsets), len(srcIntIDs.GetData()))
		}
		for i, offset := range rowOffsets {
			if int(offset) >= len(dstIntIDs.Data) {
				return merr.WrapErrServiceInternalMsg("row offset %d out of mutation result int id range %d", offset, len(dstIntIDs.Data))
			}
			dstIntIDs.Data[offset] = srcIntIDs.GetData()[i]
		}
		return nil
	}
	if srcStrIDs := src.GetStrId(); srcStrIDs != nil {
		dstStrIDs := dst.GetStrId()
		if dstStrIDs == nil {
			return merr.WrapErrServiceInternalMsg("id type mismatch for idempotent insert result merge")
		}
		if len(rowOffsets) != len(srcStrIDs.GetData()) {
			return merr.WrapErrServiceInternalMsg("row offsets length %d mismatches string ids length %d", len(rowOffsets), len(srcStrIDs.GetData()))
		}
		for i, offset := range rowOffsets {
			if int(offset) >= len(dstStrIDs.Data) {
				return merr.WrapErrServiceInternalMsg("row offset %d out of mutation result string id range %d", offset, len(dstStrIDs.Data))
			}
			dstStrIDs.Data[offset] = srcStrIDs.GetData()[i]
		}
		return nil
	}
	return merr.WrapErrServiceInternalMsg("unsupported idempotent insert result ids type")
}

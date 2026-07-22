package proxy

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"hash"
	"sort"
	"strconv"

	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/mq/msgstream"
	"github.com/milvus-io/milvus/pkg/v3/proto/messagespb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/types"
	"github.com/milvus-io/milvus/pkg/v3/util/funcutil"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

func collectionInsertIdempotencyEnabled(properties []*commonpb.KeyValuePair) bool {
	value, ok := funcutil.TryGetAttrByKeyFromRepeatedKV(common.CollectionInsertIdempotencyEnabledKey, properties)
	if !ok {
		return false
	}
	enabled, err := strconv.ParseBool(value)
	if err != nil {
		// DDL validation rejects unparseable values, but a property written
		// before that validation existed may still carry one; make the silent
		// downgrade observable instead of quietly disabling the durability
		// guarantee the operator believes is on.
		mlog.Warn(context.TODO(), "malformed collection insert idempotency property; treating idempotency as disabled",
			mlog.String("key", common.CollectionInsertIdempotencyEnabledKey),
			mlog.String("value", value))
		return false
	}
	return enabled
}

// validateInsertIdempotencyProperty rejects an unparseable
// collection.insert.idempotency.enabled value at DDL time, so a typo cannot
// silently disable the durability feature the operator believes is on.
func validateInsertIdempotencyProperty(props []*commonpb.KeyValuePair) error {
	value, ok := funcutil.TryGetAttrByKeyFromRepeatedKV(common.CollectionInsertIdempotencyEnabledKey, props)
	if !ok {
		return nil
	}
	if _, err := strconv.ParseBool(value); err != nil {
		return merr.WrapErrParameterInvalidMsg("%s should be a boolean, but got %q", common.CollectionInsertIdempotencyEnabledKey, value)
	}
	return nil
}

func (it *insertTask) prepareAutoIdempotencyKeyIfEnabled(ctx context.Context, collectionProperties []*commonpb.KeyValuePair, excludeAutoIDPrimary bool) error {
	globalIdempotencyEnabled := Params.StreamingCfg.IdempotencyEnabled.GetAsBool()
	collectionIdempotencyEnabled := collectionInsertIdempotencyEnabled(collectionProperties)
	it.idempotencyEnabled = globalIdempotencyEnabled && collectionIdempotencyEnabled
	if it.idempotencyKey != "" && !globalIdempotencyEnabled {
		return merr.WrapErrParameterInvalidMsg("idempotency key is not accepted when idempotent write is globally disabled")
	}
	if it.idempotencyKey != "" && !collectionIdempotencyEnabled {
		return merr.WrapErrParameterInvalidMsg("idempotency key is not accepted when collection idempotent write is disabled")
	}
	if !it.idempotencyEnabled {
		it.idempotencyKey = ""
		return nil
	}

	log := mlog.With(mlog.String("collectionName", it.insertMsg.GetCollectionName()))
	if it.idempotencyKey == "" {
		autoIdempotencyKey, err := canonicalInsertPayloadKey(insertIdempotencyScopeOf(it.insertMsg), it.insertMsg.GetNumRows(), it.insertMsg.GetFieldsData(), it.schema, excludeAutoIDPrimary)
		if err != nil {
			log.Warn(ctx, "compute insert idempotency key failed", mlog.Err(err))
			return err
		}
		it.idempotencyKey = autoIdempotencyKey
	}
	// Validate the effective key length (client-supplied or auto-generated) after it
	// is resolved, so an over-limit auto key is rejected here at the proxy instead of
	// slipping through and getting a confusing rejection from the streaming node.
	if limit := Params.StreamingCfg.IdempotencyMaxKeyLength.GetAsInt(); limit > 0 && len(it.idempotencyKey) > limit {
		return merr.WrapErrParameterInvalidMsg("idempotency key length %d exceeds limit %d", len(it.idempotencyKey), limit)
	}
	return nil
}

func (it *insertTask) reassignAutoIDForIdempotencyIfNeeded(ctx context.Context, excludeAutoIDPrimary bool, primaryFieldSchema *schemapb.FieldSchema) error {
	if !it.idempotencyEnabled || !excludeAutoIDPrimary {
		return nil
	}

	log := mlog.With(mlog.String("collectionName", it.insertMsg.GetCollectionName()))
	channelNames, err := it.chMgr.getVChannels(it.collectionID)
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

// idempotentInsertHeaderDecorator single-sources idempotency header construction
// for inserts in the proxy: it stamps both the idempotency key and the
// per-write-unit insert result onto each insert header, so the idempotency
// interceptor's "insert result requires key" invariant is satisfied at one site.
// It returns nil (a no-op) when idempotency is disabled for this insert; the
// transaction commit message, synthesized later in the producer, still gets the
// key applied there.
func (it *insertTask) idempotentInsertHeaderDecorator() func(*message.InsertMessageHeader, []int) error {
	if !it.idempotencyEnabled {
		return nil
	}
	ids := it.result.GetIDs()
	key := it.idempotencyKey
	return func(header *message.InsertMessageHeader, rowOffsets []int) error {
		result, err := buildInsertWriteUnitIdempotentInsertResult(ids, rowOffsets)
		if err != nil {
			return err
		}
		message.SetInsertHeaderIdempotentInsertResult(header, result)
		if key != "" {
			header.IdempotencyKey = proto.String(key)
		}
		return nil
	}
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
// only reported; the uniform-TTL rule in the idempotency window is what keeps
// retention from diverging between shards in the first place.
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
	mlog.Warn(ctx, "idempotent insert was deduplicated on part of its write units only; the fresh ones were appended again",
		mlog.String("idempotencyKey", key),
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

// insertIdempotencyScope is the request-level destination of an insert: the
// fields that select where the rows go but never appear in the column payload.
//
// They must be part of the auto-derived key. The server-side dedup window is
// keyed by vchannel (see the idempotency interceptor), and a vchannel is a
// collection shard, not a partition or a namespace: two logically distinct
// inserts of the same rows into two partitions of one collection would hash to
// one key, and the second one would be answered from the window as a duplicate
// with the first insert's primary keys while its rows never reach the WAL. An
// auto key is not under the caller's control, so the "do not reuse a key"
// contract that covers an explicit client key does not excuse that collision.
type insertIdempotencyScope struct {
	dbName         string
	collectionName string
	partitionName  string
	// namespace is optional: an unset namespace routes by primary key while an
	// empty-string namespace routes by namespace, so nil and "" are distinct.
	namespace *string
}

func insertIdempotencyScopeOf(insertMsg *msgstream.InsertMsg) insertIdempotencyScope {
	return insertIdempotencyScope{
		dbName:         insertMsg.GetDbName(),
		collectionName: insertMsg.GetCollectionName(),
		partitionName:  insertMsg.GetPartitionName(),
		namespace:      insertMsg.Namespace,
	}
}

// writeTo mixes the destination into h, length-prefixing every string so
// neighboring fields cannot be shifted into each other.
func (scope insertIdempotencyScope) writeTo(h hash.Hash) {
	var buf [8]byte
	writeString := func(value string) {
		binary.LittleEndian.PutUint64(buf[:], uint64(len(value)))
		h.Write(buf[:])
		h.Write([]byte(value))
	}
	writeString(scope.dbName)
	writeString(scope.collectionName)
	writeString(scope.partitionName)
	if scope.namespace == nil {
		h.Write([]byte{0})
		return
	}
	h.Write([]byte{1})
	writeString(*scope.namespace)
}

// canonicalInsertPayloadKey derives a deterministic idempotency key from the
// insert destination plus the insert payload. On the payload side it
// intentionally covers only the client-supplied columns plus numRows: it runs
// before the proxy fills field properties / function output / dynamic /
// namespace fields, so by design it must not depend on any of them.
//
// NOTE: the destination is hashed exactly as the client sent it, before the
// proxy resolves an empty partition name to the default partition. A retry
// resends the same request, so the key stays stable; spelling the same
// destination two ways only costs a dedup hit, it can never merge two
// destinations.
//
// NOTE: at this point the client FieldData carry no field id yet (FieldId == 0),
// so the sort below effectively orders by field NAME (the FieldID comparison is a
// no-op), and the auto-id primary exclusion below matches by name. The hash is
// still deterministic for byte-identical client payloads, which is all an
// idempotent retry needs; do not assume field-id ordering here.
func canonicalInsertPayloadKey(scope insertIdempotencyScope, numRows uint64, fieldsData []*schemapb.FieldData, schema *schemapb.CollectionSchema, excludeAutoIDPrimary bool) (string, error) {
	if excludeAutoIDPrimary {
		primaryField, err := typeutil.GetPrimaryFieldSchema(schema)
		if err != nil {
			return "", err
		}
		if primaryField.GetAutoID() {
			filtered := make([]*schemapb.FieldData, 0, len(fieldsData))
			for _, fieldData := range fieldsData {
				if fieldData.GetFieldId() == primaryField.GetFieldID() || fieldData.GetFieldName() == primaryField.GetName() {
					continue
				}
				filtered = append(filtered, fieldData)
			}
			fieldsData = filtered
		}
	}
	if err := validateCanonicalInsertPayloadFields(fieldsData); err != nil {
		return "", err
	}

	sortedFields := append([]*schemapb.FieldData(nil), fieldsData...)
	sort.Slice(sortedFields, func(i, j int) bool {
		left, right := sortedFields[i], sortedFields[j]
		if left.GetFieldId() != right.GetFieldId() {
			return left.GetFieldId() < right.GetFieldId()
		}
		return left.GetFieldName() < right.GetFieldName()
	})

	hash := sha256.New()
	scope.writeTo(hash)
	var buf [8]byte
	binary.LittleEndian.PutUint64(buf[:], numRows)
	hash.Write(buf[:])
	for _, fieldData := range sortedFields {
		payload, err := proto.MarshalOptions{Deterministic: true}.Marshal(fieldData)
		if err != nil {
			return "", err
		}
		binary.LittleEndian.PutUint64(buf[:], uint64(len(payload)))
		hash.Write(buf[:])
		hash.Write(payload)
	}
	return hex.EncodeToString(hash.Sum(nil)), nil
}

func validateCanonicalInsertPayloadFields(fieldsData []*schemapb.FieldData) error {
	seenFieldIDs := make(map[int64]struct{}, len(fieldsData))
	seenFieldNames := make(map[string]struct{}, len(fieldsData))
	for _, fieldData := range fieldsData {
		if fieldData == nil {
			return merr.WrapErrParameterInvalidMsg("nil field data in insert payload")
		}
		if fieldID := fieldData.GetFieldId(); fieldID != 0 {
			if _, ok := seenFieldIDs[fieldID]; ok {
				return merr.WrapErrParameterInvalidMsg("duplicate field id %d in insert payload", fieldID)
			}
			seenFieldIDs[fieldID] = struct{}{}
		}
		if fieldName := fieldData.GetFieldName(); fieldName != "" {
			if _, ok := seenFieldNames[fieldName]; ok {
				return merr.WrapErrParameterInvalidMsg("duplicate field name %q in insert payload", fieldName)
			}
			seenFieldNames[fieldName] = struct{}{}
		}
	}
	return nil
}

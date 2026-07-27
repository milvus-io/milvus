package message

import (
	"crypto/sha256"
	"encoding/hex"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/messagespb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

const idempotencyKeyFingerprintBytes = 16

// IdempotencyKeyFingerprint returns a stable, non-plaintext identifier suitable
// for correlating idempotency-key events in logs. The original key must never be
// logged: explicit keys are client-controlled and may contain sensitive data.
func IdempotencyKeyFingerprint(key string) string {
	sum := sha256.Sum256([]byte(key))
	return hex.EncodeToString(sum[:idempotencyKeyFingerprintBytes])
}

func NewIdempotentInsertResult(rowOffsets []uint32, ids *schemapb.IDs) *messagespb.IdempotentInsertResult {
	return &messagespb.IdempotentInsertResult{
		RowOffsets: rowOffsets,
		Ids:        ids,
	}
}

func IdempotentInsertResultFromInsertHeader(header *InsertMessageHeader) (*messagespb.IdempotentInsertResult, bool) {
	if header == nil {
		return nil, false
	}
	if result := header.GetIdempotentResult(); result != nil {
		return result, true
	}
	return nil, false
}

func SetInsertHeaderIdempotentInsertResult(header *InsertMessageHeader, result *messagespb.IdempotentInsertResult) {
	if header == nil {
		return
	}
	header.IdempotentResult = nil
	if result == nil {
		return
	}
	header.IdempotentResult = result
}

// ValidateIdempotentInsertResult validates an idempotent insert result. The
// helper is shared by trust-boundary and internal recovery/result-building
// callers, so malformed shapes originate as system errors here; a caller that
// knows the value came directly from an untrusted boundary may translate the
// error there. It rejects malformed shapes rather than tolerating them:
//   - row offsets present but no ids, or ids present but no row offsets;
//   - ids set but neither the int nor the string field is populated, or both;
//   - row offsets length not matching the populated id field length.
//
// A fully empty result (no row offsets and no ids) is valid.
func ValidateIdempotentInsertResult(result *messagespb.IdempotentInsertResult) error {
	if result == nil {
		return nil
	}
	rowCount := len(result.GetRowOffsets())
	ids := result.GetIds()
	if ids == nil {
		if rowCount != 0 {
			return merr.WrapErrServiceInternalMsg("idempotent insert result has %d row offsets but no ids", rowCount)
		}
		return nil
	}
	intIDs := ids.GetIntId()
	strIDs := ids.GetStrId()
	switch {
	case intIDs != nil && strIDs != nil:
		return merr.WrapErrServiceInternalMsg("idempotent insert result ids set both int and string fields")
	case intIDs != nil:
		if rowCount != len(intIDs.GetData()) {
			return merr.WrapErrServiceInternalMsg("row offsets length %d mismatches int ids length %d", rowCount, len(intIDs.GetData()))
		}
	case strIDs != nil:
		if rowCount != len(strIDs.GetData()) {
			return merr.WrapErrServiceInternalMsg("row offsets length %d mismatches string ids length %d", rowCount, len(strIDs.GetData()))
		}
	default:
		return merr.WrapErrServiceInternalMsg("idempotent insert result ids set neither int nor string field")
	}
	return nil
}

// MergeIdempotentInsertResults concatenates the row offsets and ids of the given
// per-write-unit insert results, in order.
//
// hadAny reports whether at least one non-empty result contributed to merged; it
// is false (with merged nil and err nil) when there is nothing to merge (no
// results, only nil results, or only empty results). err is non-nil only when an
// input is malformed: it fails ValidateIdempotentInsertResult, or the results mix
// int and string id types. Callers must distinguish err (corruption) from
// !hadAny (no payload) rather than collapsing both into "no payload".
func MergeIdempotentInsertResults(results ...*messagespb.IdempotentInsertResult) (merged *messagespb.IdempotentInsertResult, hadAny bool, err error) {
	out := &messagespb.IdempotentInsertResult{}
	for _, result := range results {
		if result == nil {
			continue
		}
		if err := ValidateIdempotentInsertResult(result); err != nil {
			return nil, false, err
		}
		ids := result.GetIds()
		if ids == nil {
			// Validated empty result (no row offsets, no ids): nothing to merge.
			continue
		}
		out.RowOffsets = append(out.RowOffsets, result.GetRowOffsets()...)
		if !appendIDs(out, ids) {
			return nil, false, merr.WrapErrServiceInternalMsg("idempotent insert results mix int and string id types")
		}
		hadAny = true
	}
	if !hadAny {
		return nil, false, nil
	}
	return out, true, nil
}

func appendIDs(result *messagespb.IdempotentInsertResult, ids *schemapb.IDs) bool {
	if intIDs := ids.GetIntId(); intIDs != nil {
		if result.Ids == nil {
			result.Ids = &schemapb.IDs{
				IdField: &schemapb.IDs_IntId{IntId: &schemapb.LongArray{}},
			}
		}
		dst := result.Ids.GetIntId()
		if dst == nil {
			return false
		}
		dst.Data = append(dst.Data, intIDs.GetData()...)
		return true
	}
	if strIDs := ids.GetStrId(); strIDs != nil {
		if result.Ids == nil {
			result.Ids = &schemapb.IDs{
				IdField: &schemapb.IDs_StrId{StrId: &schemapb.StringArray{}},
			}
		}
		dst := result.Ids.GetStrId()
		if dst == nil {
			return false
		}
		dst.Data = append(dst.Data, strIDs.GetData()...)
		return true
	}
	return false
}

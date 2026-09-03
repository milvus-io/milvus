package message

import (
	"crypto/sha256"
	"encoding/hex"
	"strconv"
	"strings"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"

	"github.com/milvus-io/milvus/pkg/v3/proto/messagespb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

const idempotencyKeyFingerprintBytes = 16

// IdempotencyScopeDomain names the kind of object a client key deduplicates within.
//
// The domain is carried explicitly rather than inferred from the scope id, even
// though collection ids and database ids come from one rootcoord allocator today
// and therefore never collide. Inferring it would make the broadcaster depend on
// an allocator property it does not own, and would lock every future scope into
// having an int64 identity at all. The failure mode of getting that wrong is two
// unrelated operations sharing one dedup entry, i.e. one of them silently
// swallowed, and the encoding is persisted in the WAL and in etcd, so it cannot
// be revised cheaply after release.
type IdempotencyScopeDomain int

const (
	IdempotencyScopeCluster    IdempotencyScopeDomain = 1
	IdempotencyScopeDatabase   IdempotencyScopeDomain = 2
	IdempotencyScopeCollection IdempotencyScopeDomain = 3
)

// IdempotencyKey is a client-supplied idempotency key together with the scope it
// deduplicates within, encoded as `<domain>:<scopeID>:<clientKey>`.
//
// The scope is an IDENTITY, never a name: a collection id rather than a
// collection name. That is what keeps a key bound to the object it was issued
// against when that object is renamed -- a retry that names the renamed object
// still resolves to the original -- and what makes a drop-and-recreate under the
// same name a different operation. It does not make a stale name work: whatever
// resolves the caller's name to an id runs before any of this.
//
// Callers choose the scope explicitly through one of the New*ScopedIdempotencyKey
// constructors; there is no constructor that takes a bare string, so a caller
// cannot end up with an unscoped key by omission. Choosing cluster scope is a
// decision that reads as one.
//
// The encoding is injective without any framing tricks: the domain and the scope
// id are decimal digits, so the first two colons delimit them and the client key
// is the unbounded tail. A crafted client key cannot impersonate another scope no
// matter what it contains.
//
// The empty value means "not an idempotent write". Every constructor returns it
// for an empty client key, so an absent key can never encode to a non-empty scope
// -- which would otherwise make every keyless broadcast of one message type
// deduplicate against every other.
type IdempotencyKey string

// NewClusterScopedIdempotencyKey scopes the key to the whole cluster: the client
// key must be unique across every object, and the operation is deduplicated
// wherever it happens.
func NewClusterScopedIdempotencyKey(clientKey string) IdempotencyKey {
	return newIdempotencyKey(IdempotencyScopeCluster, 0, clientKey)
}

// NewDatabaseScopedIdempotencyKey scopes the key to one database, so the same
// client key stays a distinct operation against another database.
func NewDatabaseScopedIdempotencyKey(dbID int64, clientKey string) IdempotencyKey {
	return newIdempotencyKey(IdempotencyScopeDatabase, dbID, clientKey)
}

// NewCollectionScopedIdempotencyKey scopes the key to one collection, so the same
// client key stays a distinct operation against another collection.
func NewCollectionScopedIdempotencyKey(collectionID int64, clientKey string) IdempotencyKey {
	return newIdempotencyKey(IdempotencyScopeCollection, collectionID, clientKey)
}

// newIdempotencyKey encodes the scope onto the client key. The scope id is unused
// for cluster scope and encoded as 0 there.
func newIdempotencyKey(domain IdempotencyScopeDomain, scopeID int64, clientKey string) IdempotencyKey {
	if clientKey == "" {
		return ""
	}
	var b strings.Builder
	b.WriteString(strconv.Itoa(int(domain)))
	b.WriteByte(':')
	b.WriteString(strconv.FormatInt(scopeID, 10))
	b.WriteByte(':')
	b.WriteString(clientKey)
	return IdempotencyKey(b.String())
}

// ClientKey returns the client-supplied portion, i.e. the key without the scope
// this package encoded onto it. It is the only way back: the encoding is one-way
// for everything else, and the scope is chosen by the caller, so a holder of an
// encoded key cannot otherwise tell which bytes came from the client.
//
// Nothing in the request path needs it. Both places that bound or fingerprint a
// client key -- the REST middleware and the propagation interceptor -- see the raw
// string before this package encodes anything onto it, and the broadcaster indexes
// the encoded form whole. This exists for a holder of an encoded key that must show
// the client its own key back, or attribute one in a log.
//
// A value this package did not produce has no recoverable client portion, so it is
// returned whole rather than guessed at.
func (k IdempotencyKey) ClientKey() string {
	parts := strings.SplitN(string(k), ":", 3)
	if len(parts) < 3 {
		return string(k)
	}
	return parts[2]
}

// IdempotencyKeyOf returns the idempotency key carried by the message, or "" when
// the message is not an idempotent write.
//
// The key lives in the `_ik` property rather than in a header field, so this one
// accessor serves every message type and every message stage (broadcast, mutable,
// immutable). Callers that only honor the key on specific message types must gate
// on the type themselves: any message may technically carry the property.
func IdempotencyKeyOf(msg BasicMessage) IdempotencyKey {
	if msg == nil {
		return ""
	}
	key, _ := msg.Properties().Get(messageIdempotencyKey)
	return IdempotencyKey(key)
}

// IdempotencyKeyFingerprint returns a stable identifier suitable for correlating
// idempotency-key events in logs. The original key must never be logged: it is
// client-controlled and may contain sensitive data.
//
// This obfuscates the key, it does not protect it: an unkeyed digest of a
// low-entropy client key (a run id, a short batch name) is recoverable by anyone
// who can guess candidates. Use it to correlate log lines, never as a security
// control or an authorization token.
func IdempotencyKeyFingerprint(clientKey string) string {
	sum := sha256.Sum256([]byte(clientKey))
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

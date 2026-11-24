package predicates

import "github.com/cockroachdb/errors"

// ErrNotMet marks the error of a conditional write whose predicates do not
// hold. Match it with errors.Is.
var ErrNotMet = errors.New("predicate not met")

// PredicateTarget is what a predicate checks. Each target carries its own
// condition.
type PredicateTarget int32

const (
	// PredTargetValueEqual holds when the key exists and its value equals
	// TargetValue.
	PredTargetValueEqual PredicateTarget = iota + 1
	// PredTargetKeyNotExist holds when the key does not exist.
	PredTargetKeyNotExist
	// PredTargetKeyExist holds when the key exists.
	PredTargetKeyExist
	// PredTargetUnchangedSince holds when the key did not change after the
	// revision in TargetValue (an int64): its last change (etcd
	// mod_revision) is at or before it, or the key does not exist.
	PredTargetUnchangedSince
)

// Predicate provides interface for kv predicate.
type Predicate interface {
	Target() PredicateTarget
	// IsTrue reports whether the predicate holds for the value of an
	// existing key: the value for PredTargetValueEqual, the mod revision
	// (an int64) for PredTargetUnchangedSince.
	IsTrue(any) bool
	Key() string
	TargetValue() any
}

type predicate struct {
	target PredicateTarget
	k      string
	v      any
}

func (p *predicate) Target() PredicateTarget {
	return p.target
}

func (p *predicate) Key() string {
	return p.k
}

func (p *predicate) TargetValue() any {
	return p.v
}

func (p *predicate) IsTrue(target any) bool {
	switch p.target {
	case PredTargetValueEqual:
		switch v := target.(type) {
		case string:
			return v == p.v
		case []byte:
			return string(v) == p.v
		}
	case PredTargetKeyExist:
		return true
	case PredTargetUnchangedSince:
		rev, ok := target.(int64)
		return ok && rev <= p.v.(int64)
	}
	return false
}

// ValueEqual holds when key exists with value v.
func ValueEqual(k, v string) Predicate {
	return &predicate{target: PredTargetValueEqual, k: k, v: v}
}

// KeyNotExist holds when key does not exist. On TiKV a guard on a key the
// call does not write takes a pessimistic lock on it; on a written key the
// guard detects a concurrent creator as a write-write conflict.
func KeyNotExist(key string) Predicate {
	return &predicate{target: PredTargetKeyNotExist, k: key}
}

// KeyExist holds when key exists. On TiKV it guards an unwritten key with a
// pessimistic lock, like KeyNotExist.
func KeyExist(key string) Predicate {
	return &predicate{target: PredTargetKeyExist, k: key}
}

// UnchangedSince holds when key has not changed after revision: its last
// change is at or before revision, or it does not exist. A key deleted after
// revision does not exist, so the predicate holds for it; pair it with
// KeyExist to require the key to be there.
func UnchangedSince(key string, revision int64) Predicate {
	return &predicate{target: PredTargetUnchangedSince, k: key, v: revision}
}

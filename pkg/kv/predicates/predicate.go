package predicates

// PredicateTarget is enum for Predicate target type.
type PredicateTarget int32

const (
	// PredTargetValue is predicate target for key-value perid
	PredTargetValue PredicateTarget = iota + 1
	// PredTargetKeyExistence is predicate target for whether the key exists.
	PredTargetKeyExistence
)

type PredicateType int32

const (
	PredTypeEqual PredicateType = iota + 1
	PredTypeNotExist
)

// Predicate provides interface for kv predicate.
type Predicate interface {
	Target() PredicateTarget
	Type() PredicateType
	IsTrue(any) bool
	Key() string
	TargetValue() any
}

type valuePredicate struct {
	k, v string
	pt   PredicateType
}

func (p *valuePredicate) Target() PredicateTarget {
	return PredTargetValue
}

func (p *valuePredicate) Type() PredicateType {
	return p.pt
}

func (p *valuePredicate) IsTrue(target any) bool {
	switch v := target.(type) {
	case string:
		return predicateValue(p.pt, v, p.v)
	case []byte:
		return predicateValue(p.pt, string(v), p.v)
	default:
		return false
	}
}

func (p *valuePredicate) Key() string {
	return p.k
}

func (p *valuePredicate) TargetValue() any {
	return p.v
}

func predicateValue[T comparable](pt PredicateType, v1, v2 T) bool {
	switch pt {
	case PredTypeEqual:
		return v1 == v2
	default:
		return false
	}
}

func ValueEqual(k, v string) Predicate {
	return &valuePredicate{
		k:  k,
		v:  v,
		pt: PredTypeEqual,
	}
}

// keyNotExistPredicate holds when its key does not exist in the kv store.
type keyNotExistPredicate struct {
	k string
}

func (p *keyNotExistPredicate) Target() PredicateTarget {
	return PredTargetKeyExistence
}

func (p *keyNotExistPredicate) Type() PredicateType {
	return PredTypeNotExist
}

// IsTrue receives the value of a key that exists, so it is always false.
func (p *keyNotExistPredicate) IsTrue(any) bool {
	return false
}

func (p *keyNotExistPredicate) Key() string {
	return p.k
}

func (p *keyNotExistPredicate) TargetValue() any {
	return nil
}

// KeyNotExist returns a predicate that holds when key does not exist.
// The same call must also write key (in saves): on TiKV the guard detects a
// concurrent creator only as a write-write conflict on key.
func KeyNotExist(key string) Predicate {
	return &keyNotExistPredicate{k: key}
}

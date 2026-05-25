package predicates

// PredicateTarget is enum for Predicate target type.
type PredicateTarget int32

const (
	PredTargetValue          PredicateTarget = iota + 1
	PredTargetCreateRevision                 // etcd key CreateRevision (0 = key does not exist)
	PredTargetModRevision                    // etcd key ModRevision
)

type PredicateType int32

const (
	PredTypeEqual PredicateType = iota + 1
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

type revisionPredicate struct {
	k        string
	revision int64
	target   PredicateTarget
	pt       PredicateType
}

func (p *revisionPredicate) Target() PredicateTarget { return p.target }
func (p *revisionPredicate) Type() PredicateType     { return p.pt }
func (p *revisionPredicate) Key() string             { return p.k }
func (p *revisionPredicate) TargetValue() any        { return p.revision }

func (p *revisionPredicate) IsTrue(target any) bool {
	switch v := target.(type) {
	case int64:
		return predicateValue(p.pt, v, p.revision)
	default:
		return false
	}
}

// KeyNotExists returns a predicate that asserts the key does not exist (CreateRevision == 0).
func KeyNotExists(key string) Predicate {
	return &revisionPredicate{k: key, revision: 0, target: PredTargetCreateRevision, pt: PredTypeEqual}
}

// ModRevisionEqual returns a predicate that asserts the key's ModRevision matches the expected value.
func ModRevisionEqual(key string, revision int64) Predicate {
	return &revisionPredicate{k: key, revision: revision, target: PredTargetModRevision, pt: PredTypeEqual}
}

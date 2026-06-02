package predicates

// PredicateTarget is enum for Predicate target type.
type PredicateTarget int32

const (
	// PredTargetValue is predicate target for key-value perid
	PredTargetValue PredicateTarget = iota + 1
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

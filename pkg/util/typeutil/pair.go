package typeutil

type Pair[T, U any] struct {
	A T
	B U
}

func NewPair[T, U any](a T, b U) Pair[T, U] {
	return Pair[T, U]{A: a, B: b}
}

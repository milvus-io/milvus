package funcutil

/*
type aggFunc[T constraints.Ordered] func(t1, t2 T) T

func agg[T constraints.Ordered](op aggFunc[T], s ...T) T {
	l := len(s)
	if l <= 0 {
		var zero T
		return zero
	}
	m := s[0]
	for i := 1; i < l; i++ {
		m = op(s[i], m)
	}
	return m
}

func getMin[T constraints.Ordered](t1, t2 T) T {
	if t1 < t2 {
		return t1
	}
	return t2
}

func getMax[T constraints.Ordered](t1, t2 T) T {
	if t1 < t2 {
		return t2
	}
	return t1
}

func getSum[T constraints.Ordered](t1, t2 T) T {
	return t1 + t2
}

func Min[T constraints.Ordered](s ...T) T {
	return agg[T](getMin[T], s...)
}

func Max[T constraints.Ordered](s ...T) T {
	return agg[T](getMax[T], s...)
}

func Sum[T constraints.Ordered](s ...T) T {
	return agg[T](getSum[T], s...)
}

*/

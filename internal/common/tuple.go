package common

type Int64Tuple struct {
	Key, Value int64
}

func (t Int64Tuple) Clone() *Int64Tuple {
	return &Int64Tuple{
		Key:   t.Key,
		Value: t.Value,
	}
}

func CloneInt64Tuples(tuples []Int64Tuple) []Int64Tuple {
	clone := make([]Int64Tuple, 0, len(tuples))
	for _, tuple := range tuples {
		clone = append(clone, *tuple.Clone())
	}
	return clone
}

package common

import "reflect"

type Str2Str map[string]string

func (m Str2Str) Clone() Str2Str {
	if m == nil {
		return nil
	}
	clone := make(Str2Str)
	for key, value := range m {
		clone[key] = value
	}
	return clone
}

func (m Str2Str) Equal(other Str2Str) bool {
	return reflect.DeepEqual(m, other)
}

func CloneStr2Str(m Str2Str) Str2Str {
	return m.Clone()
}

package common

import "reflect"

type ByteSlice []byte

func (s ByteSlice) Clone() ByteSlice {
	clone := make(ByteSlice, len(s))
	copy(clone, s)
	return clone
}

func (s ByteSlice) Equal(other ByteSlice) bool {
	return reflect.DeepEqual(s, other)
}

func CloneByteSlice(s ByteSlice) ByteSlice {
	return s.Clone()
}

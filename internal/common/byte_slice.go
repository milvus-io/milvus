package common

type ByteSlice []byte

func (s ByteSlice) Clone() ByteSlice {
	clone := make(ByteSlice, 0, len(s))
	copy(clone, s)
	return clone
}

func CloneByteSlice(s ByteSlice) ByteSlice {
	return s.Clone()
}

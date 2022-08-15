package common

type S2S map[string]string

func (m S2S) Clone() S2S {
	clone := make(S2S)
	for key, value := range m {
		clone[key] = value
	}
	return clone
}

func CloneS2S(m S2S) S2S {
	return m.Clone()
}

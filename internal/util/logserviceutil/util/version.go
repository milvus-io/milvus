package util

type Version interface {
	GT(Version) bool

	Equal(Version) bool
}

func NewVersionInt64() Version {
	return VersionInt64(-1)
}

type VersionInt64 int64

func (v VersionInt64) GT(v2 Version) bool {
	return v > v2.(VersionInt64)
}

func (v VersionInt64) Equal(v2 Version) bool {
	return v == v2.(VersionInt64)
}

type VersionInt64Pair struct {
	Global int64
	Local  int64
}

func NewVersionInt64Pair() Version {
	return &VersionInt64Pair{
		Global: -1,
		Local:  -1,
	}
}

func (v *VersionInt64Pair) GT(v2 Version) bool {
	vPair := v2.(*VersionInt64Pair)
	return v.Global > vPair.Global || (v.Global == vPair.Global && v.Local > vPair.Local)
}

func (v *VersionInt64Pair) Equal(v2 Version) bool {
	vPair := v2.(*VersionInt64Pair)
	return v.Global == vPair.Global && v.Local == vPair.Local
}

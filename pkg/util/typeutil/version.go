package typeutil

import (
	"fmt"
	"strconv"
)

// Version is a interface for version comparison.
type Version interface {
	// GT returns true if v > v2.
	GT(Version) bool

	// EQ returns true if v == v2.
	EQ(Version) bool

	// String returns the string representation of the version.
	String() string
}

// VersionInt64 is a int64 type version.
type VersionInt64 int64

func (v VersionInt64) GT(v2 Version) bool {
	return v > mustCastVersionInt64(v2)
}

func (v VersionInt64) EQ(v2 Version) bool {
	return v == mustCastVersionInt64(v2)
}

func (v VersionInt64) String() string {
	return strconv.FormatInt(int64(v), 10)
}

func mustCastVersionInt64(v2 Version) VersionInt64 {
	if v2i, ok := v2.(VersionInt64); ok {
		return v2i
	} else if v2i, ok := v2.(*VersionInt64); ok {
		return *v2i
	}
	panic("invalid version type")
}

// VersionInt64Pair is a pair of int64 type version.
// It's easy to be used in multi node version comparison.
type VersionInt64Pair struct {
	Global int64
	Local  int64
}

func (v VersionInt64Pair) GT(v2 Version) bool {
	vPair := mustCastVersionInt64Pair(v2)
	return v.Global > vPair.Global || (v.Global == vPair.Global && v.Local > vPair.Local)
}

func (v VersionInt64Pair) EQ(v2 Version) bool {
	vPair := mustCastVersionInt64Pair(v2)
	return v.Global == vPair.Global && v.Local == vPair.Local
}

func (v VersionInt64Pair) String() string {
	return fmt.Sprintf("%d/%d", v.Global, v.Local)
}

func mustCastVersionInt64Pair(v2 Version) VersionInt64Pair {
	if v2i, ok := v2.(VersionInt64Pair); ok {
		return v2i
	} else if v2i, ok := v2.(*VersionInt64Pair); ok {
		return *v2i
	}
	panic("invalid version type")
}

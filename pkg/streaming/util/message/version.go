package message

import "strconv"

var (
	VersionOld Version = 0 // old version before streamingnode, keep in 2.6 and will be removed from 3.0.
	VersionV1  Version = 1 // The message marshal unmarshal still use msgstream.
	VersionV2  Version = 2 // The message marshal unmarshal never rely on msgstream.
)

type Version int // message version for compatibility.

func newMessageVersionFromString(s string) Version {
	if s == "" {
		return VersionOld
	}
	v, err := strconv.ParseInt(s, 10, 64)
	if err != nil {
		panic("unexpected message version")
	}
	return Version(v)
}

func (v Version) String() string {
	return strconv.FormatInt(int64(v), 10)
}

func (v Version) GT(v2 Version) bool {
	return v > v2
}

func (v Version) EQ(v2 Version) bool {
	return v == v2
}

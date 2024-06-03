package message

import "strconv"

var (
	VersionOld Version = 0 // old version before lognode.
	VersionV1  Version = 1
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

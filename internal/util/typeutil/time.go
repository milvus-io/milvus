package typeutil

import "time"

// ZeroTime is a zero time.
var ZeroTime = time.Time{}
var ZeroTimestamp = Timestamp(0)

// ParseTimestamp returns a timestamp for a given byte slice.
func ParseTimestamp(data []byte) (time.Time, error) {
	nano, err := BytesToUint64(data)
	if err != nil {
		return ZeroTime, err
	}

	return time.Unix(0, int64(nano)), nil
}

// SubTimeByWallClock returns the duration between two different timestamps.
func SubTimeByWallClock(after time.Time, before time.Time) time.Duration {
	return time.Duration(after.UnixNano() - before.UnixNano())
}

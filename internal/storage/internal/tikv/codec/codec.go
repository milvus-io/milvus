package codec

import (
	"encoding/binary"
	"errors"
	"github.com/tikv/client-go/codec"
)

var (
	Delimiter        = byte('_')
	DelimiterPlusOne = Delimiter + 0x01
	DeleteMark       = byte('d')
	SegmentIndexMark = byte('i')
	SegmentDLMark    = byte('d')
)

// EncodeKey append timestamp, delimiter, and suffix string
// to one slice key.
// Note: suffix string should not contains Delimiter
func EncodeKey(key []byte, timestamp uint64, suffix string) []byte {
	//TODO: should we encode key to memory comparable
	ret := EncodeDelimiter(key, Delimiter)
	ret = codec.EncodeUintDesc(ret, timestamp)
	return append(ret, suffix...)
}

func DecodeKey(key []byte) ([]byte, uint64, string, error) {
	if len(key) < 8 {
		return nil, 0, "", errors.New("insufficient bytes to decode value")
	}

	lenDeKey := 0
	for i := len(key) - 1; i > 0; i-- {
		if key[i] == Delimiter {
			lenDeKey = i
			break
		}
	}

	if lenDeKey == 0 || lenDeKey+8 > len(key) {
		return nil, 0, "", errors.New("insufficient bytes to decode value")
	}

	tsBytes := key[lenDeKey+1 : lenDeKey+9]
	ts := binary.BigEndian.Uint64(tsBytes)
	suffix := string(key[lenDeKey+9:])
	key = key[:lenDeKey-1]
	return key, ^ts, suffix, nil
}

// EncodeDelimiter append a delimiter byte to slice b, and return the appended slice.
func EncodeDelimiter(b []byte, delimiter byte) []byte {
	return append(b, delimiter)
}

func EncodeSegment(segName []byte, segType byte) []byte {
	segmentKey := []byte("segment")
	segmentKey = append(segmentKey, Delimiter)
	segmentKey = append(segmentKey, segName...)
	return append(segmentKey, Delimiter, segType)
}

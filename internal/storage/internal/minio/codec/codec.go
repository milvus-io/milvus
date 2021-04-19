package codec

import (
	"errors"
	"fmt"
)

func MvccEncode(key []byte, ts uint64, suffix string) ([]byte, error) {
	return []byte(string(key) + "_" + fmt.Sprintf("%016x", ^ts) + "_" + suffix), nil
}

func MvccDecode(key []byte) (string, uint64, string, error) {
	if len(key) < 16 {
		return "", 0, "", errors.New("insufficient bytes to decode value")
	}

	suffixIndex := 0
	TSIndex := 0
	undersCount := 0
	for i := len(key) - 1; i > 0; i-- {
		if key[i] == byte('_') {
			undersCount++
			if undersCount == 1 {
				suffixIndex = i + 1
			}
			if undersCount == 2 {
				TSIndex = i + 1
				break
			}
		}
	}
	if suffixIndex == 0 || TSIndex == 0 {
		return "", 0, "", errors.New("key is wrong formatted")
	}

	var TS uint64
	_, err := fmt.Sscanf(string(key[TSIndex:suffixIndex-1]), "%x", &TS)
	TS = ^TS
	if err != nil {
		return "", 0, "", err
	}

	return string(key[0 : TSIndex-1]), TS, string(key[suffixIndex:]), nil
}

func LogEncode(key []byte, ts uint64, channel int) []byte {
	suffix := string(key) + "_" + fmt.Sprintf("%d", channel)
	logKey, err := MvccEncode([]byte("log"), ts, suffix)
	if err != nil {
		return nil
	}
	return logKey
}

func LogDecode(logKey string) (string, uint64, int, error) {
	if len(logKey) < 16 {
		return "", 0, 0, errors.New("insufficient bytes to decode value")
	}

	channelIndex := 0
	keyIndex := 0
	TSIndex := 0
	undersCount := 0

	for i := len(logKey) - 1; i > 0; i-- {
		if logKey[i] == '_' {
			undersCount++
			if undersCount == 1 {
				channelIndex = i + 1
			}
			if undersCount == 2 {
				keyIndex = i + 1
			}
			if undersCount == 3 {
				TSIndex = i + 1
				break
			}
		}
	}
	if channelIndex == 0 || TSIndex == 0 || keyIndex == 0 || logKey[:TSIndex-1] != "log" {
		return "", 0, 0, errors.New("key is wrong formatted")
	}

	var TS uint64
	var channel int
	_, err := fmt.Sscanf(logKey[TSIndex:keyIndex-1], "%x", &TS)
	if err != nil {
		return "", 0, 0, err
	}
	TS = ^TS

	_, err = fmt.Sscanf(logKey[channelIndex:], "%d", &channel)
	if err != nil {
		return "", 0, 0, err
	}
	return logKey[keyIndex : channelIndex-1], TS, channel, nil
}

func SegmentEncode(segment string, suffix string) []byte {
	return []byte(segment + "_" + suffix)
}

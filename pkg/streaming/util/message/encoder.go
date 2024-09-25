package message

import (
	"encoding/base64"
	"strconv"

	"github.com/cockroachdb/errors"
	"google.golang.org/protobuf/proto"
)

const base = 36

// EncodeInt64 encodes int64 to string.
func EncodeInt64(value int64) string {
	return strconv.FormatInt(value, base)
}

// EncodeUint64 encodes uint64 to string.
func EncodeUint64(value uint64) string {
	return strconv.FormatUint(value, base)
}

// DecodeUint64 decodes string to uint64.
func DecodeUint64(value string) (uint64, error) {
	return strconv.ParseUint(value, base, 64)
}

// DecodeInt64 decodes string to int64.
func DecodeInt64(value string) (int64, error) {
	return strconv.ParseInt(value, base, 64)
}

// EncodeProto encodes proto message to json string.
func EncodeProto(m proto.Message) (string, error) {
	result, err := proto.Marshal(m)
	if err != nil {
		return "", err
	}
	return base64.StdEncoding.EncodeToString(result), nil
}

// DecodeProto
func DecodeProto(data string, m proto.Message) error {
	val, err := base64.StdEncoding.DecodeString(data)
	if err != nil {
		return errors.Wrap(err, "failed to decode base64")
	}
	return proto.Unmarshal(val, m)
}

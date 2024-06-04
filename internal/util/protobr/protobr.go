package protobr

import (
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/protoadapt"
)

// Deprecated: remove later
func Marshal(m protoadapt.MessageV1) ([]byte, error) {
	return proto.Marshal(protoadapt.MessageV2Of(m))
}

// Deprecated: remove later
func Unmarshal(b []byte, m protoadapt.MessageV1) error {
	return proto.Unmarshal(b, protoadapt.MessageV2Of(m))
}

// Deprecated: remove later
func Size(m protoadapt.MessageV1) int {
	return proto.Size(protoadapt.MessageV2Of(m))
}

// Deprecated: remove later
func Equal(m1, m2 protoadapt.MessageV1) bool {
	return proto.Equal(protoadapt.MessageV2Of(m1), protoadapt.MessageV2Of(m2))
}

// Deprecated: remove later
func Clone(m protoadapt.MessageV1) protoadapt.MessageV1 {
	return protoadapt.MessageV1Of(proto.Clone(protoadapt.MessageV2Of(m)))
}

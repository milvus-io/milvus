package id

import (
	"encoding/binary"

	"github.com/rs/xid"

	"github.com/czs007/suvlim/internal/errors"
)

type ID struct {
	xid.ID
}

func BytesToUint64(b []byte) (uint64, error) {
	if len(b) != 12 {
		return 0, errors.Errorf("invalid data, must 12 bytes, but %d", len(b))
	}

	return binary.BigEndian.Uint64(b), nil
}

// Uint64ToBytes converts uint64 to a byte slice.
func Uint64ToBytes(v uint64) []byte {
	b := make([]byte, 12)
	binary.BigEndian.PutUint64(b, v)
	return b
}

func New() ID {
	return ID{
		xid.New(),
	}
}

func (id ID) Uint64() uint64 {
	b := id.Bytes()
	if len(b) != 12 {
		return 0
	}
	return binary.BigEndian.Uint64(b)

}

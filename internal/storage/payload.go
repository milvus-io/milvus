package storage

/*
#cgo CFLAGS: -I${SRCDIR}/cwrapper

#cgo LDFLAGS: -L${SRCDIR}/cwrapper/output -l:libwrapper.a -l:libparquet.a -l:libarrow.a -l:libthrift.a -l:libutf8proc.a -lstdc++ -lm
#include <stdlib.h>
#include "ParquetWrapper.h"
*/
import "C"
import (
	"unsafe"

	"github.com/zilliztech/milvus-distributed/internal/errors"
	"github.com/zilliztech/milvus-distributed/internal/proto/commonpb"
	"github.com/zilliztech/milvus-distributed/internal/proto/schemapb"
)

type PayloadWriter struct {
	payloadWriterPtr C.CPayloadWriter
}

func NewPayloadWriter(colType schemapb.DataType) (*PayloadWriter, error) {
	w := C.NewPayloadWriter(C.int(colType))
	if w == nil {
		return nil, errors.New("create Payload writer failed")
	}
	return &PayloadWriter{payloadWriterPtr: w}, nil
}

func (w *PayloadWriter) AddOneStringToPayload(msg string) error {
	if len(msg) == 0 {
		return errors.New("can't add empty string into payload")
	}
	cstr := C.CString(msg)
	defer C.free(unsafe.Pointer(cstr))
	st := C.AddOneStringToPayload(w.payloadWriterPtr, cstr, C.int(len(msg)))
	errCode := commonpb.ErrorCode(st.error_code)
	if errCode != commonpb.ErrorCode_SUCCESS {
		msg := C.GoString(st.error_msg)
		defer C.free(unsafe.Pointer(st.error_msg))
		return errors.New(msg)
	}
	return nil
}

func (w *PayloadWriter) FinishPayloadWriter() error {
	st := C.FinishPayloadWriter(w.payloadWriterPtr)
	errCode := commonpb.ErrorCode(st.error_code)
	if errCode != commonpb.ErrorCode_SUCCESS {
		msg := C.GoString(st.error_msg)
		defer C.free(unsafe.Pointer(st.error_msg))
		return errors.New(msg)
	}
	return nil
}

func (w *PayloadWriter) GetPayloadBufferFromWriter() ([]byte, error) {
	//See: https://github.com/golang/go/wiki/cgo#turning-c-arrays-into-go-slices
	cb := C.GetPayloadBufferFromWriter(w.payloadWriterPtr)
	pointer := unsafe.Pointer(cb.data)
	length := int(cb.length)
	if length <= 0 {
		return nil, errors.New("empty buffer")
	}
	slice := (*[1 << 28]byte)(pointer)[:length:length]
	return slice, nil
}

func (w *PayloadWriter) GetPayloadLengthFromWriter() (int, error) {
	length := C.GetPayloadLengthFromWriter(w.payloadWriterPtr)
	return int(length), nil
}

func (w *PayloadWriter) ReleasePayloadWriter() error {
	st := C.ReleasePayloadWriter(w.payloadWriterPtr)
	errCode := commonpb.ErrorCode(st.error_code)
	if errCode != commonpb.ErrorCode_SUCCESS {
		msg := C.GoString(st.error_msg)
		defer C.free(unsafe.Pointer(st.error_msg))
		return errors.New(msg)
	}
	return nil
}

func (w *PayloadWriter) Close() error {
	return w.ReleasePayloadWriter()
}

type PayloadReader struct {
	payloadReaderPtr C.CPayloadReader
}

func NewPayloadReader(colType schemapb.DataType, buf []byte) (*PayloadReader, error) {
	if len(buf) == 0 {
		return nil, errors.New("create Payload reader failed, buffer is empty")
	}
	r := C.NewPayloadReader(C.int(colType), (*C.uchar)(unsafe.Pointer(&buf[0])), C.long(len(buf)))
	return &PayloadReader{payloadReaderPtr: r}, nil
}

func (r *PayloadReader) ReleasePayloadReader() error {
	st := C.ReleasePayloadReader(r.payloadReaderPtr)
	errCode := commonpb.ErrorCode(st.error_code)
	if errCode != commonpb.ErrorCode_SUCCESS {
		msg := C.GoString(st.error_msg)
		defer C.free(unsafe.Pointer(st.error_msg))
		return errors.New(msg)
	}
	return nil
}

func (r *PayloadReader) GetOneStringFromPayload(idx int) (string, error) {
	var cStr *C.char
	var strSize C.int

	st := C.GetOneStringFromPayload(r.payloadReaderPtr, C.int(idx), &cStr, &strSize)
	errCode := commonpb.ErrorCode(st.error_code)
	if errCode != commonpb.ErrorCode_SUCCESS {
		msg := C.GoString(st.error_msg)
		defer C.free(unsafe.Pointer(st.error_msg))
		return "", errors.New(msg)
	}
	return C.GoStringN(cStr, strSize), nil
}

func (r *PayloadReader) GetPayloadLengthFromReader() (int, error) {
	length := C.GetPayloadLengthFromReader(r.payloadReaderPtr)
	return int(length), nil
}

func (r *PayloadReader) Close() error {
	return r.ReleasePayloadReader()
}

package importutil

import (
	"bytes"
	"encoding/binary"
	"errors"
	"io"
	"os"

	"github.com/sbinet/npyio"
	"github.com/sbinet/npyio/npy"
)

func CreateNumpyFile(path string, data interface{}) error {
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	defer f.Close()

	err = npyio.Write(f, data)
	if err != nil {
		return err
	}

	return nil
}

func CreateNumpyData(data interface{}) ([]byte, error) {
	buf := new(bytes.Buffer)
	err := npyio.Write(buf, data)
	if err != nil {
		return nil, err
	}

	return buf.Bytes(), nil
}

// a class to expand other numpy lib ability
// we evaluate two go-numpy lins: github.com/kshedden/gonpy and github.com/sbinet/npyio
// the npyio lib read data one by one, the performance is poor, we expand the read methods
// to read data in one batch, the performance is 100X faster
// the gonpy lib also read data in one batch, but it has no method to read bool data, and the ability
// to handle different data type is not strong as the npylib, so we choose the npyio lib to expand.
type NumpyAdapter struct {
	reader       io.Reader        // data source, typically is os.File
	npyReader    *npy.Reader      // reader of npyio lib
	order        binary.ByteOrder // LittleEndian or BigEndian
	readPosition int              // how many elements have been read
}

func NewNumpyAdapter(reader io.Reader) (*NumpyAdapter, error) {
	r, err := npyio.NewReader(reader)
	if err != nil {
		return nil, err
	}
	adapter := &NumpyAdapter{
		reader:       reader,
		npyReader:    r,
		readPosition: 0,
	}
	adapter.setByteOrder()

	return adapter, err
}

// the logic of this method is copied from npyio lib
func (n *NumpyAdapter) setByteOrder() {
	var nativeEndian binary.ByteOrder
	v := uint16(1)
	switch byte(v >> 8) {
	case 0:
		nativeEndian = binary.LittleEndian
	case 1:
		nativeEndian = binary.BigEndian
	}

	switch n.npyReader.Header.Descr.Type[0] {
	case '<':
		n.order = binary.LittleEndian
	case '>':
		n.order = binary.BigEndian
	default:
		n.order = nativeEndian
	}
}

func (n *NumpyAdapter) Reader() io.Reader {
	return n.reader
}

func (n *NumpyAdapter) NpyReader() *npy.Reader {
	return n.npyReader
}

func (n *NumpyAdapter) GetType() string {
	return n.npyReader.Header.Descr.Type
}

func (n *NumpyAdapter) GetShape() []int {
	return n.npyReader.Header.Descr.Shape
}

func (n *NumpyAdapter) checkSize(size int) int {
	shape := n.GetShape()

	// empty file?
	if len(shape) == 0 {
		return 0
	}

	total := 1
	for i := 0; i < len(shape); i++ {
		total *= shape[i]
	}

	if total == 0 {
		return 0
	}

	// overflow?
	if size > (total - n.readPosition) {
		return total - n.readPosition
	}

	return size
}

func (n *NumpyAdapter) ReadBool(size int) ([]bool, error) {
	if n.npyReader == nil {
		return nil, errors.New("reader is not initialized")
	}

	// incorrect type
	switch n.npyReader.Header.Descr.Type {
	case "b1", "<b1", "|b1", "bool":
	default:
		return nil, errors.New("numpy data is not bool type")
	}

	// avoid read overflow
	readSize := n.checkSize(size)
	if readSize <= 0 {
		return nil, errors.New("nothing to read")
	}

	data := make([]bool, readSize)
	err := binary.Read(n.reader, n.order, &data)
	if err != nil {
		return nil, err
	}

	// update read position after successfully read
	n.readPosition += readSize

	return data, nil
}

func (n *NumpyAdapter) ReadUint8(size int) ([]uint8, error) {
	if n.npyReader == nil {
		return nil, errors.New("reader is not initialized")
	}

	// incorrect type
	switch n.npyReader.Header.Descr.Type {
	case "u1", "<u1", "|u1", "uint8":
	default:
		return nil, errors.New("numpy data is not uint8 type")
	}

	// avoid read overflow
	readSize := n.checkSize(size)
	if readSize <= 0 {
		return nil, errors.New("nothing to read")
	}

	data := make([]uint8, readSize)
	err := binary.Read(n.reader, n.order, &data)
	if err != nil {
		return nil, err
	}

	// update read position after successfully read
	n.readPosition += readSize

	return data, nil
}

func (n *NumpyAdapter) ReadInt8(size int) ([]int8, error) {
	if n.npyReader == nil {
		return nil, errors.New("reader is not initialized")
	}

	// incorrect type
	switch n.npyReader.Header.Descr.Type {
	case "i1", "<i1", "|i1", ">i1", "int8":
	default:
		return nil, errors.New("numpy data is not int8 type")
	}

	// avoid read overflow
	readSize := n.checkSize(size)
	if readSize <= 0 {
		return nil, errors.New("nothing to read")
	}

	data := make([]int8, readSize)
	err := binary.Read(n.reader, n.order, &data)
	if err != nil {
		return nil, err
	}

	// update read position after successfully read
	n.readPosition += readSize

	return data, nil
}

func (n *NumpyAdapter) ReadInt16(size int) ([]int16, error) {
	if n.npyReader == nil {
		return nil, errors.New("reader is not initialized")
	}

	// incorrect type
	switch n.npyReader.Header.Descr.Type {
	case "i2", "<i2", "|i2", ">i2", "int16":
	default:
		return nil, errors.New("numpy data is not int16 type")
	}

	// avoid read overflow
	readSize := n.checkSize(size)
	if readSize <= 0 {
		return nil, errors.New("nothing to read")
	}

	data := make([]int16, readSize)
	err := binary.Read(n.reader, n.order, &data)
	if err != nil {
		return nil, err
	}

	// update read position after successfully read
	n.readPosition += readSize

	return data, nil
}

func (n *NumpyAdapter) ReadInt32(size int) ([]int32, error) {
	if n.npyReader == nil {
		return nil, errors.New("reader is not initialized")
	}

	// incorrect type
	switch n.npyReader.Header.Descr.Type {
	case "i4", "<i4", "|i4", ">i4", "int32":
	default:
		return nil, errors.New("numpy data is not int32 type")
	}

	// avoid read overflow
	readSize := n.checkSize(size)
	if readSize <= 0 {
		return nil, errors.New("nothing to read")
	}

	data := make([]int32, readSize)
	err := binary.Read(n.reader, n.order, &data)
	if err != nil {
		return nil, err
	}

	// update read position after successfully read
	n.readPosition += readSize

	return data, nil
}

func (n *NumpyAdapter) ReadInt64(size int) ([]int64, error) {
	if n.npyReader == nil {
		return nil, errors.New("reader is not initialized")
	}

	// incorrect type
	switch n.npyReader.Header.Descr.Type {
	case "i8", "<i8", "|i8", ">i8", "int64":
	default:
		return nil, errors.New("numpy data is not int64 type")
	}

	// avoid read overflow
	readSize := n.checkSize(size)
	if readSize <= 0 {
		return nil, errors.New("nothing to read")
	}

	data := make([]int64, readSize)
	err := binary.Read(n.reader, n.order, &data)
	if err != nil {
		return nil, err
	}

	// update read position after successfully read
	n.readPosition += readSize

	return data, nil
}

func (n *NumpyAdapter) ReadFloat32(size int) ([]float32, error) {
	if n.npyReader == nil {
		return nil, errors.New("reader is not initialized")
	}

	// incorrect type
	switch n.npyReader.Header.Descr.Type {
	case "f4", "<f4", "|f4", ">f4", "float32":
	default:
		return nil, errors.New("numpy data is not float32 type")
	}

	// avoid read overflow
	readSize := n.checkSize(size)
	if readSize <= 0 {
		return nil, errors.New("nothing to read")
	}

	data := make([]float32, readSize)
	err := binary.Read(n.reader, n.order, &data)
	if err != nil {
		return nil, err
	}

	// update read position after successfully read
	n.readPosition += readSize

	return data, nil
}

func (n *NumpyAdapter) ReadFloat64(size int) ([]float64, error) {
	if n.npyReader == nil {
		return nil, errors.New("reader is not initialized")
	}

	// incorrect type
	switch n.npyReader.Header.Descr.Type {
	case "f8", "<f8", "|f8", ">f8", "float64":
	default:
		return nil, errors.New("numpy data is not float32 type")
	}

	// avoid read overflow
	readSize := n.checkSize(size)
	if readSize <= 0 {
		return nil, errors.New("nothing to read")
	}

	data := make([]float64, readSize)
	err := binary.Read(n.reader, n.order, &data)
	if err != nil {
		return nil, err
	}

	// update read position after successfully read
	n.readPosition += readSize

	return data, nil
}

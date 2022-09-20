package storage

import (
	"fmt"
	"testing"

	"github.com/apache/arrow/go/v8/parquet/file"
	"github.com/stretchr/testify/suite"
)

type ReadDataFromAllRowGroupsSuite struct {
	suite.Suite
	size int

	logData []byte

	reader *PayloadReader
}

func (s *ReadDataFromAllRowGroupsSuite) SetupSuite() {
	w := NewIndexFileBinlogWriter(0, 0, 1, 2, 3, 100, "", 0, "test")
	defer w.Close()
	ew, err := w.NextIndexFileEventWriter()
	s.Require().NoError(err)
	defer ew.Close()

	s.size = 1 << 10

	data := make([]byte, s.size)
	err = ew.AddByteToPayload(data)
	s.Require().NoError(err)

	ew.SetEventTimestamp(1, 1)
	w.SetEventTimeStamp(1, 1)

	w.AddExtra(originalSizeKey, fmt.Sprintf("%v", len(data)))

	err = w.Finish()
	s.Require().NoError(err)

	buffer, err := w.GetBuffer()
	s.Require().NoError(err)

	s.logData = buffer
}

func (s *ReadDataFromAllRowGroupsSuite) TearDownSuite() {}

func (s *ReadDataFromAllRowGroupsSuite) SetupTest() {
	br, err := NewBinlogReader(s.logData)
	s.Require().NoError(err)
	er, err := br.NextEventReader()
	s.Require().NoError(err)

	reader, ok := er.PayloadReaderInterface.(*PayloadReader)
	s.Require().True(ok)

	s.reader = reader
}

func (s *ReadDataFromAllRowGroupsSuite) TearDownTest() {
	s.reader.Close()
	s.reader = nil
}

func (s *ReadDataFromAllRowGroupsSuite) TestNormalRun() {
	values := make([]int32, s.size)
	valuesRead, err := ReadDataFromAllRowGroups[int32, *file.Int32ColumnChunkReader](s.reader.reader, values, 0, int64(s.size))
	s.Assert().NoError(err)
	s.Assert().EqualValues(s.size, valuesRead)
}

func (s *ReadDataFromAllRowGroupsSuite) TestColIdxOutOfRange() {
	values := make([]int32, s.size)
	_, err := ReadDataFromAllRowGroups[int32, *file.Int32ColumnChunkReader](s.reader.reader, values, 1, int64(s.size))
	s.Assert().Error(err)
}

func TestReadDataFromAllRowGroupsSuite(t *testing.T) {
	suite.Run(t, new(ReadDataFromAllRowGroupsSuite))
}

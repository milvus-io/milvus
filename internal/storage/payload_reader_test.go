package storage

import (
	"fmt"
	"testing"

	"github.com/apache/arrow/go/v17/parquet/file"
	"github.com/stretchr/testify/suite"

	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
)

type ReadDataFromAllRowGroupsSuite struct {
	suite.Suite
	size int

	logData []byte

	reader *PayloadReader
}

func (s *ReadDataFromAllRowGroupsSuite) SetupSuite() {
	w := NewInsertBinlogWriter(schemapb.DataType_Int8, 1, 2, 3, 4, false)
	defer w.Close()
	// make sure it's still written int8 data
	w.PayloadDataType = schemapb.DataType_Int8
	ew, err := w.NextInsertEventWriter()
	s.Require().NoError(err)
	defer ew.Close()

	s.size = 1 << 10

	data := make([]int8, s.size)
	err = ew.AddInt8ToPayload(data, nil)
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

func TestReadDataFromAllRowGroupsSuite(t *testing.T) {
	suite.Run(t, new(ReadDataFromAllRowGroupsSuite))
}

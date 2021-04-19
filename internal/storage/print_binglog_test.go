package storage

import (
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/zilliztech/milvus-distributed/internal/proto/schemapb"
	"github.com/zilliztech/milvus-distributed/internal/util/tsoutil"
)

func TestPrintBinlogFilesInt64(t *testing.T) {
	w, err := NewInsertBinlogWriter(schemapb.DataType_INT64, 10, 20, 30, 40)
	assert.Nil(t, err)

	curTS := time.Now().UnixNano() / int64(time.Millisecond)

	e1, err := w.NextInsertEventWriter()
	assert.Nil(t, err)
	err = e1.AddDataToPayload([]int64{1, 2, 3})
	assert.Nil(t, err)
	err = e1.AddDataToPayload([]int32{4, 5, 6})
	assert.NotNil(t, err)
	err = e1.AddDataToPayload([]int64{4, 5, 6})
	assert.Nil(t, err)
	e1.SetStartTimestamp(tsoutil.ComposeTS(curTS+10*60*1000, 0))
	e1.SetEndTimestamp(tsoutil.ComposeTS(curTS+20*60*1000, 0))

	e2, err := w.NextInsertEventWriter()
	assert.Nil(t, err)
	err = e2.AddDataToPayload([]int64{7, 8, 9})
	assert.Nil(t, err)
	err = e2.AddDataToPayload([]bool{true, false, true})
	assert.NotNil(t, err)
	err = e2.AddDataToPayload([]int64{10, 11, 12})
	assert.Nil(t, err)
	e2.SetStartTimestamp(tsoutil.ComposeTS(curTS+30*60*1000, 0))
	e2.SetEndTimestamp(tsoutil.ComposeTS(curTS+40*60*1000, 0))

	w.SetStartTimeStamp(tsoutil.ComposeTS(curTS, 0))
	w.SetEndTimeStamp(tsoutil.ComposeTS(curTS+3600*1000, 0))

	_, err = w.GetBuffer()
	assert.NotNil(t, err)
	err = w.Close()
	assert.Nil(t, err)
	buf, err := w.GetBuffer()
	assert.Nil(t, err)

	fd, err := os.Create("/tmp/binlog_int64.db")
	assert.Nil(t, err)
	num, err := fd.Write(buf)
	assert.Nil(t, err)
	assert.Equal(t, num, len(buf))
	err = fd.Close()
	assert.Nil(t, err)
}

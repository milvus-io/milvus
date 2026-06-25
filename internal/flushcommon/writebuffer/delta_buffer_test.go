package writebuffer

import (
	"fmt"
	"testing"
	"time"

	"github.com/samber/lo"
	"github.com/stretchr/testify/suite"

	"github.com/milvus-io/milvus-proto/go-api/v3/msgpb"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v3/util/tsoutil"
)

type DeltaBufferSuite struct {
	suite.Suite
}

func (s *DeltaBufferSuite) TestBuffer() {
	s.Run("int64_pk", func() {
		deltaBuffer := NewDeltaBuffer()

		tss := lo.RepeatBy(100, func(idx int) uint64 { return tsoutil.ComposeTSByTime(time.Now(), int64(idx)) })
		pks := lo.Map(tss, func(ts uint64, _ int) storage.PrimaryKey { return storage.NewInt64PrimaryKey(int64(ts)) })

		memSize := deltaBuffer.Buffer(pks, tss, &msgpb.MsgPosition{Timestamp: 100}, &msgpb.MsgPosition{Timestamp: 200})
		// 24 = 16(pk) + 8(ts)
		s.EqualValues(100*24, memSize)
	})

	s.Run("string_pk", func() {
		deltaBuffer := NewDeltaBuffer()

		tss := lo.RepeatBy(100, func(idx int) uint64 { return tsoutil.ComposeTSByTime(time.Now(), int64(idx)) })
		pks := lo.Map(tss, func(ts uint64, idx int) storage.PrimaryKey {
			return storage.NewVarCharPrimaryKey(fmt.Sprintf("%03d", idx))
		})

		memSize := deltaBuffer.Buffer(pks, tss, &msgpb.MsgPosition{Timestamp: 100}, &msgpb.MsgPosition{Timestamp: 200})
		// 19 = (3+8)(string pk) + 8(ts)
		s.EqualValues(100*19, memSize)
	})

	s.Run("predicate", func() {
		deltaBuffer := NewDeltaBuffer()

		memSize := deltaBuffer.BufferPredicate([]byte("expr"), 100, &msgpb.MsgPosition{Timestamp: 100}, &msgpb.MsgPosition{Timestamp: 200})
		// 12 = 4(expr) + 8(ts)
		s.EqualValues(12, memSize)
		s.EqualValues(1, deltaBuffer.rows)
		s.EqualValues(12, deltaBuffer.size)
		s.EqualValues(100, deltaBuffer.TimestampFrom)
		s.EqualValues(100, deltaBuffer.TimestampTo)
	})
}

func (s *DeltaBufferSuite) TestYield() {
	deltaBuffer := NewDeltaBuffer()

	result := deltaBuffer.Yield()
	s.Nil(result)

	deltaBuffer = NewDeltaBuffer()

	tss := lo.RepeatBy(100, func(idx int) uint64 { return tsoutil.ComposeTSByTime(time.Now(), int64(idx)) })
	pks := lo.Map(tss, func(ts uint64, _ int) storage.PrimaryKey { return storage.NewInt64PrimaryKey(int64(ts)) })

	deltaBuffer.Buffer(pks, tss, &msgpb.MsgPosition{Timestamp: 100}, &msgpb.MsgPosition{Timestamp: 200})

	result = deltaBuffer.Yield()
	s.NotNil(result)

	s.ElementsMatch(tss, result.Tss)
	s.ElementsMatch(pks, result.Pks)

	deltaBuffer = NewDeltaBuffer()
	deltaBuffer.BufferPredicate([]byte("expr"), 100, &msgpb.MsgPosition{Timestamp: 100}, &msgpb.MsgPosition{Timestamp: 200})

	result = deltaBuffer.Yield()
	s.NotNil(result)
	s.Equal([][]byte{[]byte("expr")}, result.SerializedExprPlans)
	s.Equal([]uint64{100}, result.PredicateTss)
}

func (s *DeltaBufferSuite) SetupSuite() {
	paramtable.Init()
}

func TestDeltaBuffer(t *testing.T) {
	suite.Run(t, new(DeltaBufferSuite))
}

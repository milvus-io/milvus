package iterator

import (
	"testing"

	"github.com/stretchr/testify/suite"

	"github.com/milvus-io/milvus/internal/storage"
)

func TestDeltalogIteratorSuite(t *testing.T) {
	suite.Run(t, new(DeltalogIteratorSuite))
}

type DeltalogIteratorSuite struct {
	suite.Suite
}

func (s *DeltalogIteratorSuite) TestDeltalogIteratorIntPK() {
	s.Run("invalid blobs", func() {
		iter := NewDeltalogIterator([][]byte{}, nil)

		s.NotNil(iter)
		s.False(iter.HasNext())
	})

	testpks := []int64{1, 2, 3, 4}
	testtss := []uint64{43757345, 43757346, 43757347, 43757348}

	dData := &storage.DeleteData{}
	for i := range testpks {
		dData.Append(storage.NewInt64PrimaryKey(testpks[i]), testtss[i])
	}

	dCodec := storage.NewDeleteCodec()
	blob, err := dCodec.Serialize(10000, 1, 1, dData)
	s.Require().NoError(err)
	value := [][]byte{blob.Value}

	iter := NewDeltalogIterator(value, &Label{segmentID: 100})
	s.NotNil(iter)

	var (
		gotpks = []int64{}
		gottss = []uint64{}
	)

	for iter.HasNext() {
		labeled, err := iter.Next()
		s.NoError(err)

		s.Equal(labeled.GetSegmentID(), int64(100))
		gotpks = append(gotpks, labeled.GetPk().GetValue().(int64))
		gottss = append(gottss, labeled.GetTimestamp())
	}

	s.ElementsMatch(gotpks, testpks)
	s.ElementsMatch(gottss, testtss)

	_, err = iter.Next()
	s.ErrorIs(err, ErrNoMoreRecord)

	iter.Dispose()
	iter.WaitForDisposed()

	_, err = iter.Next()
	s.ErrorIs(err, ErrDisposed)
}

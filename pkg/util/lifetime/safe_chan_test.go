package lifetime

import (
	"testing"

	"github.com/stretchr/testify/suite"

	"github.com/milvus-io/milvus/pkg/v2/util/typeutil"
)

type SafeChanSuite struct {
	suite.Suite
}

func (s *SafeChanSuite) TestClose() {
	sc := NewSafeChan()

	s.False(sc.IsClosed(), "IsClosed() shall return false before Close()")
	s.False(typeutil.IsChanClosed(sc.CloseCh()), "CloseCh() returned channel shall not be closed before Close()")

	s.NotPanics(func() {
		sc.Close()
	}, "SafeChan shall not panic during first close")

	s.True(sc.IsClosed(), "IsClosed() shall return true after Close()")
	s.True(typeutil.IsChanClosed(sc.CloseCh()), "CloseCh() returned channel shall be closed after Close()")

	s.NotPanics(func() {
		sc.Close()
	}, "SafeChan shall not panic during second close")

	s.True(sc.IsClosed(), "IsClosed() shall return true after double Close()")
	s.True(typeutil.IsChanClosed(sc.CloseCh()), "CloseCh() returned channel shall be still closed after double Close()")
}

func TestSafeChan(t *testing.T) {
	suite.Run(t, new(SafeChanSuite))
}

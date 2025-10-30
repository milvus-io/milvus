package etcdkv

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/suite"
	"google.golang.org/grpc/metadata"

	"github.com/milvus-io/milvus/pkg/v2/kv/predicates"
	"github.com/milvus-io/milvus/pkg/v2/util"
)

type EtcdKVUtilSuite struct {
	suite.Suite
}

func (s *EtcdKVUtilSuite) TestParsePredicateType() {
	type testCase struct {
		tag           string
		pt            predicates.PredicateType
		expectResult  string
		expectSucceed bool
	}

	cases := []testCase{
		{tag: "equal", pt: predicates.PredTypeEqual, expectResult: "=", expectSucceed: true},
		{tag: "zero_value", pt: 0, expectResult: "", expectSucceed: false},
	}

	for _, tc := range cases {
		s.Run(tc.tag, func() {
			result, err := parsePredicateType(tc.pt)
			if tc.expectSucceed {
				s.NoError(err)
				s.Equal(tc.expectResult, result)
			} else {
				s.Error(err)
			}
		})
	}
}

func (s *EtcdKVUtilSuite) TestParsePredicates() {
	type testCase struct {
		tag           string
		input         []predicates.Predicate
		expectSucceed bool
	}

	badPredicate := predicates.NewMockPredicate(s.T())
	badPredicate.EXPECT().Target().Return(0)

	cases := []testCase{
		{tag: "normal_value_equal", input: []predicates.Predicate{predicates.ValueEqual("a", "b")}, expectSucceed: true},
		{tag: "empty_input", input: nil, expectSucceed: true},
		{tag: "bad_predicates", input: []predicates.Predicate{badPredicate}, expectSucceed: false},
	}

	for _, tc := range cases {
		s.Run(tc.tag, func() {
			result, err := parsePredicates("", tc.input...)
			if tc.expectSucceed {
				s.NoError(err)
				s.Equal(len(tc.input), len(result))
			} else {
				s.Error(err)
			}
		})
	}
}

func (s *EtcdKVUtilSuite) TestGetContextWithTimeout() {
	s.Run("remove_auth_from_metadata", func() {
		// Create a parent context with metadata including auth information
		parentMD := metadata.New(map[string]string{
			strings.ToLower(util.HeaderAuthorize): "test-auth-token",
			strings.ToLower(util.HeaderToken):     "test-token",
			"other-key":                           "other-value",
			"tenant-id":                           "test-tenant",
		})
		parentCtx := metadata.NewIncomingContext(context.Background(), parentMD)

		// Call getContextWithTimeout
		newCtx, cancel := getContextWithTimeout(parentCtx, 5*time.Second)
		defer cancel()

		// Verify that the new context's metadata does not contain auth information
		newMD, ok := metadata.FromIncomingContext(newCtx)
		s.True(ok, "new context should have metadata")

		// Auth keys should be removed
		authValues := newMD.Get(strings.ToLower(util.HeaderAuthorize))
		s.Empty(authValues, "authorization should be removed from new context")

		tokenValues := newMD.Get(strings.ToLower(util.HeaderToken))
		s.Empty(tokenValues, "token should be removed from new context")

		// Other keys should be preserved
		otherValues := newMD.Get("other-key")
		s.Equal([]string{"other-value"}, otherValues, "other metadata should be preserved")

		tenantValues := newMD.Get("tenant-id")
		s.Equal([]string{"test-tenant"}, tenantValues, "tenant-id should be preserved")

		// Verify that the parent context's metadata is not affected
		parentMDAfter, ok := metadata.FromIncomingContext(parentCtx)
		s.True(ok, "parent context should still have metadata")

		parentAuthValues := parentMDAfter.Get(strings.ToLower(util.HeaderAuthorize))
		s.Equal([]string{"test-auth-token"}, parentAuthValues, "parent context auth should not be modified")

		parentTokenValues := parentMDAfter.Get(strings.ToLower(util.HeaderToken))
		s.Equal([]string{"test-token"}, parentTokenValues, "parent context token should not be modified")
	})

	s.Run("context_without_metadata", func() {
		// Create a context without metadata
		parentCtx := context.Background()

		// Call getContextWithTimeout
		newCtx, cancel := getContextWithTimeout(parentCtx, 5*time.Second)
		defer cancel()

		// Verify that the new context is created successfully
		s.NotNil(newCtx)

		// Verify timeout is set
		deadline, ok := newCtx.Deadline()
		s.True(ok, "new context should have deadline")
		s.True(time.Until(deadline) <= 5*time.Second, "deadline should be within 5 seconds")
	})

	s.Run("context_with_only_auth_metadata", func() {
		// Create a context with only auth metadata
		parentMD := metadata.New(map[string]string{
			strings.ToLower(util.HeaderAuthorize): "test-auth-token",
			strings.ToLower(util.HeaderToken):     "test-token",
		})
		parentCtx := metadata.NewIncomingContext(context.Background(), parentMD)

		// Call getContextWithTimeout
		newCtx, cancel := getContextWithTimeout(parentCtx, 5*time.Second)
		defer cancel()

		// Verify that the new context's metadata is empty
		newMD, ok := metadata.FromIncomingContext(newCtx)
		s.True(ok, "new context should have metadata")
		s.Empty(newMD.Get(strings.ToLower(util.HeaderAuthorize)), "authorization should be removed")
		s.Empty(newMD.Get(strings.ToLower(util.HeaderToken)), "token should be removed")
	})

	s.Run("verify_timeout_works", func() {
		// Create a context with metadata
		parentMD := metadata.New(map[string]string{
			"test-key": "test-value",
		})
		parentCtx := metadata.NewIncomingContext(context.Background(), parentMD)

		// Call getContextWithTimeout with a very short timeout
		timeout := 100 * time.Millisecond
		newCtx, cancel := getContextWithTimeout(parentCtx, timeout)
		defer cancel()

		// Verify deadline is set correctly
		deadline, ok := newCtx.Deadline()
		s.True(ok, "context should have deadline")
		s.True(time.Until(deadline) <= timeout, "deadline should be within timeout duration")

		// Wait for timeout
		<-newCtx.Done()
		s.ErrorIs(newCtx.Err(), context.DeadlineExceeded, "context should be canceled due to timeout")
	})
}

func TestEtcdKVUtil(t *testing.T) {
	suite.Run(t, new(EtcdKVUtilSuite))
}

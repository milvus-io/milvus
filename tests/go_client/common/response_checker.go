package common

import (
	"strings"
	"testing"

	"github.com/milvus-io/milvus/pkg/log"
	"github.com/stretchr/testify/require"

	clientv2 "github.com/milvus-io/milvus/client/v2"
)

func CheckErr(t *testing.T, actualErr error, expErrNil bool, expErrorMsg ...string) {
	if expErrNil {
		require.NoError(t, actualErr)
	} else {
		require.Error(t, actualErr)
		switch len(expErrorMsg) {
		case 0:
			log.Fatal("expect error message should not be empty")
		case 1:
			require.ErrorContains(t, actualErr, expErrorMsg[0])
		default:
			contains := false
			for i := 0; i < len(expErrorMsg); i++ {
				if strings.Contains(actualErr.Error(), expErrorMsg[i]) {
					contains = true
				}
			}
			if !contains {
				t.FailNow()
			}
		}
	}
}

// CheckSearchResult check search result, check nq, topk, ids, score
func CheckSearchResult(t *testing.T, actualSearchResults []clientv2.ResultSet, expNq int, expTopK int) {
	require.Equal(t, len(actualSearchResults), expNq)
	require.Len(t, actualSearchResults, expNq)
	for _, actualSearchResult := range actualSearchResults {
		require.Equal(t, actualSearchResult.ResultCount, expTopK)
	}
}

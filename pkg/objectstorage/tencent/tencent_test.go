package tencent

import (
	"testing"

	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
	"github.com/stretchr/testify/assert"
)

func Test_NewMinioClient(t *testing.T) {
	t.Run("ak sk ok", func(t *testing.T) {
		minioCli, err := NewMinioClient("xxx.cos.ap-beijing.myqcloud.com", &minio.Options{
			Creds:  credentials.NewStaticV2("ak", "sk", ""),
			Secure: true,
		})
		assert.NoError(t, err)
		assert.Equal(t, "https", minioCli.EndpointURL().Scheme)
	})

	t.Run("iam failed", func(t *testing.T) {
		_, err := NewMinioClient("", nil)
		assert.Error(t, err)
	})
}

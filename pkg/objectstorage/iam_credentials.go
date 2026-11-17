package objectstorage

import (
	"net/http"
	"os"
	"time"

	"github.com/minio/minio-go/v7/pkg/credentials"

	"github.com/milvus-io/milvus/pkg/v3/objectstorage/credentialcache"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

func newIAMCredentials() *credentials.Credentials {
	iam := &credentials.IAM{Client: &http.Client{Transport: http.DefaultTransport}}
	// IAM.Retrieve consults these variables on each call. A failure must not
	// cause this client to adopt a different role or metadata source. Projected
	// token file contents and container authorization tokens may still rotate.
	selected := make(map[string]string)
	for _, key := range []string{
		"AWS_WEB_IDENTITY_TOKEN_FILE", "AWS_ROLE_ARN", "AWS_ROLE_SESSION_NAME", "AWS_REGION",
		"AWS_CONTAINER_CREDENTIALS_RELATIVE_URI", "AWS_CONTAINER_CREDENTIALS_FULL_URI", "AWS_CONTAINER_AUTHORIZATION_TOKEN_FILE",
	} {
		selected[key] = os.Getenv(key)
	}
	unchanged := func() bool {
		for key, value := range selected {
			if os.Getenv(key) != value {
				return false
			}
		}
		return true
	}
	fetch := func() (credentials.Value, error) {
		if !unchanged() {
			return credentials.Value{}, merr.WrapErrServiceUnavailableMsg("IAM credential source configuration changed; recreate the client to select a new identity")
		}
		candidate, err := iam.Retrieve()
		if !unchanged() {
			return credentials.Value{}, merr.WrapErrServiceUnavailableMsg("IAM credential source changed during refresh")
		}
		return candidate, err
	}
	return credentials.New(credentialcache.New(fetch, 3*time.Minute, true, false))
}

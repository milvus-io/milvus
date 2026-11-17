// Derived from github.com/aliyun/credentials-go v1.2.7 (Apache-2.0).
// Local maintenance and modifications are documented in UPSTREAM.md.

package credentials

import (
	"net/http"
	"sync"
	"time"
)

const defaultInAdvanceScale = 0.95

var hookDo = func(fn func(req *http.Request) (*http.Response, error)) func(req *http.Request) (*http.Response, error) {
	return fn
}

type credentialUpdater struct {
	mu                   sync.Mutex
	expiresAt            time.Time
	now                  func() time.Time
	credentialExpiration int
	lastUpdateTimestamp  int64
	inAdvanceScale       float64
}

func (updater *credentialUpdater) needUpdateCredential() (result bool) {
	if updater.inAdvanceScale == 0 {
		updater.inAdvanceScale = defaultInAdvanceScale
	}
	return updater.timeNow().Unix()-updater.lastUpdateTimestamp >= int64(float64(updater.credentialExpiration)*updater.inAdvanceScale)
}

func (updater *credentialUpdater) timeNow() time.Time {
	if updater.now != nil {
		return updater.now()
	}
	return time.Now()
}

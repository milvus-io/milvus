// Derived from github.com/aliyun/credentials-go v1.2.7 (Apache-2.0).
// Local maintenance and modifications are documented in UPSTREAM.md.

package request

import (
	"fmt"
	"net/url"
	"strings"
	"time"

	"github.com/milvus-io/milvus/pkg/v3/objectstorage/aliyun/internal/credentials/utils"
)

// CommonRequest is for requesting credential
type CommonRequest struct {
	Scheme         string
	Method         string
	Domain         string
	RegionId       string
	URL            string
	ReadTimeout    time.Duration
	ConnectTimeout time.Duration
	isInsecure     *bool
	BodyParams     map[string]string
	userAgent      map[string]string
	QueryParams    map[string]string
	Headers        map[string]string

	queries string
}

// NewCommonRequest returns a CommonRequest
func NewCommonRequest() *CommonRequest {
	return &CommonRequest{
		BodyParams:  make(map[string]string),
		QueryParams: make(map[string]string),
		Headers:     make(map[string]string),
	}
}

// BuildURL returns a url
func (request *CommonRequest) BuildURL() string {
	url := fmt.Sprintf("%s://%s", strings.ToLower(request.Scheme), request.Domain)
	request.queries = "/?" + utils.GetURLFormedMap(request.QueryParams)
	return url + request.queries
}

// BuildStringToSign returns BuildStringToSign
func (request *CommonRequest) BuildStringToSign() (stringToSign string) {
	signParams := make(map[string]string)
	for key, value := range request.QueryParams {
		signParams[key] = value
	}

	for key, value := range request.BodyParams {
		signParams[key] = value
	}
	stringToSign = utils.GetURLFormedMap(signParams)
	stringToSign = strings.Replace(stringToSign, "+", "%20", -1)
	stringToSign = strings.Replace(stringToSign, "*", "%2A", -1)
	stringToSign = strings.Replace(stringToSign, "%7E", "~", -1)
	stringToSign = url.QueryEscape(stringToSign)
	stringToSign = request.Method + "&%2F&" + stringToSign
	return
}

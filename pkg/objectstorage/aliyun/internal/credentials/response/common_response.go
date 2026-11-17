// Derived from github.com/aliyun/credentials-go v1.2.7 (Apache-2.0).
// Local maintenance and modifications are documented in UPSTREAM.md.

package response

import (
	"io"
	"io/ioutil"
	"net/http"
)

var hookReadAll = func(fn func(r io.Reader) (b []byte, err error)) func(r io.Reader) (b []byte, err error) {
	return fn
}

// CommonResponse is for storing message of httpResponse
type CommonResponse struct {
	httpStatus        int
	httpHeaders       map[string][]string
	httpContentString string
	httpContentBytes  []byte
}

// ParseFromHTTPResponse assigns for CommonResponse, returns err when body is too large.
func (resp *CommonResponse) ParseFromHTTPResponse(httpResponse *http.Response) (err error) {
	defer httpResponse.Body.Close()
	body, err := hookReadAll(ioutil.ReadAll)(httpResponse.Body)
	if err != nil {
		return
	}
	resp.httpStatus = httpResponse.StatusCode
	resp.httpHeaders = httpResponse.Header
	resp.httpContentBytes = body
	resp.httpContentString = string(body)
	return
}

// GetHTTPStatus returns httpStatus
func (resp *CommonResponse) GetHTTPStatus() int {
	return resp.httpStatus
}

// GetHTTPHeaders returns httpresponse's headers
func (resp *CommonResponse) GetHTTPHeaders() map[string][]string {
	return resp.httpHeaders
}

// GetHTTPContentString return body content as string
func (resp *CommonResponse) GetHTTPContentString() string {
	return resp.httpContentString
}

// GetHTTPContentBytes return body content as []byte
func (resp *CommonResponse) GetHTTPContentBytes() []byte {
	return resp.httpContentBytes
}

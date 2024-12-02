// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package httpserver

import (
	"io"
	"net/http"

	"github.com/cockroachdb/errors"
	"github.com/gin-gonic/gin"
	"github.com/gin-gonic/gin/binding"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
)

var errBadRequest = errors.New("bad request")

// handlerFunc handles http request with gin context
type handlerFunc func(c *gin.Context) (interface{}, error)

// ErrResponse of server
type ErrResponse = commonpb.Status

// wrapHandler wraps a handlerFunc into a gin.HandlerFunc
func wrapHandler(handle handlerFunc) gin.HandlerFunc {
	return func(c *gin.Context) {
		data, err := handle(c)
		// format body by accept header, protobuf marshal not supported by gin by default
		// TODO: add marshal handler to support protobuf response
		formatOffered := []string{binding.MIMEJSON, binding.MIMEYAML}
		bodyFormatNegotiate := gin.Negotiate{
			Offered: formatOffered,
			Data:    data,
		}
		if err != nil {
			switch {
			case errors.Is(err, errBadRequest):
				bodyFormatNegotiate.Data = ErrResponse{
					ErrorCode: commonpb.ErrorCode_IllegalArgument,
					Reason:    err.Error(),
				}
				c.Negotiate(http.StatusBadRequest, bodyFormatNegotiate)
				return
			default:
				bodyFormatNegotiate.Data = ErrResponse{
					ErrorCode: commonpb.ErrorCode_UnexpectedError,
					Reason:    err.Error(),
				}
				c.Negotiate(http.StatusInternalServerError, bodyFormatNegotiate)
				return
			}
		}
		c.Negotiate(http.StatusOK, bodyFormatNegotiate)
	}
}

// gin.ShouldBind() default as `form`, but we want JSON
func shouldBind(c *gin.Context, obj interface{}) error {
	b := getBinding(c.ContentType())
	err := c.ShouldBindWith(obj, b)
	if errors.Is(err, io.EOF) {
		return nil
	}
	return err
}

func getBinding(contentType string) binding.Binding {
	// ref: binding.Default
	switch contentType {
	case binding.MIMEYAML:
		return binding.YAML
	default:
		return binding.JSON
	}
}

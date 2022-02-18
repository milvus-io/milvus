package httpserver

import (
	"errors"
	"net/http"

	"github.com/gin-gonic/gin"
	"github.com/gin-gonic/gin/binding"
	"github.com/milvus-io/milvus/internal/proto/commonpb"
)

var (
	errBadRequest = errors.New("bad request")
)

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
		formatOffered := []string{binding.MIMEJSON, binding.MIMEYAML, binding.MIMEXML}
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

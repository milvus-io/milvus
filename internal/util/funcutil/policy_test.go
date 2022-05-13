package funcutil

import (
	"testing"

	"github.com/milvus-io/milvus/internal/proto/commonpb"
	"github.com/milvus-io/milvus/internal/proto/milvuspb"

	"github.com/stretchr/testify/assert"
)

func Test_GetVersion(t *testing.T) {
	request := &milvuspb.HasCollectionRequest{
		Base: &commonpb.MsgBase{
			MsgType: commonpb.MsgType_Undefined,
			MsgID:   123,
		},
		DbName:         "test",
		CollectionName: "col1",
	}
	str, err := GetVersion(request)
	assert.Nil(t, err)
	assert.Equal(t, str, "2.1.0")

	request2 := &commonpb.MsgBase{}
	_, err = GetVersion(request2)
	assert.NotNil(t, err)
}

func Test_GetPrivilegeExtObj(t *testing.T) {
	request := &milvuspb.HasCollectionRequest{
		DbName:         "test",
		CollectionName: "col1",
	}
	privilegeExt, err := GetPrivilegeExtObj(request)
	assert.Nil(t, err)
	assert.Equal(t, commonpb.ResourceType_Collection, privilegeExt.ResourceType)
	assert.Equal(t, commonpb.ResourcePrivilege_PrivilegeRead, privilegeExt.ResourcePrivilege)
	assert.Equal(t, int32(3), privilegeExt.ResourceNameIndex)

	request2 := &milvuspb.CreatePartitionRequest{}
	_, err = GetPrivilegeExtObj(request2)
	assert.NotNil(t, err)
}

func Test_GetResourceName(t *testing.T) {
	request := &milvuspb.HasCollectionRequest{
		DbName:         "test",
		CollectionName: "col1",
	}
	assert.Equal(t, "", GetResourceName(request, 0))
	assert.Equal(t, "col1", GetResourceName(request, 3))
}

func Test_PolicyForPrivilege(t *testing.T) {
	assert.Equal(t,
		`{"PType":"p","V0":"admin","V1":"COLLECTION-col1","V2":"ALL"}`,
		PolicyForPrivilege("admin", "COLLECTION", "col1", "ALL"))
}

func Test_PolicyForRole(t *testing.T) {
	assert.Equal(t,
		`{"PType":"g","V0":"root","V1":"admin"}`,
		PolicyForRole("root", "admin"))
}

func Test_PolicyForResource(t *testing.T) {
	assert.Equal(t,
		`COLLECTION-col1`,
		PolicyForResource("COLLECTION", "col1"))
}

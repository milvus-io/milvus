package funcutil

import (
	"testing"

	"github.com/milvus-io/milvus/api/commonpb"
	"github.com/milvus-io/milvus/api/milvuspb"

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
	request := &milvuspb.LoadCollectionRequest{
		DbName:         "test",
		CollectionName: "col1",
	}
	privilegeExt, err := GetPrivilegeExtObj(request)
	assert.Nil(t, err)
	assert.Equal(t, commonpb.ObjectType_Collection, privilegeExt.ObjectType)
	assert.Equal(t, commonpb.ObjectPrivilege_PrivilegeLoad, privilegeExt.ObjectPrivilege)
	assert.Equal(t, int32(3), privilegeExt.ObjectNameIndex)

	request2 := &milvuspb.CreatePartitionRequest{}
	_, err = GetPrivilegeExtObj(request2)
	assert.NotNil(t, err)
}

func Test_GetResourceName(t *testing.T) {
	{
		request := &milvuspb.HasCollectionRequest{
			DbName:         "test",
			CollectionName: "col1",
		}
		assert.Equal(t, "*", GetObjectName(request, 0))
		assert.Equal(t, "col1", GetObjectName(request, 3))
	}

	{
		request := &milvuspb.SelectUserRequest{
			User: &milvuspb.UserEntity{Name: "test"},
		}
		assert.Equal(t, "test", GetObjectName(request, 2))

		request = &milvuspb.SelectUserRequest{}
		assert.Equal(t, "*", GetObjectName(request, 2))
	}

}

func Test_GetResourceNames(t *testing.T) {
	request := &milvuspb.FlushRequest{
		DbName:          "test",
		CollectionNames: []string{"col1", "col2"},
	}
	assert.Equal(t, 0, len(GetObjectNames(request, 0)))
	assert.Equal(t, 0, len(GetObjectNames(request, 2)))
	names := GetObjectNames(request, 3)
	assert.Equal(t, 2, len(names))
}

func Test_PolicyForPrivilege(t *testing.T) {
	assert.Equal(t,
		`{"PType":"p","V0":"admin","V1":"COLLECTION-col1","V2":"ALL"}`,
		PolicyForPrivilege("admin", "COLLECTION", "col1", "ALL"))
}

func Test_PolicyForResource(t *testing.T) {
	assert.Equal(t,
		`COLLECTION-col1`,
		PolicyForResource("COLLECTION", "col1"))
}

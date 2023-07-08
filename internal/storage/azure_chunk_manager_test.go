package storage

import (
	"bytes"
	"context"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/suite"
	"net/http"
	"sort"
	"testing"
	"time"
)

type CalculatorTestSuite struct {
	suite.Suite
	cli        *AzureChunkManager
	normalData map[string][]byte // a-folder/a/c, a-folder/ac, a-folder/bc, a, b, c
}

var (
	defaultContainerName = "test-container"
	defaultTimeOut       = time.Second * 20
	// this is the default account name and key for the emulator
	// https://learn.microsoft.com/en-us/azure/storage/common/storage-use-azurite?toc=%2Fazure%2Fstorage%2Fblobs%2Ftoc.json&bc=%2Fazure%2Fstorage%2Fblobs%2Fbreadcrumb%2Ftoc.json&tabs=visual-studio#authorization-for-tools-and-sdks
	accountName = "devstoreaccount1"
	accountKey  = "Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw=="
	serverURL   = "http://localhost:10000/devstoreaccount1/"
)

//SetupSuite create client begins execution
func (suite *CalculatorTestSuite) SetupSuite() {
	// create container and add some blob
	cred, err := azblob.NewSharedKeyCredential(accountName, accountKey)
	if err != nil {
		suite.Failf("Failed to create credential", err.Error())
	}
	client, err := azblob.NewClientWithSharedKeyCredential(serverURL, cred, nil)
	if err != nil {
		suite.Failf("Failed to create client", err.Error())
	}
	suite.cli = newAzureChunkManager(client, defaultContainerName, "/")
	if len(suite.normalData) == 0 {
		suite.normalData = make(map[string][]byte)
		for _, v := range []string{"a-folder/ac", "a-folder/bc", "a", "b", "c"} {
			suite.normalData[v] = []byte(v + time.Now().String())
		}
	}
	// create container and add some blob
	ctx, _ := context.WithTimeout(context.Background(), defaultTimeOut)
	if _, err := client.CreateContainer(ctx, defaultContainerName, nil); err != nil {
		suite.Failf("Failed to create container", err.Error())
	}
}

//SetupTest write blob to container before each test
func (suite *CalculatorTestSuite) SetupTest() {
	ctx, cancel := context.WithTimeout(context.Background(), defaultTimeOut)
	defer cancel()
	for k, v := range suite.normalData {
		if _, err := suite.cli.cli.UploadBuffer(ctx, defaultContainerName, k, v, nil); err != nil {
			suite.Failf("Failed to upload blob", err.Error())
		}
	}
}

// this function executes after all tests executed
func (suite *CalculatorTestSuite) TearDownSuite() {
	//remove container and blob
	ctx, _ := context.WithTimeout(context.Background(), defaultTimeOut)
	if _, err := suite.cli.cli.DeleteContainer(ctx, defaultContainerName, nil); err != nil {
		suite.Failf("Failed to delete container", err.Error())
	}
}

func TestCalculatorTestSuite(t *testing.T) {
	suite.Run(t, new(CalculatorTestSuite))
}

func (suite *CalculatorTestSuite) Test_getProperties() {
	assert := assert.New(suite.T())

	ctx, cancel := context.WithTimeout(context.Background(), defaultTimeOut)
	defer cancel()
	suite.Run("exists blob", func() {
		for name := range suite.normalData {
			resp, err := suite.cli.getProperties(ctx, name)
			assert.Nil(err)
			if assert.NotNil(resp.ContentLength) {
				assert.Equal(int64(len(suite.normalData[name])), *resp.ContentLength) // size
			}
		}
	})
	suite.Run("not exists blob", func() {
		for name := range suite.normalData {
			name += "not-exist"
			resp, err := suite.cli.getProperties(ctx, name)
			assert.NotNil(err)
			assert.Nil(resp)
		}
	})
}

func (suite *CalculatorTestSuite) Test_ReadOperation() {
	assert := assert.New(suite.T())

	ctx, cancel := context.WithTimeout(context.Background(), defaultTimeOut)
	defer cancel()
	suite.Run("read exist blob", func() {
		for name, data := range suite.normalData {
			b, err := suite.cli.Read(ctx, name)
			assert.Nil(err)
			assert.Equal(data, b)
		}
	})
	suite.Run("read not exists blob", func() {
		for name := range suite.normalData {
			b, err := suite.cli.Read(ctx, name+"-not-exist")
			assert.NotNil(err)
			assert.Nil(b)
		}
	})

	suite.Run("read blob with range option", func() {
		type oneCase struct {
			name, blobName string
			off, len       int64
			want           []byte
			wantErr        bool
		}
		existBlobName, notExistBlobName := "a", "not-exist-blob"
		baseData := suite.normalData[existBlobName]
		dataLen := int64(len(baseData))
		cases := []oneCase{
			{"invalid param", existBlobName, -1, 0, nil, true},
			{"invalid param", existBlobName, -1, -1, nil, true},
			{"len = 0 mean read all data", existBlobName, 0, 0, baseData, false},
			{"read not exist blob", notExistBlobName, 0, 0, nil, true},
			{"read a part", existBlobName, 0, 1, baseData[:1], false},
			{"read all", existBlobName, 0, dataLen, baseData, false},
			{"length > blob.length should return all blob data", existBlobName, 0, dataLen + 2, baseData[0:], false}, // length > blob length
		}
		for _, c := range cases {
			b, err := suite.cli.ReadAt(ctx, c.blobName, c.off, c.len)
			if c.wantErr {
				assert.NotNil(err, c.name)
				assert.Nil(b, c.name)
			} else {
				assert.Nil(err, c.name)
				assert.ElementsMatch(c.want, b, c.name)
			}
		}
	})

	suite.Run("get exist blob reader", func() {
		for name, data := range suite.normalData {
			r, err := suite.cli.Reader(ctx, name)
			assert.Nil(err)
			defer r.Close()

			var buf bytes.Buffer
			if _, err := buf.ReadFrom(r); err != nil {
				assert.Fail("read error", err.Error())
			}
			assert.ElementsMatch(data, buf.Bytes())
		}
	})

	suite.Run("get not exist blob reader", func() {
		reader, err := suite.cli.Reader(ctx, "not-exist-blob")
		assert.NotNil(err)
		assert.Nil(reader)
	})
}

func (suite *CalculatorTestSuite) Test_OperationWithPrefix() {
	assert := assert.New(suite.T())
	ctx, cancel := context.WithTimeout(context.Background(), defaultTimeOut)
	defer cancel()
	type oneCase struct {
		prefix    string
		want      []string
		recursive bool
	}
	suite.Run("ListWithPrefix", func() {
		cases := []oneCase{
			{"a", []string{"a-folder/ac", "a-folder/bc", "a"}, true},
			{"a", []string{"a"}, false},
			{"b", []string{"b"}, true},
			{"b", []string{"b"}, false},
			{"c", []string{"c"}, true},
			{"c", []string{"c"}, false},
		}
		for _, c := range cases {
			resp, _, err := suite.cli.ListWithPrefix(ctx, c.prefix, c.recursive)
			assert.Nil(err)
			suite.stringSliceEqual(c.want, resp)
		}
	})

	suite.Run("ReadWithPrefix", func() {
		cases := []oneCase{
			{"a", []string{"a-folder/ac", "a-folder/bc", "a"}, true},
			{"b", []string{"b"}, true},
			{"c", []string{"c"}, true},
		}
		for _, c := range cases {
			blobNames, blobValues, err := suite.cli.ReadWithPrefix(ctx, c.prefix)
			assert.Nil(err)
			if suite.stringSliceEqual(c.want, blobNames) {
				for i, v := range blobValues {
					assert.ElementsMatch(suite.normalData[blobNames[i]], v)
				}
			}
		}
	})

	suite.Run("RemoveWithPrefix", func() {
		cases := []oneCase{
			{"a", []string{"a-folder/ac", "a-folder/bc", "a"}, true},
			{"b", []string{"b"}, true},
			{"c", []string{"c"}, true},
		}

		allBlobName := make(map[string]int)
		for k := range suite.normalData {
			allBlobName[k] = 1
		}

		for _, c := range cases {
			err := suite.cli.RemoveWithPrefix(ctx, c.prefix)
			assert.Nil(err)

			blobNames, _, err := suite.cli.ListWithPrefix(ctx, "", c.recursive) // list all
			assert.Nil(err)
			for _, name := range c.want { // check del want success
				assert.NotContains(blobNames, name)
				delete(allBlobName, name)
			}

			// check not del other
			var shouldExistBlobName []string
			for name := range allBlobName {
				shouldExistBlobName = append(shouldExistBlobName, name)
			}
			suite.stringSliceEqual(shouldExistBlobName, blobNames)
		}
	})

}

func (suite *CalculatorTestSuite) Test_Multi() {
	ctx, cancel := context.WithTimeout(context.Background(), defaultTimeOut)
	defer cancel()
	// 1. check MultiWrite data
	var keys []string
	assert := assert.New(suite.T())
	for name, data := range suite.normalData {
		suite.normalData[name] = []byte(string(data) + "-new") // write exist blob
		keys = append(keys, name)
	}
	suite.normalData["d"] = []byte("d" + "-new") // write new data
	keys = append(keys, "d")
	err := suite.cli.MultiWrite(ctx, suite.normalData)
	assert.Nil(err)

	// 2. check MultiRead data
	datas, err := suite.cli.MultiRead(ctx, keys)
	assert.Nil(err)
	for index, key := range keys {
		assert.ElementsMatch(suite.normalData[key], datas[index])
	}
	// 3. check MultiRemove data
	err = suite.cli.MultiRemove(ctx, keys)
	assert.Nil(err)

	// check remove successfully
	for _, key := range keys {
		b, err := suite.cli.Read(ctx, key)
		if assert.NotNil(err) {
			err, ok := err.(*azcore.ResponseError)
			if assert.True(ok) {
				assert.Equal(http.StatusNotFound, err.StatusCode)
			}
		}
		assert.Nil(b)
	}
}

func (suite *CalculatorTestSuite) stringSliceEqual(want, actual []string) bool {
	sort.Strings(want)
	sort.Strings(actual)
	return assert.ElementsMatch(suite.T(), want, actual)
}

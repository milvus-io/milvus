package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"testing"

	//"github.com/stretchr/testify/suite"
	"milvus_go_test/utils"

	"github.com/milvus-io/milvus-sdk-go/milvus"
)

var ip string
var port int64

// type _Suite struct {
// 	suite.Suite
// }

var Server ArgsServer

type ArgsServer struct {
	ip     string
	port   int64
	client milvus.MilvusClient
}

func init() {
	flag.StringVar(&ip, "ip", "127.0.0.1", "server host ip")
	flag.Int64Var(&port, "port", 19530, "server host port")
}

func GetClient() milvus.MilvusClient {
	var grpcClient milvus.Milvusclient
	client := milvus.NewMilvusClient(grpcClient.Instance)
	connectParam := milvus.ConnectParam{IPAddress: ip, Port: port}
	err := client.Connect(connectParam)
	if err != nil {
		fmt.Println("Connect failed")
		return nil
	}
	return client
}

func GenDisconnectClient() milvus.MilvusClient {
	client := GetClient()
	error := client.Disconnect()
	if error != nil && client.IsConnected() {
		fmt.Println("Disconnect failed")
		return nil
	}
	return client
}

func GenDefaultFields(fieldType milvus.DataType) []milvus.Field {
	var field milvus.Field
	fields := []milvus.Field{
		{
			utils.DefaultFieldFloatName,
			milvus.FLOAT,
			"",
			"",
		},
	}
	params := map[string]interface{}{
		"dim": utils.DefaultDimension,
	}
	paramsStr, _ := json.Marshal(params)
	if fieldType == milvus.VECTORFLOAT {
		field = milvus.Field{
			utils.DefaultFieldFloatVectorName,
			milvus.VECTORFLOAT,
			"",
			string(paramsStr),
		}
	} else {
		field = milvus.Field{
			utils.DefaultFieldBinaryVectorName,
			milvus.VECTORBINARY,
			"",
			string(paramsStr),
		}
	}
	return append(fields, field)
}

func GenDefaultFieldValues(fieldType milvus.DataType) []milvus.FieldValue {
	fieldValues := []milvus.FieldValue{
		{
			utils.DefaultFieldFloatName,
			utils.DefaultFloatValues,
		},
	}
	var fieldValue milvus.FieldValue
	if fieldType == milvus.VECTORFLOAT {
		fieldValue = milvus.FieldValue{
			utils.DefaultFieldFloatVectorName,
			utils.DefaultFloatVectors,
		}
	} else {
		fieldValue = milvus.FieldValue{
			utils.DefaultFieldBinaryVectorName,
			utils.DefaultBinaryVectors,
		}
	}
	return append(fieldValues, fieldValue)
}

func Collection(autoId bool, vectorType milvus.DataType) (milvus.MilvusClient, string) {
	client := GetClient()
	name := ""
	if client != nil {
		name = utils.RandString(8)
		fmt.Printf(name)
		params := map[string]interface{}{
			"auto_id":           autoId,
			"segment_row_count": utils.DefaultSegmentRowLimit,
		}
		paramsStr, _ := json.Marshal(params)
		mapping := milvus.Mapping{CollectionName: name, Fields: GenDefaultFields(vectorType), ExtraParams: string(paramsStr)}
		status, _ := client.CreateCollection(mapping)
		if !status.Ok() {
			fmt.Println("Create collection failed")
			os.Exit(-1)
		}
	} else {
		os.Exit(-2)
	}
	return client, name
}

func GenCollectionParams(name string, autoId bool, segmentRowLimit int) milvus.Mapping {
	//client := GetClient()
	var mapping milvus.Mapping
	//if client != nil {
	params := map[string]interface{}{
		"auto_id":           autoId,
		"segment_row_count": utils.DefaultSegmentRowLimit,
	}
	paramsStr, _ := json.Marshal(params)
	mapping = milvus.Mapping{CollectionName: name, Fields: GenDefaultFields(milvus.VECTORFLOAT), ExtraParams: string(paramsStr)}
	//} else {
	//	os.Exit(-2)
	//}
	return mapping
}

func teardown() {
	client := GetClient()
	listCollections, _, _ := client.ListCollections()
	println(len(listCollections))
	for i := 0; i < len(listCollections); i++ {
		client.DropCollection(listCollections[i])
	}
}

func TestMain(m *testing.M) {
	// ip, port = Args()
	flag.Parse()
	Server.ip = ip
	Server.port = port
	Server.client = GetClient()
	fmt.Println(Server.ip)
	code := m.Run()
	teardown()
	os.Exit(code)
	//suite.Run(t, new(_Suite))
}

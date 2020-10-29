package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"strconv"
	"testing"

	//"github.com/stretchr/testify/suite"
	"milvus_go_test/utils"

	"github.com/milvus-io/milvus-sdk-go/milvus"
)

var ip string
var port int
var fieldFloatName string = "float"
var fieldIntName string = "int64"
var fieldFloatVectorName string = "float_vector"
var fieldBinaryVectorName string = "binary_vector"
var autoId bool = false
var dimension int = 128
var segmentRowLimit int = 5000
var defaultNb = 6000
var defaultIntValues = utils.GenDefaultIntValues(defaultNb)
var defaultFloatValues = utils.GenDefaultFloatValues(defaultNb)
var defaultFloatVector = utils.GenFloatVectors(dimension, 1, false)
var defaultFloatVectors = utils.GenFloatVectors(dimension, defaultNb, false)

var defaultBinaryVector = utils.GenBinaryVectors(dimension, 1)
var defaultBinaryVectors = utils.GenBinaryVectors(dimension, defaultNb)

var l2Indexes = utils.GenIndexes(milvus.L2)
var ipIndexes = utils.GenIndexes(milvus.IP)

// type _Suite struct {
// 	suite.Suite
// }

var Server ArgsServer

type ArgsServer struct {
	ip     string
	port   int
	client milvus.MilvusClient
}

func init() {
	flag.StringVar(&ip, "ip", "127.0.0.1", "server host ip")
	flag.IntVar(&port, "port", 19530, "server host port")
}

func GetClient() milvus.MilvusClient {
	var grpcClient milvus.Milvusclient
	client := milvus.NewMilvusClient(grpcClient.Instance)
	connectParam := milvus.ConnectParam{ip, strconv.Itoa(port)}
	err := client.Connect(connectParam)
	if err != nil {
		fmt.Println("Connect failed")
		return nil
	}
	return client
}

func GenDefaultFields(fieldType milvus.DataType, dimension int) []milvus.Field {
	var field milvus.Field
	fields := []milvus.Field{
		{
			fieldFloatName,
			milvus.FLOAT,
			"",
			"",
		},
	}
	params := map[string]interface{}{
		"dim": dimension,
	}
	paramsStr, _ := json.Marshal(params)
	if fieldType == milvus.VECTORFLOAT {
		field = milvus.Field{
			fieldFloatVectorName,
			milvus.VECTORFLOAT,
			"",
			string(paramsStr),
		}
	} else {
		field = milvus.Field{
			fieldBinaryVectorName,
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
			fieldFloatName,
			defaultFloatValues,
		},
	}
	var fieldValue milvus.FieldValue
	if fieldType == milvus.VECTORFLOAT {
		fieldValue = milvus.FieldValue{
			fieldFloatVectorName,
			defaultFloatVectors,
		}
	} else {
		fieldValue = milvus.FieldValue{
			Name:    fieldBinaryVectorName,
			RawData: defaultBinaryVectors,
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
			"segment_row_limit": segmentRowLimit,
		}
		paramsStr, _ := json.Marshal(params)
		mapping := milvus.Mapping{CollectionName: name, Fields: GenDefaultFields(vectorType, dimension), ExtraParams: string(paramsStr)}
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

func GenCollectionParams(name string, autoId bool, segmentRowLimit int, dim int) (milvus.MilvusClient, milvus.Mapping) {
	client := GetClient()
	var mapping milvus.Mapping
	if client != nil {
		params := map[string]interface{}{
			"auto_id":           autoId,
			"segment_row_limit": segmentRowLimit,
		}
		paramsStr, _ := json.Marshal(params)
		mapping = milvus.Mapping{CollectionName: name, Fields: GenDefaultFields(milvus.VECTORFLOAT, dim), ExtraParams: string(paramsStr)}
	} else {
		os.Exit(-2)
	}
	return client, mapping
}

func teardown()  {
	client := GetClient()
	listCollections, _, _ := client.ListCollections()
	println(len(listCollections))
	for i :=0; i<len(listCollections); i++ {
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

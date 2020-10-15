package com1;

import com.alibaba.fastjson.JSONArray;
import io.milvus.client.*;
import com.alibaba.fastjson.JSONObject;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.RandomStringUtils;

import java.nio.ByteBuffer;
import java.util.*;
import java.util.stream.Collectors;
import com1.Constants;
import org.testng.Assert;

public class Utils {

    public static List<Float> normalize(List<Float> w2v){
        float squareSum = w2v.stream().map(x -> x * x).reduce((float) 0, Float::sum);
        final float norm = (float) Math.sqrt(squareSum);
        w2v = w2v.stream().map(x -> x / norm).collect(Collectors.toList());
        return w2v;
    }

    public static String genUniqueStr(String str_value){
        String prefix = "_"+RandomStringUtils.randomAlphabetic(10);
        String str = str_value == null || str_value.trim().isEmpty() ? "test" : str_value;
        return str.trim()+prefix;
    }

    public static List<List<Float>> genVectors(int vectorCount, int dimension, boolean norm) {
        Random random = new Random();
        List<List<Float>> vectors = new ArrayList<>();
        for (int i = 0; i < vectorCount; ++i) {
            List<Float> vector = new ArrayList<>();
            for (int j = 0; j < dimension; ++j) {
                vector.add(random.nextFloat());
            }
            if (norm) {
                vector = normalize(vector);
            }
            vectors.add(vector);
        }
        return vectors;
    }

    static List<ByteBuffer> genBinaryVectors(int vectorCount, int dimension) {
        Random random = new Random();
        List<ByteBuffer> vectors = new ArrayList<>(vectorCount);
        final int dimensionInByte = dimension / 8;
        for (int i = 0; i < vectorCount; ++i) {
            ByteBuffer byteBuffer = ByteBuffer.allocate(dimensionInByte);
            random.nextBytes(byteBuffer.array());
            vectors.add(byteBuffer);
        }
        return vectors;
    }

    private static List<Map<String, Object>> genBaseFieldsWithoutVector(){
        List<Map<String,Object>> fieldsList = new ArrayList<>();
        Map<String, Object> intFields = new HashMap<>();
        intFields.put(Constants.fieldNameKey,Constants.intFieldName);
        intFields.put("type",DataType.INT64);
        Map<String, Object> floatField = new HashMap<>();
        floatField.put(Constants.fieldNameKey,Constants.floatFieldName);
        floatField.put("type",DataType.FLOAT);
        fieldsList.add(intFields);
        fieldsList.add(floatField);
        return fieldsList;

    }
    
    public static List<Map<String, Object>> genDefaultFields(int dimension, boolean isBinary){
        List<Map<String, Object>> defaultFieldList = genBaseFieldsWithoutVector();
        Map<String, Object> vectorField = new HashMap<>();
        if (isBinary){
            vectorField.put(Constants.fieldNameKey, Constants.binaryVectorFieldName);
            vectorField.put("type",DataType.VECTOR_BINARY);
        }else {
            vectorField.put(Constants.fieldNameKey, Constants.floatVectorFieldName);
            vectorField.put("type",DataType.VECTOR_FLOAT);
        }
        JSONObject jsonObject = new JSONObject();
        jsonObject.put("dim", dimension);
        vectorField.put("params", jsonObject.toString());

        defaultFieldList.add(vectorField);
        return defaultFieldList;
    }

    public static Map<String, List> genDefaultEntities(int vectorCount, List<List<Float>> vectors){
//        Map<String,Object> fieldsMap = genDefaultFields(dimension, false);
        Map<String, List> fieldsMap =new HashMap<>();
        List<Long> intValues = new ArrayList<>(vectorCount);
        List<Float> floatValues = new ArrayList<>(vectorCount);
        for (int i = 0; i < vectorCount; ++i) {
            intValues.add((long) i);
            floatValues.add((float) i);
        }
        fieldsMap.put(Constants.intFieldName,intValues);
        fieldsMap.put(Constants.floatFieldName,floatValues);
        fieldsMap.put(Constants.floatVectorFieldName,vectors);
        return fieldsMap;
    }

    public static Map<String, List> genDefaultBinaryEntities(int vectorCount, List<ByteBuffer> vectorsBinary){
//        List<Map<String,Object>> binaryFieldsMap = genDefaultFields(dimension, true);
        Map<String, List> binaryFieldsMap =new HashMap<>();
        List<Long> intValues = new ArrayList<>(vectorCount);
        List<Float> floatValues = new ArrayList<>(vectorCount);
        for (int i = 0; i < vectorCount; ++i) {
            intValues.add((long) i);
            floatValues.add((float) i);
        }
        binaryFieldsMap.put(Constants.intFieldName,intValues);
        binaryFieldsMap.put(Constants.floatFieldName,floatValues);
        binaryFieldsMap.put(Constants.binaryVectorFieldName,vectorsBinary);
        return binaryFieldsMap;
    }

    public static String setIndexParam(String indexType, String metricType, int nlist) {
//        ("{\"index_type\": \"IVF_SQ8\", \"metric_type\": \"L2\", \"\"params\": {\"nlist\": 2048}}")
//        JSONObject indexParam = new JSONObject();
//        indexParam.put("nlist", nlist);
//        return JSONObject.toJSONString(indexParam);
        String indexParams = String.format("{\"index_type\": %s, \"metric_type\": %s, \"params\": {\"nlist\": %s}}", indexType, metricType, nlist);
        return indexParams;
    }

    static JSONObject genVectorParam(MetricType metricType, List<List<Float>> queryVectors, int topk, int nprobe) {
        JSONObject searchParam = new JSONObject();
        JSONObject fieldParam = new JSONObject();
        fieldParam.put("topk", topk);
        fieldParam.put("metric_type", metricType);
        fieldParam.put("query", queryVectors);
        fieldParam.put("type", Constants.vectorType);
        JSONObject tmpSearchParam = new JSONObject();
        tmpSearchParam.put("nprobe", nprobe);
        fieldParam.put("params", tmpSearchParam);
        JSONObject vectorParams = new JSONObject();
        vectorParams.put(Constants.floatVectorFieldName, fieldParam);
        searchParam.put("vector", vectorParams);
        return searchParam;
    }

    static JSONObject genBinaryVectorParam(MetricType metricType, List<ByteBuffer> queryVectors, int topk, int nprobe) {
        JSONObject searchParam = new JSONObject();
        JSONObject fieldParam = new JSONObject();
        fieldParam.put("topk", topk);
        fieldParam.put("metric_type", metricType);
        List<List<Byte>> vectorsToSearch = new ArrayList<>();
        for (ByteBuffer byteBuffer : queryVectors) {
            byte[] b = new byte[byteBuffer.remaining()];
            byteBuffer.get(b);
            vectorsToSearch.add(Arrays.asList(ArrayUtils.toObject(b)));
        }
        fieldParam.put("query", vectorsToSearch);
        fieldParam.put("type", Constants.binaryVectorType);
        JSONObject tmpSearchParam = new JSONObject();
        tmpSearchParam.put("nprobe", nprobe);
        fieldParam.put("params", tmpSearchParam);
        JSONObject vectorParams = new JSONObject();
        vectorParams.put(Constants.binaryVectorFieldName, fieldParam);
        searchParam.put("vector", vectorParams);
        return searchParam;
    }

    public static String setSearchParam(MetricType metricType, List<List<Float>> queryVectors, int topk, int nprobe) {
        JSONObject searchParam = genVectorParam(metricType, queryVectors, topk, nprobe);
        JSONObject boolParam = new JSONObject();
        JSONObject mustParam = new JSONObject();
        JSONArray tmp = new JSONArray();
        tmp.add(searchParam);
        mustParam.put("must", tmp);
        boolParam.put("bool", mustParam);
        return JSONObject.toJSONString(boolParam);
    }

    public static String setBinarySearchParam(MetricType metricType, List<ByteBuffer> queryVectors, int topk, int nprobe) {
        JSONObject searchParam = genBinaryVectorParam(metricType, queryVectors, topk, nprobe);
        JSONObject boolParam = new JSONObject();
        JSONObject mustParam = new JSONObject();
        JSONArray tmp = new JSONArray();
        tmp.add(searchParam);
        mustParam.put("must", tmp);
        boolParam.put("bool", mustParam);
        System.out.println(JSONObject.toJSONString(boolParam));
        return JSONObject.toJSONString(boolParam);
    }

    public static int getIndexParamValue(String indexParam, String key) {
        return JSONObject.parseObject(indexParam).getIntValue(key);
    }

    public static JSONObject getCollectionInfo(String collectionInfo) {
        return JSONObject.parseObject(collectionInfo);
    }

    public static List<Long> toListIds(int id) {
        List<Long> ids = new ArrayList<>();
        ids.add((long)id);
        return ids;
    }

    public static List<Long> toListIds(long id) {
        List<Long> ids = new ArrayList<>();
        ids.add(id);
        return ids;
    }

    public static int getParam(String params, String key){
        JSONObject jsonObject = JSONObject.parseObject(params);
        System.out.println(jsonObject.toString());
        Integer value = jsonObject.getInteger(key);
        return value;
    }

    public static List<Float> getVector(List<Map<String,Object>> entities, int i){
       List<Float> vector = new ArrayList<>();
        entities.forEach(entity -> {
            if(Constants.floatVectorFieldName.equals(entity.get("field")) && Objects.nonNull(entity.get("values"))){
                vector.add(((List<Float>)entity.get("values")).get(i));
            }
        });
        return vector;
    }

    public static JSONArray parseJsonArray(String message, String type) {
        JSONObject jsonObject = JSONObject.parseObject(message);
        JSONArray partitionsJsonArray = jsonObject.getJSONArray("partitions");
        if ("partitions".equals(type))
            return partitionsJsonArray;
        JSONArray segmentsJsonArray = ((JSONObject)partitionsJsonArray.get(0)).getJSONArray("segments");
        if ("segments".equals(type))
            return segmentsJsonArray;
        JSONArray filesJsonArray = ((JSONObject)segmentsJsonArray.get(0)).getJSONArray("files");
        if ("files".equals(type))
            return filesJsonArray;
        throw  new RuntimeException("unsupported type");
    }

    public static InsertParam genInsertParam(String collectionName) {
        Map<String, List> entities = Constants.defaultEntities;
        InsertParam insertParam = InsertParam
                .create(collectionName)
                .addField(Constants.intFieldName, DataType.INT64, entities.get(Constants.intFieldName))
                .addField(Constants.floatFieldName, DataType.FLOAT, entities.get(Constants.floatFieldName))
                .addVectorField(Constants.floatVectorFieldName, DataType.VECTOR_FLOAT, entities.get(Constants.floatVectorFieldName));
        return insertParam;
    }

    public static InsertParam genBinaryInsertParam(String collectionName) {
        List<Long> intValues = new ArrayList<>(Constants.nb);
        List<Float> floatValues = new ArrayList<>(Constants.nb);
        for (int i = 0; i < Constants.nb; ++i) {
            intValues.add((long) i);
            floatValues.add((float) i);
        }
        InsertParam insertParam = InsertParam
                .create(collectionName)
                .addField(Constants.intFieldName, DataType.INT64, intValues)
                .addField(Constants.floatFieldName, DataType.FLOAT, floatValues)
                .addVectorField(Constants.binaryVectorFieldName, DataType.VECTOR_BINARY, Constants.vectorsBinary);
        return insertParam;
    }

    public static CollectionMapping genCreateCollectionMapping(String collectionName, Boolean autoId, Boolean isBinary) {
        CollectionMapping cm = CollectionMapping.create(collectionName)
                .addField(Constants.intFieldName, DataType.INT64)
                .addField(Constants.floatFieldName, DataType.FLOAT)
                .setParamsInJson(new JsonBuilder()
                        .param("segment_row_limit", Constants.segmentRowLimit)
                        .param("auto_id", autoId)
                        .build());
        if (isBinary) {
            cm.addVectorField(Constants.binaryVectorFieldName, DataType.VECTOR_BINARY, Constants.dimension);
        } else {
            cm.addVectorField(Constants.floatVectorFieldName, DataType.VECTOR_FLOAT, Constants.dimension);
        }
        return cm;
    }

    public static List<Long> initData(MilvusClient client, String collectionName) {
        InsertParam insertParam = Utils.genInsertParam(collectionName);
        List<Long> ids = client.insert(insertParam);
        client.flush(collectionName);
        Assert.assertEquals(client.countEntities(collectionName), Constants.nb);
        return ids;
    }

    public static List<Long> initBinaryData(MilvusClient client, String collectionName) {
        InsertParam insertParam = Utils.genBinaryInsertParam(collectionName);
        List<Long> ids = client.insert(insertParam);
        client.flush(collectionName);
        Assert.assertEquals(client.countEntities(collectionName), Constants.nb);
        return ids;
    }

    ////////////////////////////////////////////////////////////////////////

//    public static CollectionMapping genDefaultCollectionMapping(String collectionName, int dimension,
//                                                                int segmentRowCount, boolean isBinary) {
//        Map<String, Object> vectorFieldMap;
//        if (isBinary) {
//            vectorFieldMap = new FieldBuilder("binary_vector", DataType.VECTOR_BINARY)
//                                .param("dim", dimension)
//                                .build();
//        } else {
//            vectorFieldMap = new FieldBuilder("float_vector", DataType.VECTOR_FLOAT)
//                                .param("dim", dimension)
//                                .build();
//        }
//
//        return new CollectionMapping.Builder(collectionName)
//            .field(new FieldBuilder("int64", DataType.INT64).build())
//            .field(new FieldBuilder("float", DataType.FLOAT).build())
//            .field(vectorFieldMap)
//            .withParamsInJson(new JsonBuilder()
//                    .param("segment_row_count", segmentRowCount)
//                    .build())
//            .build();
//    }
//
//    public static InsertParam genDefaultInsertParam(String collectionName, int dimension, int vectorCount,
//                                                    List<List<Float>> vectors) {
//        List<Long> intValues = new ArrayList<>(vectorCount);
//        List<Float> floatValues = new ArrayList<>(vectorCount);
//        for (int i = 0; i < vectorCount; ++i) {
//            intValues.add((long) i);
//            floatValues.add((float) i);
//        }
//
//        return new InsertParam.Builder(collectionName)
//                .field(new FieldBuilder("int64", DataType.INT64)
//                        .values(intValues)
//                        .build())
//                .field(new FieldBuilder("float", DataType.FLOAT)
//                        .values(floatValues)
//                        .build())
//                .field(new FieldBuilder("float_vector", DataType.VECTOR_FLOAT)
//                        .values(vectors)
//                        .param("dim", dimension)
//                        .build())
//                .build();
//    }
//
//    public static InsertParam genDefaultInsertParam(String collectionName, int dimension, int vectorCount,
//                                                    List<List<Float>> vectors, List<Long> entityIds) {
//        List<Long> intValues = new ArrayList<>(vectorCount);
//        List<Float> floatValues = new ArrayList<>(vectorCount);
//        for (int i = 0; i < vectorCount; ++i) {
//            intValues.add((long) i);
//            floatValues.add((float) i);
//        }
//
//        return new InsertParam.Builder(collectionName)
//                .field(new FieldBuilder("int64", DataType.INT64)
//                        .values(intValues)
//                        .build())
//                .field(new FieldBuilder("float", DataType.FLOAT)
//                        .values(floatValues)
//                        .build())
//                .field(new FieldBuilder("float_vector", DataType.VECTOR_FLOAT)
//                        .values(vectors)
//                        .param("dim", dimension)
//                        .build())
//                .withEntityIds(entityIds)
//                .build();
//    }
//
//    public static InsertParam genDefaultInsertParam(String collectionName, int dimension, int vectorCount,
//                                                    List<List<Float>> vectors, String tag) {
//        List<Long> intValues = new ArrayList<>(vectorCount);
//        List<Float> floatValues = new ArrayList<>(vectorCount);
//        for (int i = 0; i < vectorCount; ++i) {
//            intValues.add((long) i);
//            floatValues.add((float) i);
//        }
//
//        return new InsertParam.Builder(collectionName)
//                .field(new FieldBuilder("int64", DataType.INT64)
//                        .values(intValues)
//                        .build())
//                .field(new FieldBuilder("float", DataType.FLOAT)
//                        .values(floatValues)
//                        .build())
//                .field(new FieldBuilder("float_vector", DataType.VECTOR_FLOAT)
//                        .values(vectors)
//                        .param("dim", dimension)
//                        .build())
//                .withPartitionTag(tag)
//                .build();
//    }
//
//    public static InsertParam genDefaultBinaryInsertParam(String collectionName, int dimension, int vectorCount,
//                                                          List<List<Byte>> vectorsBinary) {
//        List<Long> intValues = new ArrayList<>(vectorCount);
//        List<Float> floatValues = new ArrayList<>(vectorCount);
//        for (int i = 0; i < vectorCount; ++i) {
//            intValues.add((long) i);
//            floatValues.add((float) i);
//        }
//
//        return new InsertParam.Builder(collectionName)
//                .field(new FieldBuilder("int64", DataType.INT64)
//                        .values(intValues)
//                        .build())
//                .field(new FieldBuilder("float", DataType.FLOAT)
//                        .values(floatValues)
//                        .build())
//                .field(new FieldBuilder("binary_vector", DataType.VECTOR_BINARY)
//                        .values(vectorsBinary)
//                        .param("dim", dimension)
//                        .build())
//                .build();
//    }
//
//    public static InsertParam genDefaultBinaryInsertParam(String collectionName, int dimension, int vectorCount,
//                                                          List<List<Byte>> vectorsBinary, List<Long> entityIds) {
//        List<Long> intValues = new ArrayList<>(vectorCount);
//        List<Float> floatValues = new ArrayList<>(vectorCount);
//        for (int i = 0; i < vectorCount; ++i) {
//            intValues.add((long) i);
//            floatValues.add((float) i);
//        }
//
//        return new InsertParam.Builder(collectionName)
//                .field(new FieldBuilder("int64", DataType.INT64)
//                        .values(intValues)
//                        .build())
//                .field(new FieldBuilder("float", DataType.FLOAT)
//                        .values(floatValues)
//                        .build())
//                .field(new FieldBuilder("binary_vector", DataType.VECTOR_BINARY)
//                        .values(vectorsBinary)
//                        .param("dim", dimension)
//                        .build())
//                .withEntityIds(entityIds)
//                .build();
//    }
//
//    public static InsertParam genDefaultBinaryInsertParam(String collectionName, int dimension, int vectorCount,
//                                                          List<List<Byte>> vectorsBinary, String tag) {
//        List<Long> intValues = new ArrayList<>(vectorCount);
//        List<Float> floatValues = new ArrayList<>(vectorCount);
//        for (int i = 0; i < vectorCount; ++i) {
//            intValues.add((long) i);
//            floatValues.add((float) i);
//        }
//
//        return new InsertParam.Builder(collectionName)
//                .field(new FieldBuilder("int64", DataType.INT64)
//                        .values(intValues)
//                        .build())
//                .field(new FieldBuilder("float", DataType.FLOAT)
//                        .values(floatValues)
//                        .build())
//                .field(new FieldBuilder("binary_vector", DataType.VECTOR_BINARY)
//                        .values(vectorsBinary)
//                        .param("dim", dimension)
//                        .build())
//                .withPartitionTag(tag)
//                .build();
//    }
//
//    public static Index genDefaultIndex(String collectionName, String fieldName, String indexType, String metricType, int nlist) {
//        return new Index.Builder(collectionName, fieldName)
//                .withParamsInJson(new JsonBuilder()
//                        .param("index_type", indexType)
//                        .param("metric_type", metricType)
//                        .indexParam("nlist", nlist)
//                        .build())
//                .build();
//    }
}

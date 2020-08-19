package com;

import io.milvus.client.*;
import com.alibaba.fastjson.JSONObject;
import jdk.nashorn.internal.runtime.regexp.joni.constants.OPCode;
import org.apache.commons.lang3.RandomStringUtils;

import java.nio.ByteBuffer;
import java.util.*;
import java.util.stream.Collectors;

public class Utils {

    public static List<Float> normalize(List<Float> w2v){
        float squareSum = w2v.stream().map(x -> x * x).reduce((float) 0, Float::sum);
        final float norm = (float) Math.sqrt(squareSum);
        w2v = w2v.stream().map(x -> x / norm).collect(Collectors.toList());
        return w2v;
    }
    public static String gen_unique_str(String str_value){
        String prefix = "_"+RandomStringUtils.randomAlphabetic(10);
        String str = str_value == null || str_value.trim().isEmpty() ? "test" : str_value;
        return str.trim()+prefix;
    }
    public static List<List<Float>> genVectors(int nb, int dimension, boolean norm) {
        List<List<Float>> xb = new ArrayList<>();
        Random random = new Random();
        for (int i = 0; i < nb; ++i) {
            List<Float> vector = new ArrayList<>();
            for (int j = 0; j < dimension; j++) {
                vector.add(random.nextFloat());
            }
            if (norm == true) {
                vector = normalize(vector);
            }
            xb.add(vector);
        }
        return xb;
    }

    static List<ByteBuffer> genBinaryVectors(long vectorCount, long dimension) {
        Random random = new Random();
        List<ByteBuffer> vectors = new ArrayList<>();
        final long dimensionInByte = dimension / 8;
        for (long i = 0; i < vectorCount; ++i) {
            ByteBuffer byteBuffer = ByteBuffer.allocate((int) dimensionInByte);
            random.nextBytes(byteBuffer.array());
            vectors.add(byteBuffer);
        }
        return vectors;
    }
    private static List<Map<String, Object>> genBaseFieldsWithoutVector(){
        List<Map<String,Object>> fieldsList = new ArrayList<>();
        Map<String, Object> intFields = new HashMap<>();
        intFields.put("field","int64");
        intFields.put("type",DataType.INT64);
        Map<String, Object> floatField = new HashMap<>();
        floatField.put("field","float");
        floatField.put("type",DataType.FLOAT);
        fieldsList.add(intFields);
        fieldsList.add(floatField);
        return fieldsList;

    }
    public static List<Map<String, Object>> genDefaultFields(int dimension, boolean isBinary){
        List<Map<String, Object>> defaultFieldList = genBaseFieldsWithoutVector();
        Map<String, Object> vectorField = new HashMap<>();
        if (isBinary){
            vectorField.put("field","binary_vector");
            vectorField.put("type",DataType.VECTOR_BINARY);
        }else {
            vectorField.put("field","float_vector");
            vectorField.put("type",DataType.VECTOR_FLOAT);
        }
        JSONObject jsonObject = new JSONObject();
        jsonObject.put("dim", dimension);
        vectorField.put("params", jsonObject.toString());

        defaultFieldList.add(vectorField);
        return defaultFieldList;
    }
    public static String setIndexParam(int nlist) {
        JSONObject indexParam = new JSONObject();
        indexParam.put("nlist", nlist);
        return JSONObject.toJSONString(indexParam);
    }

    public static String setSearchParam(int nprobe) {
        JSONObject searchParam = new JSONObject();
        searchParam.put("nprobe", nprobe);
        return JSONObject.toJSONString(searchParam);
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
}

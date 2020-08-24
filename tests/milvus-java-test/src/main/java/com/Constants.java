package com;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;

public final class Constants {

    public static final int dimension = 128;

    public static final int n_list = 128;

    public static final int n_probe = 64;

    public static final int nq = 5;

    public static final int topk = 10;

    public static final int nb = 8000;

    public static final double epsilon = 0.001;

    public static final String vectorType = "float";

    public static final String defaultMetricType = "L2";

    public static final String indexType = "IVF_SQ8";

    public static final String defaultIndexType = "FLAT";

    public static final String defaultBinaryIndexType = "BIN_FLAT";

    public static final String defaultBinaryMetricType = "JACCARD";

    public static final String floatFieldName = "float_vector";

    public static final String binaryFieldName = "binary_vector";

    public static final String indexParam = Utils.setIndexParam(indexType, "L2", n_list);

    public static final String binaryIndexParam = Utils.setIndexParam(defaultBinaryIndexType, defaultBinaryMetricType, n_list);

    public static final List<List<Float>> vectors = Utils.genVectors(nb, dimension, true);

    public static final List<ByteBuffer> vectorsBinary = Utils.genBinaryVectors(nb, dimension);

    public static final List<Map<String,Object>> defaultFields = Utils.genDefaultFields(dimension,false);

    public static final List<Map<String,Object>> defaultBinaryFields = Utils.genDefaultFields(dimension,true);

    public static final List<Map<String,Object>> defaultEntities = Utils.genDefaultEntities(dimension, nb, vectors);

    public static final List<Map<String,Object>> defaultBinaryEntities = Utils.genDefaultBinaryEntities(dimension, nb, vectorsBinary);

    public static final String searchParam = Utils.setSearchParam(defaultMetricType, vectors.subList(0, nq), topk, n_probe);

    public static final String binarySearchParam = Utils.setBinarySearchParam(defaultBinaryMetricType, vectorsBinary.subList(0, nq), topk, n_probe);

}

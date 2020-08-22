package com;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;

public final class Constants {

    public static final int dimension = 128;

    public static final int n_list = 128;

    public static final int nb = 8000;

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

    public static final List<Map<String,Object>> defaultEntities = Utils.genDefaultEntities(dimension, nb, vectors);

    public static final List<Map<String,Object>> defaultBinaryEntities = Utils.genDefaultBinaryEntities(dimension, nb, vectorsBinary);
}

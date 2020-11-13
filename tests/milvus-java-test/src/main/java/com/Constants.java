package com;

import io.milvus.client.ConnectParam;
import io.milvus.client.IndexType;
import io.milvus.client.MetricType;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;

public interface Constants {

    int dimension = 128;

    int n_list = 128;

    int n_probe = 64;

    int nq = 5;

    int topk = 10;

    int nb = 8000;

    double epsilon = 0.001;

    int segmentRowLimit = 5000;

    String fieldNameKey = "name";

    String vectorType = "float";

    String binaryVectorType = "binary";

    MetricType defaultMetricType = MetricType.L2;

    IndexType indexType = IndexType.IVF_SQ8;

    IndexType defaultIndexType = IndexType.FLAT;

    IndexType defaultBinaryIndexType = IndexType.BIN_IVF_FLAT;

    MetricType defaultBinaryMetricType = MetricType.JACCARD;

    String intFieldName = "int64";

    String floatFieldName = "float";

    String floatVectorFieldName = "float_vector";

    String binaryVectorFieldName = "binary_vector";

    List<List<Float>> vectors = Utils.genVectors(nb, dimension, true);

    List<ByteBuffer> vectorsBinary = Utils.genBinaryVectors(nb, dimension);

    Map<String, List> defaultEntities = Utils.genDefaultEntities(nb, vectors);

    Map<String, List> defaultBinaryEntities =
            Utils.genDefaultBinaryEntities(nb, vectorsBinary);

    String searchParam =
            Utils.setSearchParam(defaultMetricType, vectors.subList(0, nq), topk, n_probe);

    String binarySearchParam =
            Utils.setBinarySearchParam(
                    defaultBinaryMetricType, vectorsBinary.subList(0, nq), topk, n_probe);

    ConnectParam connectParam = MainClass.getConnectParam();
}

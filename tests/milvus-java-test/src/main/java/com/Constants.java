package com;

import io.milvus.client.IndexType;
import io.milvus.client.MetricType;
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

  public static final int segmentRowLimit = 5000;

  public static final String fieldNameKey = "name";

  public static final String vectorType = "float";

  public static final String binaryVectorType = "binary";

  public static final MetricType defaultMetricType = MetricType.L2;

  public static final IndexType indexType = IndexType.IVF_SQ8;

  public static final IndexType defaultIndexType = IndexType.FLAT;

  public static final IndexType defaultBinaryIndexType = IndexType.BIN_IVF_FLAT;

  public static final MetricType defaultBinaryMetricType = MetricType.JACCARD;

  public static final String intFieldName = "int64";

  public static final String floatFieldName = "float";

  public static final String floatVectorFieldName = "float_vector";

  public static final String binaryVectorFieldName = "binary_vector";

  public static final List<List<Float>> vectors = Utils.genVectors(nb, dimension, true);

  public static final List<ByteBuffer> vectorsBinary = Utils.genBinaryVectors(nb, dimension);

  public static final Map<String, List> defaultEntities = Utils.genDefaultEntities(nb, vectors);

  public static final Map<String, List> defaultBinaryEntities =
      Utils.genDefaultBinaryEntities(nb, vectorsBinary);

  public static final String searchParam =
      Utils.setSearchParam(defaultMetricType, vectors.subList(0, nq), topk, n_probe);

  public static final String binarySearchParam =
      Utils.setBinarySearchParam(
          defaultBinaryMetricType, vectorsBinary.subList(0, nq), topk, n_probe);
}

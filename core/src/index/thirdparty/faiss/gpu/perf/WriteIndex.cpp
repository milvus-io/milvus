/**
 * Copyright (c) Facebook, Inc. and its affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */


#include <faiss/IndexIVFFlat.h>
#include <faiss/IndexIVFPQ.h>
#include <faiss/IndexFlat.h>
#include <faiss/index_io.h>
#include <faiss/gpu/test/TestUtils.h>
#include <vector>
#include <gflags/gflags.h>

// For IVFPQ:
DEFINE_bool(ivfpq, false, "use IVFPQ encoding");
DEFINE_int32(codes, 4, "number of PQ codes per vector");
DEFINE_int32(bits_per_code, 8, "number of bits per PQ code");

// For IVFFlat:
DEFINE_bool(l2, true, "use L2 metric (versus IP metric)");
DEFINE_bool(ivfflat, false, "use IVF flat encoding");

// For both:
DEFINE_string(out, "/home/jhj/local/index.out", "index file for output");
DEFINE_int32(dim, 128, "vector dimension");
DEFINE_int32(num_coarse, 100, "number of coarse centroids");
DEFINE_int32(num, 100000, "total database size");
DEFINE_int32(num_train, -1, "number of database vecs to train on");

template <typename T>
void fillAndSave(T& index, int numTrain, int num, int dim) {
  auto trainVecs = faiss::gpu::randVecs(numTrain, dim);
  index.train(numTrain, trainVecs.data());

  constexpr int kAddChunk = 1000000;

  for (int i = 0; i < num; i += kAddChunk) {
    int numRemaining = (num - i) < kAddChunk ? (num - i) : kAddChunk;
    auto vecs = faiss::gpu::randVecs(numRemaining, dim);

    printf("adding at %d: %d\n", i, numRemaining);
    index.add(numRemaining, vecs.data());
  }

  faiss::write_index(&index, FLAGS_out.c_str());
}

int main(int argc, char** argv) {
  gflags::ParseCommandLineFlags(&argc, &argv, true);

  // Either ivfpq or ivfflat must be set
  if ((FLAGS_ivfpq && FLAGS_ivfflat) ||
      (!FLAGS_ivfpq && !FLAGS_ivfflat)) {
    printf("must specify either ivfpq or ivfflat\n");
    return 1;
  }

  auto dim = FLAGS_dim;
  auto numCentroids = FLAGS_num_coarse;
  auto num = FLAGS_num;
  auto numTrain = FLAGS_num_train;
  numTrain = numTrain == -1 ? std::max((num / 4), 1) : numTrain;
  numTrain = std::min(num, numTrain);

  if (FLAGS_ivfpq) {
    faiss::IndexFlatL2 quantizer(dim);
    faiss::IndexIVFPQ index(&quantizer, dim, numCentroids,
                            FLAGS_codes, FLAGS_bits_per_code);
    index.verbose = true;

    printf("IVFPQ: codes %d bits per code %d\n",
           FLAGS_codes, FLAGS_bits_per_code);
    printf("Lists: %d\n", numCentroids);
    printf("Database: dim %d num vecs %d trained on %d\n", dim, num, numTrain);
    printf("output file: %s\n", FLAGS_out.c_str());

    fillAndSave(index, numTrain, num, dim);
  } else if (FLAGS_ivfflat) {
    faiss::IndexFlatL2 quantizerL2(dim);
    faiss::IndexFlatIP quantizerIP(dim);

    faiss::IndexFlat* quantizer = FLAGS_l2 ?
      (faiss::IndexFlat*) &quantizerL2 :
      (faiss::IndexFlat*) &quantizerIP;

    faiss::IndexIVFFlat index(quantizer, dim, numCentroids,
                              FLAGS_l2 ? faiss::METRIC_L2 :
                              faiss::METRIC_INNER_PRODUCT);

    printf("IVFFlat: metric %s\n", FLAGS_l2 ? "L2" : "IP");
    printf("Lists: %d\n", numCentroids);
    printf("Database: dim %d num vecs %d trained on %d\n", dim, num, numTrain);
    printf("output file: %s\n", FLAGS_out.c_str());

    fillAndSave(index, numTrain, num, dim);
  }

  return 0;
}

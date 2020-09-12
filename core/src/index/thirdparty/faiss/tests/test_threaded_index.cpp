/**
 * Copyright (c) Facebook, Inc. and its affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include <faiss/impl/ThreadedIndex.h>
#include <faiss/IndexReplicas.h>
#include <faiss/IndexShards.h>

#include <chrono>
#include <gtest/gtest.h>
#include <memory>
#include <vector>
#include <thread>

namespace {

struct TestException : public std::exception { };

struct MockIndex : public faiss::Index {
  explicit MockIndex(idx_t d) :
      faiss::Index(d) {
    resetMock();
  }

  void resetMock() {
    flag = false;
    nCalled = 0;
    xCalled = nullptr;
    kCalled = 0;
    distancesCalled = nullptr;
    labelsCalled = nullptr;
  }

  void add(idx_t n, const float* x) override {
    nCalled = n;
    xCalled = x;
  }

  void search(idx_t n,
              const float* x,
              idx_t k,
              float* distances,
              idx_t* labels) const override {
    nCalled = n;
    xCalled = x;
    kCalled = k;
    distancesCalled = distances;
    labelsCalled = labels;
  }

  void reset() override { }

  bool flag;

  mutable idx_t nCalled;
  mutable const float* xCalled;
  mutable idx_t kCalled;
  mutable float* distancesCalled;
  mutable idx_t* labelsCalled;
};

template <typename IndexT>
struct MockThreadedIndex : public faiss::ThreadedIndex<IndexT> {
  using idx_t = faiss::Index::idx_t;

  explicit MockThreadedIndex(bool threaded)
      : faiss::ThreadedIndex<IndexT>(threaded) {
  }

  void add(idx_t, const float*) override { }
  void search(idx_t, const float*, idx_t, float*, idx_t*) const override {}
  void reset() override {}
};

}

TEST(ThreadedIndex, SingleException) {
  std::vector<std::unique_ptr<MockIndex>> idxs;

  for (int i = 0; i < 3; ++i) {
    idxs.emplace_back(new MockIndex(1));
  }

  auto fn =
    [](int i, MockIndex* index) {
      if (i == 1) {
        throw TestException();
      } else {
        std::this_thread::sleep_for(std::chrono::milliseconds(i * 250));

        index->flag = true;
      }
    };

  // Try with threading and without
  for (bool threaded : {true, false}) {
    // clear flags
    for (auto& idx : idxs) {
      idx->resetMock();
    }

    MockThreadedIndex<MockIndex> ti(threaded);
    for (auto& idx : idxs) {
      ti.addIndex(idx.get());
    }

    // The second index should throw
    EXPECT_THROW(ti.runOnIndex(fn), TestException);

    // Index 0 and 2 should have processed
    EXPECT_TRUE(idxs[0]->flag);
    EXPECT_TRUE(idxs[2]->flag);
  }
}

TEST(ThreadedIndex, MultipleException) {
  std::vector<std::unique_ptr<MockIndex>> idxs;

  for (int i = 0; i < 3; ++i) {
    idxs.emplace_back(new MockIndex(1));
  }

  auto fn =
    [](int i, MockIndex* index) {
      if (i < 2) {
        throw TestException();
      } else {
        std::this_thread::sleep_for(std::chrono::milliseconds(i * 250));

        index->flag = true;
      }
    };

  // Try with threading and without
  for (bool threaded : {true, false}) {
    // clear flags
    for (auto& idx : idxs) {
      idx->resetMock();
    }

    MockThreadedIndex<MockIndex> ti(threaded);
    for (auto& idx : idxs) {
      ti.addIndex(idx.get());
    }

    // Multiple indices threw an exception that was aggregated into a
    // FaissException
    EXPECT_THROW(ti.runOnIndex(fn), faiss::FaissException);

    // Index 2 should have processed
    EXPECT_TRUE(idxs[2]->flag);
  }
}

TEST(ThreadedIndex, TestReplica) {
  int numReplicas = 5;
  int n = 10 * numReplicas;
  int d = 3;
  int k = 6;

  // Try with threading and without
  for (bool threaded : {true, false}) {
    std::vector<std::unique_ptr<MockIndex>> idxs;
    faiss::IndexReplicas replica(d);

    for (int i = 0; i < numReplicas; ++i) {
      idxs.emplace_back(new MockIndex(d));
      replica.addIndex(idxs.back().get());
    }

    std::vector<float> x(n * d);
    std::vector<float> distances(n * k);
    std::vector<faiss::Index::idx_t> labels(n * k);

    replica.add(n, x.data());

    for (int i = 0; i < idxs.size(); ++i) {
      EXPECT_EQ(idxs[i]->nCalled, n);
      EXPECT_EQ(idxs[i]->xCalled, x.data());
    }

    for (auto& idx : idxs) {
      idx->resetMock();
    }

    replica.search(n, x.data(), k, distances.data(), labels.data());

    for (int i = 0; i < idxs.size(); ++i) {
      auto perReplica = n / idxs.size();

      EXPECT_EQ(idxs[i]->nCalled, perReplica);
      EXPECT_EQ(idxs[i]->xCalled, x.data() + i * perReplica * d);
      EXPECT_EQ(idxs[i]->kCalled, k);
      EXPECT_EQ(idxs[i]->distancesCalled,
                distances.data() + (i * perReplica) * k);
      EXPECT_EQ(idxs[i]->labelsCalled,
                labels.data() + (i * perReplica) * k);
    }
  }
}

TEST(ThreadedIndex, TestShards) {
  int numShards = 7;
  int d = 3;
  int n = 10 * numShards;
  int k = 6;

  // Try with threading and without
  for (bool threaded : {true, false}) {
    std::vector<std::unique_ptr<MockIndex>> idxs;
    faiss::IndexShards shards(d, threaded);

    for (int i = 0; i < numShards; ++i) {
      idxs.emplace_back(new MockIndex(d));
      shards.addIndex(idxs.back().get());
    }

    std::vector<float> x(n * d);
    std::vector<float> distances(n * k);
    std::vector<faiss::Index::idx_t> labels(n * k);

    shards.add(n, x.data());

    for (int i = 0; i < idxs.size(); ++i) {
      auto perShard = n / idxs.size();

      EXPECT_EQ(idxs[i]->nCalled, perShard);
      EXPECT_EQ(idxs[i]->xCalled, x.data() + i * perShard * d);
    }

    for (auto& idx : idxs) {
      idx->resetMock();
    }

    shards.search(n, x.data(), k, distances.data(), labels.data());

    for (int i = 0; i < idxs.size(); ++i) {
      auto perShard = n / idxs.size();

      EXPECT_EQ(idxs[i]->nCalled, n);
      EXPECT_EQ(idxs[i]->xCalled, x.data());
      EXPECT_EQ(idxs[i]->kCalled, k);
      // There is a temporary buffer used for shards
      EXPECT_EQ(idxs[i]->distancesCalled,
                idxs[0]->distancesCalled + i * k * n);
      EXPECT_EQ(idxs[i]->labelsCalled,
                idxs[0]->labelsCalled + i * k * n);
    }
  }
}

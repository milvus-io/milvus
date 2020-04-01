#include "annoylib.h"
#include "kissrandom.h"

namespace GoAnnoy {

class AnnoyIndex {
 protected:
  ::AnnoyIndexInterface<int32_t, float> *ptr;

  int f;

 public:
  ~AnnoyIndex() {
    delete ptr;
  };
  void addItem(int item, const float* w) {
    ptr->add_item(item, w);
  };
  void build(int q) {
    ptr->build(q);
  };
  bool save(const char* filename, bool prefault) {
    return ptr->save(filename, prefault);
  };
  bool save(const char* filename) {
    return ptr->save(filename, true);
  };
  void unload() {
    ptr->unload();
  };
  bool load(const char* filename, bool prefault) {
    return ptr->load(filename, prefault);
  };
  bool load(const char* filename) {
    return ptr->load(filename, true);
  };
  float getDistance(int i, int j) {
    return ptr->get_distance(i, j);
  };
  void getNnsByItem(int item, int n, int search_k, vector<int32_t>* result, vector<float>* distances) {
    ptr->get_nns_by_item(item, n, search_k, result, distances);
  };
  void getNnsByVector(const float* w, int n, int search_k, vector<int32_t>* result, vector<float>* distances) {
    ptr->get_nns_by_vector(w, n, search_k, result, distances);
  };
  void getNnsByItem(int item, int n, int search_k, vector<int32_t>* result) {
    ptr->get_nns_by_item(item, n, search_k, result, NULL);
  };
  void getNnsByVector(const float* w, int n, int search_k, vector<int32_t>* result) {
    ptr->get_nns_by_vector(w, n, search_k, result, NULL);
  };

  int getNItems() {
    return (int)ptr->get_n_items();
  };
  void verbose(bool v) {
    ptr->verbose(v);
  };
  void getItem(int item, vector<float> *v) {
    v->resize(this->f);
    ptr->get_item(item, &v->front());
  };
  bool onDiskBuild(const char* filename) {
    return ptr->on_disk_build(filename);
  };
};

class AnnoyIndexAngular : public AnnoyIndex 
{
 public:
  AnnoyIndexAngular(int f) {
    ptr = new ::AnnoyIndex<int32_t, float, ::Angular, ::Kiss64Random>(f);
    this->f = f;
  }
};

class AnnoyIndexEuclidean : public AnnoyIndex {
 public:
  AnnoyIndexEuclidean(int f) {
    ptr = new ::AnnoyIndex<int32_t, float, ::Euclidean, ::Kiss64Random>(f);
    this->f = f;
  }
};

class AnnoyIndexManhattan : public AnnoyIndex {
 public:
  AnnoyIndexManhattan(int f) {
    ptr = new ::AnnoyIndex<int32_t, float, ::Manhattan, ::Kiss64Random>(f);
    this->f = f;
  }
};
}

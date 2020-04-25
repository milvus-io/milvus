// Copyright (c) 2013 Spotify AB
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not
// use this file except in compliance with the License. You may obtain a copy of
// the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
// WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
// License for the specific language governing permissions and limitations under
// the License.

#include "annoylib.h"
#include "kissrandom.h"
#include "Python.h"
#include "structmember.h"
#include <exception>
#if defined(_MSC_VER) && _MSC_VER == 1500
typedef signed __int32    int32_t;
#else
#include <stdint.h>
#endif


#if defined(USE_AVX512)
#define AVX_INFO "Using 512-bit AVX instructions"
#elif defined(USE_AVX128)
#define AVX_INFO "Using 128-bit AVX instructions"
#else
#define AVX_INFO "Not using AVX instructions"
#endif

#if defined(_MSC_VER)
#define COMPILER_INFO "Compiled using MSC"
#elif defined(__GNUC__)
#define COMPILER_INFO "Compiled on GCC"
#else
#define COMPILER_INFO "Compiled on unknown platform"
#endif

#define ANNOY_DOC (COMPILER_INFO ". " AVX_INFO ".")

#if PY_MAJOR_VERSION >= 3
#define IS_PY3K
#endif

#ifndef Py_TYPE
    #define Py_TYPE(ob) (((PyObject*)(ob))->ob_type)
#endif

#ifdef IS_PY3K
    #define PyInt_FromLong PyLong_FromLong 
#endif


template class AnnoyIndexInterface<int32_t, float>;

class HammingWrapper : public AnnoyIndexInterface<int32_t, float> {
  // Wrapper class for Hamming distance, using composition.
  // This translates binary (float) vectors into packed uint64_t vectors.
  // This is questionable from a performance point of view. Should reconsider this solution.
private:
  int32_t _f_external, _f_internal;
  AnnoyIndex<int32_t, uint64_t, Hamming, Kiss64Random> _index;
  void _pack(const float* src, uint64_t* dst) const {
    for (int32_t i = 0; i < _f_internal; i++) {
      dst[i] = 0;
      for (int32_t j = 0; j < 64 && i*64+j < _f_external; j++) {
	dst[i] |= (uint64_t)(src[i * 64 + j] > 0.5) << j;
      }
    }
  };
  void _unpack(const uint64_t* src, float* dst) const {
    for (int32_t i = 0; i < _f_external; i++) {
      dst[i] = (src[i / 64] >> (i % 64)) & 1;
    }
  };
public:
  HammingWrapper(int f) : _f_external(f), _f_internal((f + 63) / 64), _index((f + 63) / 64) {};
  bool add_item(int32_t item, const float* w, char**error) {
    vector<uint64_t> w_internal(_f_internal, 0);
    _pack(w, &w_internal[0]);
    return _index.add_item(item, &w_internal[0], error);
  };
  bool build(int q, char** error) { return _index.build(q, error); };
  bool unbuild(char** error) { return _index.unbuild(error); };
  bool save(const char* filename, bool prefault, char** error) { return _index.save(filename, prefault, error); };
  void unload() { _index.unload(); };
  bool load(const char* filename, bool prefault, char** error) { return _index.load(filename, prefault, error); };
  float get_distance(int32_t i, int32_t j) const { return _index.get_distance(i, j); };
  void get_nns_by_item(int32_t item, size_t n, int search_k, vector<int32_t>* result, vector<float>* distances) const {
    if (distances) {
      vector<uint64_t> distances_internal;
      _index.get_nns_by_item(item, n, search_k, result, &distances_internal);
      distances->insert(distances->begin(), distances_internal.begin(), distances_internal.end());
    } else {
      _index.get_nns_by_item(item, n, search_k, result, nullptr);
    }
  };
  void get_nns_by_vector(const float* w, size_t n, int search_k, vector<int32_t>* result, vector<float>* distances) const {
    vector<uint64_t> w_internal(_f_internal, 0);
    _pack(w, &w_internal[0]);
    if (distances) {
      vector<uint64_t> distances_internal;
      _index.get_nns_by_vector(&w_internal[0], n, search_k, result, &distances_internal);
      distances->insert(distances->begin(), distances_internal.begin(), distances_internal.end());
    } else {
      _index.get_nns_by_vector(&w_internal[0], n, search_k, result, nullptr);
    }
  };
  int32_t get_n_items() const { return _index.get_n_items(); };
  int32_t get_n_trees() const { return _index.get_n_trees(); };
  void verbose(bool v) { _index.verbose(v); };
  void get_item(int32_t item, float* v) const {
    vector<uint64_t> v_internal(_f_internal, 0);
    _index.get_item(item, &v_internal[0]);
    _unpack(&v_internal[0], v);
  };
  void set_seed(int q) { _index.set_seed(q); };
  bool on_disk_build(const char* filename, char** error) { return _index.on_disk_build(filename, error); };
};

// annoy python object
typedef struct {
  PyObject_HEAD
  int f;
  AnnoyIndexInterface<int32_t, float>* ptr;
} py_annoy;


static PyObject *
py_an_new(PyTypeObject *type, PyObject *args, PyObject *kwargs) {
  py_annoy *self = (py_annoy *)type->tp_alloc(type, 0);
  if (self == nullptr) {
    return nullptr;
  }
  const char *metric = nullptr;

  static char const * kwlist[] = {"f", "metric", nullptr};
  if (!PyArg_ParseTupleAndKeywords(args, kwargs, "i|s", (char**)kwlist, &self->f, &metric))
    return nullptr;
  if (!metric) {
    // This keeps coming up, see #368 etc
    PyErr_WarnEx(PyExc_FutureWarning, "The default argument for metric will be removed "
		 "in future version of Annoy. Please pass metric='angular' explicitly.", 1);
    self->ptr = new AnnoyIndex<int32_t, float, Angular, Kiss64Random>(self->f);
  } else if (!strcmp(metric, "angular")) {
   self->ptr = new AnnoyIndex<int32_t, float, Angular, Kiss64Random>(self->f);
  } else if (!strcmp(metric, "euclidean")) {
    self->ptr = new AnnoyIndex<int32_t, float, Euclidean, Kiss64Random>(self->f);
  } else if (!strcmp(metric, "manhattan")) {
    self->ptr = new AnnoyIndex<int32_t, float, Manhattan, Kiss64Random>(self->f);
  } else if (!strcmp(metric, "hamming")) {
    self->ptr = new HammingWrapper(self->f);
  } else if (!strcmp(metric, "dot")) {
    self->ptr = new AnnoyIndex<int32_t, float, DotProduct, Kiss64Random>(self->f);
  } else {
    PyErr_SetString(PyExc_ValueError, "No such metric");
    return nullptr;
  }

  return (PyObject *)self;
}


static int 
py_an_init(py_annoy *self, PyObject *args, PyObject *kwargs) {
  // Seems to be needed for Python 3
  const char *metric = nullptr;
  int f;
  static char const * kwlist[] = {"f", "metric", nullptr};
  if (!PyArg_ParseTupleAndKeywords(args, kwargs, "i|s", (char**)kwlist, &f, &metric))
    return (int) nullptr;
  return 0;
}


static void 
py_an_dealloc(py_annoy* self) {
  delete self->ptr;
  Py_TYPE(self)->tp_free((PyObject*)self);
}


static PyMemberDef py_annoy_members[] = {
  {(char*)"f", T_INT, offsetof(py_annoy, f), 0,
   (char*)""},
  {nullptr}	/* Sentinel */
};


static PyObject *
py_an_load(py_annoy *self, PyObject *args, PyObject *kwargs) {
  char *filename, *error;
  bool prefault = false;
  if (!self->ptr) 
    return nullptr;
  static char const * kwlist[] = {"fn", "prefault", nullptr};
  if (!PyArg_ParseTupleAndKeywords(args, kwargs, "s|b", (char**)kwlist, &filename, &prefault))
    return nullptr;

  if (!self->ptr->load(filename, prefault, &error)) {
    PyErr_SetString(PyExc_IOError, error);
    free(error);
    return nullptr;
  }
  Py_RETURN_TRUE;
}


static PyObject *
py_an_save(py_annoy *self, PyObject *args, PyObject *kwargs) {
  char *filename, *error;
  bool prefault = false;
  if (!self->ptr) 
    return nullptr;
  static char const * kwlist[] = {"fn", "prefault", nullptr};
  if (!PyArg_ParseTupleAndKeywords(args, kwargs, "s|b", (char**)kwlist, &filename, &prefault))
    return nullptr;

  if (!self->ptr->save(filename, prefault, &error)) {
    PyErr_SetString(PyExc_IOError, error);
    free(error);
    return nullptr;
  }
  Py_RETURN_TRUE;
}


PyObject*
get_nns_to_python(const vector<int32_t>& result, const vector<float>& distances, int include_distances) {
  PyObject* l = PyList_New(result.size());
  for (size_t i = 0; i < result.size(); i++)
    PyList_SetItem(l, i, PyInt_FromLong(result[i]));
  if (!include_distances)
    return l;

  PyObject* d = PyList_New(distances.size());
  for (size_t i = 0; i < distances.size(); i++)
    PyList_SetItem(d, i, PyFloat_FromDouble(distances[i]));

  PyObject* t = PyTuple_New(2);
  PyTuple_SetItem(t, 0, l);
  PyTuple_SetItem(t, 1, d);

  return t;
}


bool check_constraints(py_annoy *self, int32_t item, bool building) {
  if (item < 0) {
    PyErr_SetString(PyExc_IndexError, "Item index can not be negative");
    return false;
  } else if (!building && item >= self->ptr->get_n_items()) {
    PyErr_SetString(PyExc_IndexError, "Item index larger than the largest item index");
    return false;
  } else {
    return true;
  }
}

static PyObject* 
py_an_get_nns_by_item(py_annoy *self, PyObject *args, PyObject *kwargs) {
  int32_t item, n, search_k=-1, include_distances=0;
  if (!self->ptr) 
    return nullptr;

  static char const * kwlist[] = {"i", "n", "search_k", "include_distances", nullptr};
  if (!PyArg_ParseTupleAndKeywords(args, kwargs, "ii|ii", (char**)kwlist, &item, &n, &search_k, &include_distances))
    return nullptr;

  if (!check_constraints(self, item, false)) {
    return nullptr;
  }

  vector<int32_t> result;
  vector<float> distances;

  Py_BEGIN_ALLOW_THREADS;
  self->ptr->get_nns_by_item(item, n, search_k, &result, include_distances ? &distances : nullptr);
  Py_END_ALLOW_THREADS;

  return get_nns_to_python(result, distances, include_distances);
}


bool
convert_list_to_vector(PyObject* v, int f, vector<float>* w) {
  if (PyObject_Size(v) == -1) {
    char buf[256];
    snprintf(buf, 256, "Expected an iterable, got an object of type \"%s\"", v->ob_type->tp_name);
    PyErr_SetString(PyExc_ValueError, buf);
    return false;
  }
  if (PyObject_Size(v) != f) {
    char buf[128];
    snprintf(buf, 128, "Vector has wrong length (expected %d, got %ld)", f, PyObject_Size(v));
    PyErr_SetString(PyExc_IndexError, buf);
    return false;
  }
  for (int z = 0; z < f; z++) {
    PyObject *key = PyInt_FromLong(z);
    PyObject *pf = PyObject_GetItem(v, key);
    (*w)[z] = PyFloat_AsDouble(pf);
    Py_DECREF(key);
    Py_DECREF(pf);
  }
  return true;
}

static PyObject* 
py_an_get_nns_by_vector(py_annoy *self, PyObject *args, PyObject *kwargs) {
  PyObject* v;
  int32_t n, search_k=-1, include_distances=0;
  if (!self->ptr) 
    return nullptr;

  static char const * kwlist[] = {"vector", "n", "search_k", "include_distances", nullptr};
  if (!PyArg_ParseTupleAndKeywords(args, kwargs, "Oi|ii", (char**)kwlist, &v, &n, &search_k, &include_distances))
    return nullptr;

  vector<float> w(self->f);
  if (!convert_list_to_vector(v, self->f, &w)) {
    return nullptr;
  }

  vector<int32_t> result;
  vector<float> distances;

  Py_BEGIN_ALLOW_THREADS;
  self->ptr->get_nns_by_vector(&w[0], n, search_k, &result, include_distances ? &distances : nullptr);
  Py_END_ALLOW_THREADS;

  return get_nns_to_python(result, distances, include_distances);
}


static PyObject* 
py_an_get_item_vector(py_annoy *self, PyObject *args) {
  int32_t item;
  if (!self->ptr) 
    return nullptr;
  if (!PyArg_ParseTuple(args, "i", &item))
    return nullptr;

  if (!check_constraints(self, item, false)) {
    return nullptr;
  }

  vector<float> v(self->f);
  self->ptr->get_item(item, &v[0]);
  PyObject* l = PyList_New(self->f);
  for (int z = 0; z < self->f; z++) {
    PyList_SetItem(l, z, PyFloat_FromDouble(v[z]));
  }

  return l;
}


static PyObject* 
py_an_add_item(py_annoy *self, PyObject *args, PyObject* kwargs) {
  PyObject* v;
  int32_t item;
  if (!self->ptr) 
    return nullptr;
  static char const * kwlist[] = {"i", "vector", nullptr};
  if (!PyArg_ParseTupleAndKeywords(args, kwargs, "iO", (char**)kwlist, &item, &v))
    return nullptr;

  if (!check_constraints(self, item, true)) {
    return nullptr;
  }

  vector<float> w(self->f);
  if (!convert_list_to_vector(v, self->f, &w)) {
    return nullptr;
  }
  char* error;
  if (!self->ptr->add_item(item, &w[0], &error)) {
    PyErr_SetString(PyExc_Exception, error);
    free(error);
    return nullptr;
  }

  Py_RETURN_NONE;
}

static PyObject *
py_an_on_disk_build(py_annoy *self, PyObject *args, PyObject *kwargs) {
  char *filename, *error;
  if (!self->ptr)
    return nullptr;
  static char const * kwlist[] = {"fn", nullptr};
  if (!PyArg_ParseTupleAndKeywords(args, kwargs, "s", (char**)kwlist, &filename))
    return nullptr;

  if (!self->ptr->on_disk_build(filename, &error)) {
    PyErr_SetString(PyExc_IOError, error);
    free(error);
    return nullptr;
  }
  Py_RETURN_TRUE;
}

static PyObject *
py_an_build(py_annoy *self, PyObject *args, PyObject *kwargs) {
  int q;
  if (!self->ptr) 
    return nullptr;
  static char const * kwlist[] = {"n_trees", nullptr};
  if (!PyArg_ParseTupleAndKeywords(args, kwargs, "i", (char**)kwlist, &q))
    return nullptr;

  bool res;
  char* error;
  Py_BEGIN_ALLOW_THREADS;
  res = self->ptr->build(q, &error);
  Py_END_ALLOW_THREADS;
  if (!res) {
    PyErr_SetString(PyExc_Exception, error);
    free(error);
    return nullptr;
  }

  Py_RETURN_TRUE;
}


static PyObject *
py_an_unbuild(py_annoy *self) {
  if (!self->ptr) 
    return nullptr;

  char* error;
  if (!self->ptr->unbuild(&error)) {
    PyErr_SetString(PyExc_Exception, error);
    free(error);
    return nullptr;
  }

  Py_RETURN_TRUE;
}


static PyObject *
py_an_unload(py_annoy *self) {
  if (!self->ptr) 
    return nullptr;

  self->ptr->unload();

  Py_RETURN_TRUE;
}


static PyObject *
py_an_get_distance(py_annoy *self, PyObject *args) {
  int32_t i, j;
  if (!self->ptr) 
    return nullptr;
  if (!PyArg_ParseTuple(args, "ii", &i, &j))
    return nullptr;

  if (!check_constraints(self, i, false) || !check_constraints(self, j, false)) {
    return nullptr;
  }

  double d = self->ptr->get_distance(i,j);
  return PyFloat_FromDouble(d);
}


static PyObject *
py_an_get_n_items(py_annoy *self) {
  if (!self->ptr) 
    return nullptr;

  int32_t n = self->ptr->get_n_items();
  return PyInt_FromLong(n);
}

static PyObject *
py_an_get_n_trees(py_annoy *self) {
  if (!self->ptr) 
    return nullptr;

  int32_t n = self->ptr->get_n_trees();
  return PyInt_FromLong(n);
}

static PyObject *
py_an_verbose(py_annoy *self, PyObject *args) {
  int verbose;
  if (!self->ptr) 
    return nullptr;
  if (!PyArg_ParseTuple(args, "i", &verbose))
    return nullptr;

  self->ptr->verbose((bool)verbose);

  Py_RETURN_TRUE;
}


static PyObject *
py_an_set_seed(py_annoy *self, PyObject *args) {
  int q;
  if (!self->ptr)
    return nullptr;
  if (!PyArg_ParseTuple(args, "i", &q))
    return nullptr;

  self->ptr->set_seed(q);

  Py_RETURN_NONE;
}


static PyMethodDef AnnoyMethods[] = {
  {"load",	(PyCFunction)py_an_load, METH_VARARGS | METH_KEYWORDS, "Loads (mmaps) an index from disk."},
  {"save",	(PyCFunction)py_an_save, METH_VARARGS | METH_KEYWORDS, "Saves the index to disk."},
  {"get_nns_by_item",(PyCFunction)py_an_get_nns_by_item, METH_VARARGS | METH_KEYWORDS, "Returns the `n` closest items to item `i`.\n\n:param search_k: the query will inspect up to `search_k` nodes.\n`search_k` gives you a run-time tradeoff between better accuracy and speed.\n`search_k` defaults to `n_trees * n` if not provided.\n\n:param include_distances: If `True`, this function will return a\n2 element tuple of lists. The first list contains the `n` closest items.\nThe second list contains the corresponding distances."},
  {"get_nns_by_vector",(PyCFunction)py_an_get_nns_by_vector, METH_VARARGS | METH_KEYWORDS, "Returns the `n` closest items to vector `vector`.\n\n:param search_k: the query will inspect up to `search_k` nodes.\n`search_k` gives you a run-time tradeoff between better accuracy and speed.\n`search_k` defaults to `n_trees * n` if not provided.\n\n:param include_distances: If `True`, this function will return a\n2 element tuple of lists. The first list contains the `n` closest items.\nThe second list contains the corresponding distances."},
  {"get_item_vector",(PyCFunction)py_an_get_item_vector, METH_VARARGS, "Returns the vector for item `i` that was previously added."},
  {"add_item",(PyCFunction)py_an_add_item, METH_VARARGS | METH_KEYWORDS, "Adds item `i` (any nonnegative integer) with vector `v`.\n\nNote that it will allocate memory for `max(i)+1` items."},
  {"on_disk_build",(PyCFunction)py_an_on_disk_build, METH_VARARGS | METH_KEYWORDS, "Build will be performed with storage on disk instead of RAM."},
  {"build",(PyCFunction)py_an_build, METH_VARARGS | METH_KEYWORDS, "Builds a forest of `n_trees` trees.\n\nMore trees give higher precision when querying. After calling `build`,\nno more items can be added."},
  {"unbuild",(PyCFunction)py_an_unbuild, METH_NOARGS, "Unbuilds the tree in order to allows adding new items.\n\nbuild() has to be called again afterwards in order to\nrun queries."},
  {"unload",(PyCFunction)py_an_unload, METH_NOARGS, "Unloads an index from disk."},
  {"get_distance",(PyCFunction)py_an_get_distance, METH_VARARGS, "Returns the distance between items `i` and `j`."},
  {"get_n_items",(PyCFunction)py_an_get_n_items, METH_NOARGS, "Returns the number of items in the index."},
  {"get_n_trees",(PyCFunction)py_an_get_n_trees, METH_NOARGS, "Returns the number of trees in the index."},
  {"verbose",(PyCFunction)py_an_verbose, METH_VARARGS, ""},
  {"set_seed",(PyCFunction)py_an_set_seed, METH_VARARGS, "Sets the seed of Annoy's random number generator."},
  {nullptr, nullptr, 0, nullptr}		 /* Sentinel */
};


static PyTypeObject PyAnnoyType = {
  PyVarObject_HEAD_INIT(nullptr, 0)
  "annoy.Annoy",          /*tp_name*/
  sizeof(py_annoy),       /*tp_basicsize*/
  0,                      /*tp_itemsize*/
  (destructor)py_an_dealloc, /*tp_dealloc*/
  0,                      /*tp_print*/
  0,                      /*tp_getattr*/
  0,                      /*tp_setattr*/
  0,                      /*tp_compare*/
  0,                      /*tp_repr*/
  0,                      /*tp_as_number*/
  0,                      /*tp_as_sequence*/
  0,                      /*tp_as_mapping*/
  0,                      /*tp_hash */
  0,                      /*tp_call*/
  0,                      /*tp_str*/
  0,                      /*tp_getattro*/
  0,                      /*tp_setattro*/
  0,                      /*tp_as_buffer*/
  Py_TPFLAGS_DEFAULT | Py_TPFLAGS_BASETYPE, /*tp_flags*/
  ANNOY_DOC,              /* tp_doc */
  0,                      /* tp_traverse */
  0,                      /* tp_clear */
  0,                      /* tp_richcompare */
  0,                      /* tp_weaklistoffset */
  0,                      /* tp_iter */
  0,                      /* tp_iternext */
  AnnoyMethods,           /* tp_methods */
  py_annoy_members,       /* tp_members */
  0,                      /* tp_getset */
  0,                      /* tp_base */
  0,                      /* tp_dict */
  0,                      /* tp_descr_get */
  0,                      /* tp_descr_set */
  0,                      /* tp_dictoffset */
  (initproc)py_an_init,   /* tp_init */
  0,                      /* tp_alloc */
  py_an_new,              /* tp_new */
};

static PyMethodDef module_methods[] = {
  {nullptr}	/* Sentinel */
};

#if PY_MAJOR_VERSION >= 3
  static struct PyModuleDef moduledef = {
    PyModuleDef_HEAD_INIT,
    "annoylib",          /* m_name */
    ANNOY_DOC,           /* m_doc */
    -1,                  /* m_size */
    module_methods,      /* m_methods */
    nullptr,                /* m_reload */
    nullptr,                /* m_traverse */
    nullptr,                /* m_clear */
    nullptr,                /* m_free */
  };
#endif

PyObject *create_module(void) {
  PyObject *m;

  if (PyType_Ready(&PyAnnoyType) < 0)
    return nullptr;

#if PY_MAJOR_VERSION >= 3
  m = PyModule_Create(&moduledef);
#else
  m = Py_InitModule("annoylib", module_methods);
#endif

  if (m == nullptr)
    return nullptr;

  Py_INCREF(&PyAnnoyType);
  PyModule_AddObject(m, "Annoy", (PyObject *)&PyAnnoyType);
  return m;
}

#if PY_MAJOR_VERSION >= 3
  PyMODINIT_FUNC PyInit_annoylib(void) {
    return create_module();      // it should return moudule object in py3
  }
#else
  PyMODINIT_FUNC initannoylib(void) {
    create_module();
  }
#endif


// vim: tabstop=2 shiftwidth=2

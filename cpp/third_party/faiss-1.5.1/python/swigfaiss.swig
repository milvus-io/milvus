/**
 * Copyright (c) 2015-present, Facebook, Inc.
 * All rights reserved.
 *
 * This source code is licensed under the BSD+Patents license found in the
 * LICENSE file in the root directory of this source tree.
 */

// -*- C++ -*-

// This file describes the C++-scripting language bridge for both Lua
// and Python It contains mainly includes and a few macros. There are
// 3 preprocessor macros of interest:
// SWIGLUA: Lua-specific code
// SWIGPYTHON: Python-specific code
// GPU_WRAPPER: also compile interfaces for GPU.

%module swigfaiss;

// fbode SWIG fails on warnings, so make them non fatal
#pragma SWIG nowarn=321
#pragma SWIG nowarn=403
#pragma SWIG nowarn=325
#pragma SWIG nowarn=389
#pragma SWIG nowarn=341
#pragma SWIG nowarn=512


typedef unsigned long uint64_t;
typedef uint64_t size_t;
typedef int int32_t;
typedef unsigned char uint8_t;

#define __restrict


/*******************************************************************
 * Copied verbatim to wrapper. Contains the C++-visible includes, and
 * the language includes for their respective matrix libraries.
 *******************************************************************/

%{


#include <stdint.h>
#include <omp.h>


#ifdef SWIGLUA

#include <pthread.h>

extern "C" {

#include <TH/TH.h>
#include <luaT.h>
#undef THTensor

}

#endif


#ifdef SWIGPYTHON

#undef popcount64

#define SWIG_FILE_WITH_INIT
#define NPY_NO_DEPRECATED_API NPY_1_7_API_VERSION
#include <numpy/arrayobject.h>

#endif


#include "IndexFlat.h"
#include "VectorTransform.h"
#include "IndexLSH.h"
#include "IndexPQ.h"
#include "IndexIVF.h"
#include "IndexIVFPQ.h"
#include "IndexIVFFlat.h"
#include "IndexScalarQuantizer.h"
#include "IndexShards.h"
#include "IndexReplicas.h"
#include "HNSW.h"
#include "IndexHNSW.h"
#include "MetaIndexes.h"
#include "FaissAssert.h"

#include "IndexBinaryFlat.h"
#include "IndexBinaryIVF.h"
#include "IndexBinaryFromFloat.h"
#include "IndexBinaryHNSW.h"

#include "index_io.h"

#include "IVFlib.h"
#include "utils.h"
#include "Heap.h"
#include "AuxIndexStructures.h"
#include "OnDiskInvertedLists.h"

#include "Clustering.h"

#include "hamming.h"

#include "AutoTune.h"



 %}

/*******************************************************************
 * Types of vectors we want to manipulate at the scripting language
 * level.
 *******************************************************************/

// simplified interface for vector
namespace std {

    template<class T>
    class vector {
    public:
        vector();
        void push_back(T);
        void clear();
        T * data();
        size_t size();
        T at (size_t n) const;
        void resize (size_t n);
        void swap (vector<T> & other);
    };
};



%template(FloatVector) std::vector<float>;
%template(DoubleVector) std::vector<double>;
%template(ByteVector) std::vector<uint8_t>;
%template(CharVector) std::vector<char>;
// NOTE(hoss): Using unsigned long instead of uint64_t because OSX defines
//   uint64_t as unsigned long long, which SWIG is not aware of.
%template(Uint64Vector) std::vector<unsigned long>;
%template(LongVector) std::vector<long>;
%template(IntVector) std::vector<int>;
%template(VectorTransformVector) std::vector<faiss::VectorTransform*>;
%template(OperatingPointVector) std::vector<faiss::OperatingPoint>;
%template(InvertedListsPtrVector) std::vector<faiss::InvertedLists*>;
%template(FloatVectorVector) std::vector<std::vector<float> >;
%template(ByteVectorVector) std::vector<std::vector<unsigned char> >;
%template(LongVectorVector) std::vector<std::vector<long> >;



#ifdef GPU_WRAPPER
%template(GpuResourcesVector) std::vector<faiss::gpu::GpuResources*>;
#endif

%include <std_string.i>

// produces an error on the Mac
%ignore faiss::hamming;


/*******************************************************************
 * Parse headers
 *******************************************************************/

#ifdef SWIGPYTHON
// %catches(faiss::FaissException);


// Python-specific: release GIL by default for all functions
%exception {
    Py_BEGIN_ALLOW_THREADS
    try {
        $action
    } catch(faiss::FaissException & e) {
        PyEval_RestoreThread(_save);

        if (PyErr_Occurred()) {
            // some previous code already set the error type.
        } else {
            PyErr_SetString(PyExc_RuntimeError, e.what());
        }
        SWIG_fail;
    }
    Py_END_ALLOW_THREADS
}

#endif

#ifdef SWIGLUA

%exception {
    try {
        $action
    } catch(faiss::FaissException & e) {
        SWIG_Lua_pushferrstring(L, "C++ exception: %s", e.what());       \
        goto fail;
    }
}

#endif



%ignore *::cmp;

%include "Heap.h"
%include "hamming.h"

int get_num_gpus();

#ifdef GPU_WRAPPER

%{

#include "gpu/StandardGpuResources.h"
#include "gpu/GpuIndicesOptions.h"
#include "gpu/GpuClonerOptions.h"
#include "gpu/utils/MemorySpace.h"
#include "gpu/GpuIndex.h"
#include "gpu/GpuIndexFlat.h"
#include "gpu/GpuIndexIVF.h"
#include "gpu/GpuIndexIVFPQ.h"
#include "gpu/GpuIndexIVFFlat.h"
#include "gpu/GpuIndexBinaryFlat.h"
#include "gpu/GpuAutoTune.h"
#include "gpu/GpuDistance.h"

int get_num_gpus()
{
    return faiss::gpu::getNumDevices();
}

%}

// causes weird wrapper bug
%ignore *::getMemoryManager;
%ignore *::getMemoryManagerCurrentDevice;

%include "gpu/GpuResources.h"
%include "gpu/StandardGpuResources.h"

#else

%{
int get_num_gpus()
{
    return 0;
}
%}


#endif


%include "utils.h"

%include "Index.h"
%include "Clustering.h"

%ignore faiss::ProductQuantizer::get_centroids(size_t,size_t) const;

%include "ProductQuantizer.h"

%include "VectorTransform.h"
%include "IndexFlat.h"
%include "IndexLSH.h"
%include "PolysemousTraining.h"
%include "IndexPQ.h"
%include "InvertedLists.h"
%ignore InvertedListScanner;
%ignore BinaryInvertedListScanner;
%include "IndexIVF.h"
// NOTE(hoss): SWIG (wrongly) believes the overloaded const version shadows the
//   non-const one.
%warnfilter(509) extract_index_ivf;
%include "IVFlib.h"
%include "IndexScalarQuantizer.h"
%include "HNSW.h"
%include "IndexHNSW.h"
%include "IndexIVFFlat.h"
%include "OnDiskInvertedLists.h"

%ignore faiss::IndexIVFPQ::alloc_type;
%include "IndexIVFPQ.h"

%include "IndexBinary.h"
%include "IndexBinaryFlat.h"
%include "IndexBinaryIVF.h"
%include "IndexBinaryFromFloat.h"
%include "IndexBinaryHNSW.h"



 // %ignore faiss::IndexReplicas::at(int) const;

%include "IndexShards.h"
%template(IndexShards) faiss::IndexShardsTemplate<faiss::Index>;
%template(IndexBinaryShards) faiss::IndexShardsTemplate<faiss::IndexBinary>;

%include "IndexReplicas.h"
%template(IndexReplicas) faiss::IndexReplicasTemplate<faiss::Index>;
%template(IndexBinaryReplicas) faiss::IndexReplicasTemplate<faiss::IndexBinary>;


%include "MetaIndexes.h"

#ifdef GPU_WRAPPER

// quiet SWIG warnings
%ignore faiss::gpu::GpuIndexIVF::GpuIndexIVF;

%include "gpu/GpuIndicesOptions.h"
%include "gpu/GpuClonerOptions.h"
%include "gpu/utils/MemorySpace.h"
%include "gpu/GpuIndex.h"
%include "gpu/GpuIndexFlat.h"
%include "gpu/GpuIndexIVF.h"
%include "gpu/GpuIndexIVFPQ.h"
%include "gpu/GpuIndexIVFFlat.h"
%include "gpu/GpuIndexBinaryFlat.h"
%include "gpu/GpuDistance.h"

#endif




/*******************************************************************
 * Lua-specific: support async execution of searches in an index
 * Python equivalent is just to use Python threads.
 *******************************************************************/


#ifdef SWIGLUA

%{


namespace faiss {

struct AsyncIndexSearchC {
    typedef Index::idx_t idx_t;
    const Index * index;

    idx_t n;
    const float *x;
    idx_t k;
    float *distances;
    idx_t *labels;

    bool is_finished;

    pthread_t thread;


    AsyncIndexSearchC (const Index *index,
                      idx_t n, const float *x, idx_t k,
                      float *distances, idx_t *labels):
        index(index), n(n), x(x), k(k), distances(distances),
        labels(labels)
    {
        is_finished = false;
        pthread_create (&thread, NULL, &AsyncIndexSearchC::callback,
                        this);
    }

    static void *callback (void *arg)
    {
        AsyncIndexSearchC *aidx = (AsyncIndexSearchC *)arg;
        aidx->do_search();
        return NULL;
    }

    void do_search ()
    {
        index->search (n, x, k, distances, labels);
    }
    void join ()
    {
        pthread_join (thread, NULL);
    }

};

}

%}

// re-decrlare only what we need
namespace faiss {

struct AsyncIndexSearchC {
    typedef Index::idx_t idx_t;
    bool is_finished;
    AsyncIndexSearchC (const Index *index,
                      idx_t n, const float *x, idx_t k,
                       float *distances, idx_t *labels);


    void join ();
};

}


#endif




/*******************************************************************
 * downcast return of some functions so that the sub-class is used
 * instead of the generic upper-class.
 *******************************************************************/

#ifdef SWIGLUA

%define DOWNCAST(subclass)
    if (dynamic_cast<faiss::subclass *> ($1)) {
      SWIG_NewPointerObj(L,$1,SWIGTYPE_p_faiss__ ## subclass, $owner);
    } else
%enddef

%define DOWNCAST2(subclass, longname)
    if (dynamic_cast<faiss::subclass *> ($1)) {
      SWIG_NewPointerObj(L,$1,SWIGTYPE_p_faiss__ ## longname, $owner);
    } else
%enddef

%define DOWNCAST_GPU(subclass)
    if (dynamic_cast<faiss::gpu::subclass *> ($1)) {
      SWIG_NewPointerObj(L,$1,SWIGTYPE_p_faiss__gpu__ ## subclass, $owner);
    } else
%enddef

#endif


#ifdef SWIGPYTHON

%define DOWNCAST(subclass)
    if (dynamic_cast<faiss::subclass *> ($1)) {
      $result = SWIG_NewPointerObj($1,SWIGTYPE_p_faiss__ ## subclass,$owner);
    } else
%enddef

%define DOWNCAST2(subclass, longname)
    if (dynamic_cast<faiss::subclass *> ($1)) {
      $result = SWIG_NewPointerObj($1,SWIGTYPE_p_faiss__ ## longname,$owner);
    } else
%enddef

%define DOWNCAST_GPU(subclass)
    if (dynamic_cast<faiss::gpu::subclass *> ($1)) {
      $result = SWIG_NewPointerObj($1,SWIGTYPE_p_faiss__gpu__ ## subclass,$owner);
    } else
%enddef

#endif

%newobject read_index;
%newobject read_index_binary;
%newobject read_VectorTransform;
%newobject read_ProductQuantizer;
%newobject clone_index;
%newobject clone_VectorTransform;

// Subclasses should appear before their parent
%typemap(out) faiss::Index * {
    DOWNCAST ( IndexIDMap )
    DOWNCAST2 ( IndexShards, IndexShardsTemplateT_faiss__Index_t )
    DOWNCAST2 ( IndexReplicas, IndexReplicasTemplateT_faiss__Index_t )
    DOWNCAST ( IndexIVFPQR )
    DOWNCAST ( IndexIVFPQ )
    DOWNCAST ( IndexIVFScalarQuantizer )
    DOWNCAST ( IndexIVFFlatDedup )
    DOWNCAST ( IndexIVFFlat )
    DOWNCAST ( IndexIVF )
    DOWNCAST ( IndexFlat )
    DOWNCAST ( IndexPQ )
    DOWNCAST ( IndexScalarQuantizer )
    DOWNCAST ( IndexLSH )
    DOWNCAST ( IndexPreTransform )
    DOWNCAST ( MultiIndexQuantizer )
    DOWNCAST ( IndexHNSWFlat )
    DOWNCAST ( IndexHNSWPQ )
    DOWNCAST ( IndexHNSWSQ )
    DOWNCAST ( IndexHNSW2Level )
    DOWNCAST ( Index2Layer )
#ifdef GPU_WRAPPER
    DOWNCAST_GPU ( GpuIndexIVFPQ )
    DOWNCAST_GPU ( GpuIndexIVFFlat )
    DOWNCAST_GPU ( GpuIndexFlat )
#endif
    // default for non-recognized classes
    DOWNCAST ( Index )
    if ($1 == NULL)
    {
#ifdef SWIGPYTHON
        $result = SWIG_Py_Void();
#endif
        // Lua does not need a push for nil
    } else {
        assert(false);
    }
#ifdef SWIGLUA
    SWIG_arg++;
#endif
}

%typemap(out) faiss::IndexBinary * {
    DOWNCAST2 ( IndexBinaryReplicas, IndexReplicasTemplateT_faiss__IndexBinary_t )
    DOWNCAST ( IndexBinaryIVF )
    DOWNCAST ( IndexBinaryFlat )
    DOWNCAST ( IndexBinaryFromFloat )
    DOWNCAST ( IndexBinaryHNSW )
#ifdef GPU_WRAPPER
    DOWNCAST_GPU ( GpuIndexBinaryFlat )
#endif
    // default for non-recognized classes
    DOWNCAST ( IndexBinary )
    if ($1 == NULL)
    {
#ifdef SWIGPYTHON
        $result = SWIG_Py_Void();
#endif
        // Lua does not need a push for nil
    } else {
        assert(false);
    }
#ifdef SWIGLUA
    SWIG_arg++;
#endif
}

%typemap(out) faiss::VectorTransform * {
    DOWNCAST (RemapDimensionsTransform)
    DOWNCAST (OPQMatrix)
    DOWNCAST (PCAMatrix)
    DOWNCAST (RandomRotationMatrix)
    DOWNCAST (LinearTransform)
    DOWNCAST (NormalizationTransform)
    DOWNCAST (CenteringTransform)
    DOWNCAST (VectorTransform)
    {
        assert(false);
    }
#ifdef SWIGLUA
    SWIG_arg++;
#endif
}

// just to downcast pointers that come from elsewhere (eg. direct
// access to object fields)
%inline %{
faiss::Index * downcast_index (faiss::Index *index)
{
    return index;
}
faiss::VectorTransform * downcast_VectorTransform (faiss::VectorTransform *vt)
{
    return vt;
}
faiss::IndexBinary * downcast_IndexBinary (faiss::IndexBinary *index)
{
    return index;
}
%}


%include "index_io.h"

%newobject index_factory;
%newobject index_binary_factory;

%include "AutoTune.h"


#ifdef GPU_WRAPPER

%newobject index_gpu_to_cpu;
%newobject index_cpu_to_gpu;
%newobject index_cpu_to_gpu_multiple;

%include "gpu/GpuAutoTune.h"

#endif

// Python-specific: do not release GIL any more, as functions below
// use the Python/C API
#ifdef SWIGPYTHON
%exception;
#endif





/*******************************************************************
 * Python specific: numpy array <-> C++ pointer interface
 *******************************************************************/

#ifdef SWIGPYTHON

%{
PyObject *swig_ptr (PyObject *a)
{
    if(!PyArray_Check(a)) {
        PyErr_SetString(PyExc_ValueError, "input not a numpy array");
        return NULL;
    }
    PyArrayObject *ao = (PyArrayObject *)a;

    if(!PyArray_ISCONTIGUOUS(ao)) {
        PyErr_SetString(PyExc_ValueError, "array is not C-contiguous");
        return NULL;
    }
    void * data = PyArray_DATA(ao);
    if(PyArray_TYPE(ao) == NPY_FLOAT32) {
        return SWIG_NewPointerObj(data, SWIGTYPE_p_float, 0);
    }
    if(PyArray_TYPE(ao) == NPY_FLOAT64) {
        return SWIG_NewPointerObj(data, SWIGTYPE_p_double, 0);
    }
    if(PyArray_TYPE(ao) == NPY_INT32) {
        return SWIG_NewPointerObj(data, SWIGTYPE_p_int, 0);
    }
    if(PyArray_TYPE(ao) == NPY_UINT8) {
        return SWIG_NewPointerObj(data, SWIGTYPE_p_unsigned_char, 0);
    }
    if(PyArray_TYPE(ao) == NPY_INT8) {
        return SWIG_NewPointerObj(data, SWIGTYPE_p_char, 0);
    }
    if(PyArray_TYPE(ao) == NPY_UINT64) {
        return SWIG_NewPointerObj(data, SWIGTYPE_p_unsigned_long, 0);
    }
    if(PyArray_TYPE(ao) == NPY_INT64) {
        return SWIG_NewPointerObj(data, SWIGTYPE_p_long, 0);
    }
    PyErr_SetString(PyExc_ValueError, "did not recognize array type");
    return NULL;
}


struct PythonInterruptCallback: faiss::InterruptCallback {

    bool want_interrupt () override {
        int err;
        {
            PyGILState_STATE gstate;
            gstate = PyGILState_Ensure();
            err = PyErr_CheckSignals();
            PyGILState_Release(gstate);
        }
        return err == -1;
    }

};


%}


%init %{
    /* needed, else crash at runtime */
    import_array();

    faiss::InterruptCallback::instance.reset(new PythonInterruptCallback());

%}

// return a pointer usable as input for functions that expect pointers
PyObject *swig_ptr (PyObject *a);

%define REV_SWIG_PTR(ctype, numpytype)

%{
PyObject * rev_swig_ptr(ctype *src, npy_intp size) {
    return PyArray_SimpleNewFromData(1, &size, numpytype, src);
}
%}

PyObject * rev_swig_ptr(ctype *src, size_t size);

%enddef

REV_SWIG_PTR(float, NPY_FLOAT32);
REV_SWIG_PTR(int, NPY_INT32);
REV_SWIG_PTR(unsigned char, NPY_UINT8);
REV_SWIG_PTR(unsigned long, NPY_UINT64);
REV_SWIG_PTR(long, NPY_INT64);


#endif



/*******************************************************************
 * Lua specific: Torch tensor <-> C++ pointer interface
 *******************************************************************/

#ifdef SWIGLUA


// provide a XXX_ptr function to convert Lua XXXTensor -> C++ XXX*

%define TYPE_CONVERSION(ctype, tensortype)

// typemap for the *_ptr_from_cdata function
%typemap(in) ctype** {
    if(lua_type(L, $input) != 10) {
        fprintf(stderr, "not cdata input\n");
        SWIG_fail;
    }
    $1 = (ctype**)lua_topointer(L, $input);
}


// SWIG and C declaration for the *_ptr_from_cdata function
%{
ctype * ctype ## _ptr_from_cdata(ctype **x, long ofs) {
     return *x + ofs;
}
%}
ctype * ctype ## _ptr_from_cdata(ctype **x, long ofs);

// the *_ptr function
%luacode {

function swigfaiss. ctype ## _ptr(tensor)
   assert(tensor:type() == "torch." .. # tensortype, "need a " .. # tensortype)
   assert(tensor:isContiguous(), "requires contiguous tensor")
   return swigfaiss. ctype ## _ptr_from_cdata(
         tensor:storage():data(),
         tensor:storageOffset() - 1)
end

}

%enddef

TYPE_CONVERSION (int, IntTensor)
TYPE_CONVERSION (float, FloatTensor)
TYPE_CONVERSION (long, LongTensor)
TYPE_CONVERSION (uint64_t, LongTensor)
TYPE_CONVERSION (uint8_t, ByteTensor)

#endif

/*******************************************************************
 * How should the template objects apprear in the scripting language?
 *******************************************************************/

// answer: the same as the C++ typedefs, but we still have to redefine them

%template() faiss::CMin<float, long>;
%template() faiss::CMin<int, long>;
%template() faiss::CMax<float, long>;
%template() faiss::CMax<int, long>;

%template(float_minheap_array_t) faiss::HeapArray<faiss::CMin<float, long> >;
%template(int_minheap_array_t) faiss::HeapArray<faiss::CMin<int, long> >;

%template(float_maxheap_array_t) faiss::HeapArray<faiss::CMax<float, long> >;
%template(int_maxheap_array_t) faiss::HeapArray<faiss::CMax<int, long> >;


/*******************************************************************
 * Expose a few basic functions
 *******************************************************************/


void omp_set_num_threads (int num_threads);
int omp_get_max_threads ();
void *memcpy(void *dest, const void *src, size_t n);


/*******************************************************************
 * For Faiss/Pytorch interop via pointers encoded as longs
 *******************************************************************/

%inline %{
float * cast_integer_to_float_ptr (long x) {
    return (float*)x;
}

long * cast_integer_to_long_ptr (long x) {
    return (long*)x;
}

int * cast_integer_to_int_ptr (long x) {
    return (int*)x;
}

%}



/*******************************************************************
 * Range search interface
 *******************************************************************/

%ignore faiss::BufferList::Buffer;
%ignore faiss::RangeSearchPartialResult::QueryResult;
%ignore faiss::IDSelectorBatch::set;
%ignore faiss::IDSelectorBatch::bloom;

%ignore faiss::InterruptCallback::instance;
%include "AuxIndexStructures.h"

%{
// may be useful for lua code launched in background from shell

#include <signal.h>
void ignore_SIGTTIN() {
    signal(SIGTTIN, SIG_IGN);
}
%}

void ignore_SIGTTIN();


%inline %{

// numpy misses a hash table implementation, hence this class. It
// represents not found values as -1 like in the Index implementation

struct MapLong2Long {
    std::unordered_map<long, long> map;

    void add(size_t n, const long *keys, const long *vals) {
        map.reserve(map.size() + n);
        for (size_t i = 0; i < n; i++) {
            map[keys[i]] = vals[i];
        }
    }

    long search(long key) {
        if (map.count(key) == 0) {
            return -1;
        } else {
            return map[key];
        }
    }

    void search_multiple(size_t n, const long *keys, long * vals) {
        for (size_t i = 0; i < n; i++) {
            vals[i] = search(keys[i]);
        }
    }
};

%}

// End of file...

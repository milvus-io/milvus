/**
 * Copyright (c) Facebook, Inc. and its affiliates.
 *
 * This source code is licensed under the MIT license found in the
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

%include <stdint.i>

typedef uint64_t size_t;

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


#include <faiss/IndexFlat.h>
#include <faiss/VectorTransform.h>
#include <faiss/IndexPreTransform.h>
#include <faiss/IndexLSH.h>
#include <faiss/IndexPQ.h>
#include <faiss/IndexIVF.h>
#include <faiss/IndexIVFPQ.h>
#include <faiss/Index2Layer.h>
#include <faiss/IndexIVFPQR.h>
#include <faiss/IndexIVFFlat.h>
#include <faiss/IndexScalarQuantizer.h>
#include <faiss/IndexIVFSpectralHash.h>
#include <faiss/impl/ThreadedIndex.h>
#include <faiss/IndexShards.h>
#include <faiss/IndexReplicas.h>
#include <faiss/impl/HNSW.h>
#include <faiss/IndexHNSW.h>
#include <faiss/MetaIndexes.h>
#include <faiss/impl/FaissAssert.h>

#include <faiss/IndexBinaryFlat.h>
#include <faiss/IndexBinaryIVF.h>
#include <faiss/IndexBinaryFromFloat.h>
#include <faiss/IndexBinaryHNSW.h>
#include <faiss/IndexBinaryHash.h>

#include <faiss/impl/io.h>
#include <faiss/index_io.h>
#include <faiss/clone_index.h>

#include <faiss/IVFlib.h>
#include <faiss/utils/utils.h>
#include <faiss/utils/distances.h>
#include <faiss/utils/extra_distances.h>
#include <faiss/utils/random.h>
#include <faiss/utils/Heap.h>
#include <faiss/impl/AuxIndexStructures.h>
#include <faiss/OnDiskInvertedLists.h>

#include <faiss/Clustering.h>

#include <faiss/utils/hamming.h>

#include <faiss/AutoTune.h>
#include <faiss/MatrixStats.h>
#include <faiss/index_factory.h>

#include <faiss/impl/lattice_Zn.h>
#include <faiss/IndexLattice.h>


%}

/********************************************************
 * GIL manipulation and exception handling
 ********************************************************/

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
    } catch(std::bad_alloc & ba) {
        PyEval_RestoreThread(_save);
        PyErr_SetString(PyExc_MemoryError, "std::bad_alloc");
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
%template(LongLongVector) std::vector<long long>;
%template(IntVector) std::vector<int>;
%template(FloatVectorVector) std::vector<std::vector<float> >;
%template(ByteVectorVector) std::vector<std::vector<unsigned char> >;
%template(LongVectorVector) std::vector<std::vector<long> >;
%template(VectorTransformVector) std::vector<faiss::VectorTransform*>;
%template(OperatingPointVector) std::vector<faiss::OperatingPoint>;
%template(InvertedListsPtrVector) std::vector<faiss::InvertedLists*>;
%template(RepeatVector) std::vector<faiss::Repeat>;
%template(ClusteringIterationStatsVector) std::vector<faiss::ClusteringIterationStats>;

#ifdef GPU_WRAPPER
%template(GpuResourcesVector) std::vector<faiss::gpu::GpuResources*>;
#endif

%include <std_string.i>

// produces an error on the Mac
%ignore faiss::hamming;

/*******************************************************************
 * Parse headers
 *******************************************************************/


%ignore *::cmp;

%include  <faiss/utils/Heap.h>
%include  <faiss/utils/hamming.h>

int get_num_gpus();
void gpu_profiler_start();
void gpu_profiler_stop();
void gpu_sync_all_devices();

#ifdef GPU_WRAPPER

%{

#include <faiss/gpu/StandardGpuResources.h>
#include <faiss/gpu/GpuIndicesOptions.h>
#include <faiss/gpu/GpuClonerOptions.h>
#include <faiss/gpu/utils/MemorySpace.h>
#include <faiss/gpu/GpuIndex.h>
#include <faiss/gpu/GpuIndexFlat.h>
#include <faiss/gpu/GpuIndexIVF.h>
#include <faiss/gpu/GpuIndexIVFPQ.h>
#include <faiss/gpu/GpuIndexIVFFlat.h>
#include <faiss/gpu/GpuIndexIVFScalarQuantizer.h>
#include <faiss/gpu/GpuIndexBinaryFlat.h>
#include <faiss/gpu/GpuAutoTune.h>
#include <faiss/gpu/GpuCloner.h>
#include <faiss/gpu/GpuDistance.h>

int get_num_gpus()
{
    return faiss::gpu::getNumDevices();
}

void gpu_profiler_start()
{
    return faiss::gpu::profilerStart();
}

void gpu_profiler_stop()
{
    return faiss::gpu::profilerStop();
}

void gpu_sync_all_devices()
{
    return faiss::gpu::synchronizeAllDevices();
}

%}

// causes weird wrapper bug
%ignore *::getMemoryManager;
%ignore *::getMemoryManagerCurrentDevice;

%include  <faiss/gpu/GpuResources.h>
%include  <faiss/gpu/StandardGpuResources.h>

#else

%{
int get_num_gpus()
{
    return 0;
}

void gpu_profiler_start()
{
}

void gpu_profiler_stop()
{
}

void gpu_sync_all_devices()
{
}
%}


#endif

// order matters because includes are not recursive

%include  <faiss/utils/utils.h>
%include  <faiss/utils/distances.h>
%include  <faiss/utils/random.h>

%include  <faiss/MetricType.h>
%include  <faiss/Index.h>
%include  <faiss/Clustering.h>

%include  <faiss/utils/extra_distances.h>

%ignore faiss::ProductQuantizer::get_centroids(size_t,size_t) const;

%include  <faiss/impl/ProductQuantizer.h>

%include  <faiss/VectorTransform.h>
%include  <faiss/IndexPreTransform.h>
%include  <faiss/IndexFlat.h>
%include  <faiss/IndexLSH.h>
%include  <faiss/impl/PolysemousTraining.h>
%include  <faiss/IndexPQ.h>
%include  <faiss/InvertedLists.h>
%include  <faiss/DirectMap.h>
%ignore InvertedListScanner;
%ignore BinaryInvertedListScanner;
%include  <faiss/IndexIVF.h>
// NOTE(hoss): SWIG (wrongly) believes the overloaded const version shadows the
//   non-const one.
%warnfilter(509) extract_index_ivf;
%warnfilter(509) try_extract_index_ivf;
%include  <faiss/IVFlib.h>
%include  <faiss/impl/ScalarQuantizer.h>
%include  <faiss/IndexScalarQuantizer.h>
%include  <faiss/IndexIVFSpectralHash.h>
%include  <faiss/impl/HNSW.h>
%include  <faiss/IndexHNSW.h>
%include  <faiss/IndexIVFFlat.h>
%include  <faiss/OnDiskInvertedLists.h>

%include  <faiss/impl/lattice_Zn.h>
%include  <faiss/IndexLattice.h>

%ignore faiss::IndexIVFPQ::alloc_type;
%include  <faiss/IndexIVFPQ.h>
%include  <faiss/IndexIVFPQR.h>
%include  <faiss/Index2Layer.h>

%include  <faiss/IndexBinary.h>
%include  <faiss/IndexBinaryFlat.h>
%include  <faiss/IndexBinaryIVF.h>
%include  <faiss/IndexBinaryFromFloat.h>
%include  <faiss/IndexBinaryHNSW.h>
%include  <faiss/IndexBinaryHash.h>



 // %ignore faiss::IndexReplicas::at(int) const;

%include  <faiss/impl/ThreadedIndex.h>
%template(ThreadedIndexBase) faiss::ThreadedIndex<faiss::Index>;
%template(ThreadedIndexBaseBinary) faiss::ThreadedIndex<faiss::IndexBinary>;

%include  <faiss/IndexShards.h>
%template(IndexShards) faiss::IndexShardsTemplate<faiss::Index>;
%template(IndexBinaryShards) faiss::IndexShardsTemplate<faiss::IndexBinary>;

%include  <faiss/IndexReplicas.h>
%template(IndexReplicas) faiss::IndexReplicasTemplate<faiss::Index>;
%template(IndexBinaryReplicas) faiss::IndexReplicasTemplate<faiss::IndexBinary>;

%include  <faiss/MetaIndexes.h>
%template(IndexIDMap) faiss::IndexIDMapTemplate<faiss::Index>;
%template(IndexBinaryIDMap) faiss::IndexIDMapTemplate<faiss::IndexBinary>;
%template(IndexIDMap2) faiss::IndexIDMap2Template<faiss::Index>;
%template(IndexBinaryIDMap2) faiss::IndexIDMap2Template<faiss::IndexBinary>;

#ifdef GPU_WRAPPER

// quiet SWIG warnings
%ignore faiss::gpu::GpuIndexIVF::GpuIndexIVF;

%include  <faiss/gpu/GpuIndicesOptions.h>
%include  <faiss/gpu/GpuClonerOptions.h>
%include  <faiss/gpu/utils/MemorySpace.h>
%include  <faiss/gpu/GpuIndex.h>
%include  <faiss/gpu/GpuIndexFlat.h>
%include  <faiss/gpu/GpuIndexIVF.h>
%include  <faiss/gpu/GpuIndexIVFPQ.h>
%include  <faiss/gpu/GpuIndexIVFFlat.h>
%include  <faiss/gpu/GpuIndexIVFScalarQuantizer.h>
%include  <faiss/gpu/GpuIndexBinaryFlat.h>
%include  <faiss/gpu/GpuDistance.h>

#ifdef SWIGLUA

/// in Lua, swigfaiss_gpu is known as swigfaiss
%luacode {
local swigfaiss = swigfaiss_gpu
}

#endif


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
    DOWNCAST2 ( IndexIDMap, IndexIDMapTemplateT_faiss__Index_t )
    DOWNCAST2 ( IndexIDMap2, IndexIDMap2TemplateT_faiss__Index_t )
    DOWNCAST2 ( IndexShards, IndexShardsTemplateT_faiss__Index_t )
    DOWNCAST2 ( IndexReplicas, IndexReplicasTemplateT_faiss__Index_t )
    DOWNCAST ( IndexIVFPQR )
    DOWNCAST ( IndexIVFPQ )
    DOWNCAST ( IndexIVFSpectralHash )
    DOWNCAST ( IndexIVFScalarQuantizer )
    DOWNCAST ( IndexIVFFlatDedup )
    DOWNCAST ( IndexIVFFlat )
    DOWNCAST ( IndexIVF )
    DOWNCAST ( IndexFlat )
    DOWNCAST ( IndexPQ )
    DOWNCAST ( IndexScalarQuantizer )
    DOWNCAST ( IndexLSH )
    DOWNCAST ( IndexLattice )
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
    DOWNCAST_GPU ( GpuIndexIVFScalarQuantizer )
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
    DOWNCAST2 ( IndexBinaryIDMap, IndexIDMapTemplateT_faiss__IndexBinary_t )
    DOWNCAST2 ( IndexBinaryIDMap2, IndexIDMap2TemplateT_faiss__IndexBinary_t )
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

%typemap(out) faiss::InvertedLists * {
    DOWNCAST (ArrayInvertedLists)
    DOWNCAST (OnDiskInvertedLists)
    DOWNCAST (VStackInvertedLists)
    DOWNCAST (HStackInvertedLists)
    DOWNCAST (MaskedInvertedLists)
    DOWNCAST (InvertedLists)
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
faiss::InvertedLists * downcast_InvertedLists (faiss::InvertedLists *il)
{
    return il;
}
%}

%include  <faiss/impl/io.h>
%include  <faiss/index_io.h>
%include  <faiss/clone_index.h>

%newobject index_factory;
%newobject index_binary_factory;

%include  <faiss/AutoTune.h>
%include  <faiss/index_factory.h>
%include  <faiss/MatrixStats.h>


#ifdef GPU_WRAPPER

%include  <faiss/gpu/GpuAutoTune.h>

%newobject index_gpu_to_cpu;
%newobject index_cpu_to_gpu;
%newobject index_cpu_to_gpu_multiple;

%include  <faiss/gpu/GpuCloner.h>

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
#ifdef SWIGWORDSIZE64
        return SWIG_NewPointerObj(data, SWIGTYPE_p_unsigned_long, 0);
#else
        return SWIG_NewPointerObj(data, SWIGTYPE_p_unsigned_long_long, 0);
#endif
    }
    if(PyArray_TYPE(ao) == NPY_INT64) {
#ifdef SWIGWORDSIZE64
        return SWIG_NewPointerObj(data, SWIGTYPE_p_long, 0);
#else
        return SWIG_NewPointerObj(data, SWIGTYPE_p_long_long, 0);
#endif
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
REV_SWIG_PTR(int64_t, NPY_INT64);
REV_SWIG_PTR(uint64_t, NPY_UINT64);

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

%template() faiss::CMin<float, int64_t>;
%template() faiss::CMin<int, int64_t>;
%template() faiss::CMax<float, int64_t>;
%template() faiss::CMax<int, int64_t>;

%template(float_minheap_array_t) faiss::HeapArray<faiss::CMin<float, int64_t> >;
%template(int_minheap_array_t) faiss::HeapArray<faiss::CMin<int, int64_t> >;

%template(float_maxheap_array_t) faiss::HeapArray<faiss::CMax<float, int64_t> >;
%template(int_maxheap_array_t) faiss::HeapArray<faiss::CMax<int, int64_t> >;


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
%ignore faiss::InterruptCallback::lock;
%include  <faiss/impl/AuxIndexStructures.h>

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
    std::unordered_map<int64_t, int64_t> map;

    void add(size_t n, const int64_t *keys, const int64_t *vals) {
        map.reserve(map.size() + n);
        for (size_t i = 0; i < n; i++) {
            map[keys[i]] = vals[i];
        }
    }

    long search(int64_t key) {
        if (map.count(key) == 0) {
            return -1;
        } else {
            return map[key];
        }
    }

    void search_multiple(size_t n, int64_t *keys, int64_t * vals) {
        for (size_t i = 0; i < n; i++) {
            vals[i] = search(keys[i]);
        }
    }
};

%}

/*******************************************************************
 * Support I/O to arbitrary functions
 *******************************************************************/


%inline %{

#ifdef SWIGPYTHON


struct PyCallbackIOWriter: faiss::IOWriter {

    PyObject * callback;
    size_t bs; // maximum write size

    PyCallbackIOWriter(PyObject *callback,
                       size_t bs = 1024 * 1024):
        callback(callback), bs(bs) {
        Py_INCREF(callback);
        name = "PyCallbackIOWriter";
    }

    size_t operator()(const void *ptrv, size_t size, size_t nitems) override {
        size_t ws = size * nitems;
        const char *ptr = (const char*)ptrv;
        PyGILState_STATE gstate;
        gstate = PyGILState_Ensure();
        while(ws > 0) {
            size_t wi = ws > bs ? bs : ws;
            PyObject* bo = PyBytes_FromStringAndSize(ptr, wi);
            PyObject *arglist = Py_BuildValue("(N)", bo);
            if(!arglist) {
                PyGILState_Release(gstate);
                return 0;
            }
            ptr += wi;
            ws -= wi;
            PyObject * result = PyObject_CallObject(callback, arglist);
            Py_DECREF(arglist);
            if (result == NULL) {
                PyGILState_Release(gstate);
                return 0;
            }
            Py_DECREF(result);
        }
        PyGILState_Release(gstate);
        return nitems;
    }

    ~PyCallbackIOWriter() {
        Py_DECREF(callback);
    }

};

struct PyCallbackIOReader: faiss::IOReader {

    PyObject * callback;
    size_t bs; // maximum buffer size

    PyCallbackIOReader(PyObject *callback,
                       size_t bs = 1024 * 1024):
        callback(callback), bs(bs) {
        Py_INCREF(callback);
        name = "PyCallbackIOReader";
    }

    size_t operator()(void *ptrv, size_t size, size_t nitems) override {
        size_t rs = size * nitems;
        char *ptr = (char*)ptrv;
        PyGILState_STATE gstate;
        gstate = PyGILState_Ensure();
        while(rs > 0) {
            size_t ri = rs > bs ? bs : rs;
            PyObject *arglist = Py_BuildValue("(n)", ri);
            PyObject * result = PyObject_CallObject(callback, arglist);
            Py_DECREF(arglist);
            if (result == NULL) {
                PyGILState_Release(gstate);
                return 0;
            }
            if(!PyBytes_Check(result)) {
                Py_DECREF(result);
                PyErr_SetString(PyExc_RuntimeError,
                                "read callback did not return a bytes object");
                PyGILState_Release(gstate);
                throw faiss::FaissException("reader error");
            }
            size_t sz = PyBytes_Size(result);
            if (sz == 0 || sz > rs) {
                Py_DECREF(result);
                PyErr_Format(PyExc_RuntimeError,
                             "read callback returned %ld bytes (asked %ld)",
                             sz, rs);
                PyGILState_Release(gstate);
                throw faiss::FaissException("reader error");
            }
            memcpy(ptr, PyBytes_AsString(result), sz);
            Py_DECREF(result);
            ptr += sz;
            rs -= sz;
        }
        PyGILState_Release(gstate);
        return nitems;
    }

    ~PyCallbackIOReader() {
        Py_DECREF(callback);
    }

};

#endif

%}



%inline %{
    void wait() {
        // in gdb, use return to get out of this function
        for(int i = 0; i == 0; i += 0);
    }
 %}

// End of file...

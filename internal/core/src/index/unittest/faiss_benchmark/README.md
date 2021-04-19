### To run this FAISS benchmark, please follow these steps:
 
#### Step 1:
Download the HDF5 source from:
  https://support.hdfgroup.org/ftp/HDF5/releases/
and build/install to "/usr/local/hdf5".

#### Step 2:
Download HDF5 data files from:
  https://github.com/erikbern/ann-benchmarks

#### Step 3:
Update 'milvus/core/src/index/unittest/CMakeLists.txt',
uncomment "#add_subdirectory(faiss_benchmark)".

#### Step 4:
Build Milvus with unittest enabled: "./build.sh -t Release -u",
binary 'test_faiss_benchmark' will be generated.

#### Step 5:
Put HDF5 data files into the same directory with binary 'test_faiss_benchmark'.

#### Step 6:
Run test binary 'test_faiss_benchmark'.


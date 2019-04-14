FROM soumith/conda-cuda:latest

COPY ./ faiss
WORKDIR /faiss/conda

ENV FAISS_BUILD_VERSION 1.5.1
ENV FAISS_BUILD_NUMBER  1
RUN conda build faiss --no-anaconda-upload -c pytorch
RUN CUDA_ROOT=/usr/local/cuda-8.0 \
    CUDA_ARCH="-gencode=arch=compute_35,code=compute_35 \
               -gencode=arch=compute_52,code=compute_52 \
               -gencode=arch=compute_60,code=compute_60 \
               -gencode=arch=compute_61,code=compute_61" \
    conda build faiss-gpu --variants '{ "cudatoolkit": "8.0" }' \
          --no-anaconda-upload -c pytorch --no-test
RUN CUDA_ROOT=/usr/local/cuda-9.0 \
    CUDA_ARCH="-gencode=arch=compute_35,code=compute_35 \
               -gencode=arch=compute_52,code=compute_52 \
               -gencode=arch=compute_60,code=compute_60 \
               -gencode=arch=compute_61,code=compute_61 \
               -gencode=arch=compute_70,code=compute_70" \
    conda build faiss-gpu --variants '{ "cudatoolkit": "9.0" }' \
          --no-anaconda-upload -c pytorch --no-test
RUN CUDA_ROOT=/usr/local/cuda-10.0 \
    CUDA_ARCH="-gencode=arch=compute_35,code=compute_35 \
               -gencode=arch=compute_52,code=compute_52 \
               -gencode=arch=compute_60,code=compute_60 \
               -gencode=arch=compute_61,code=compute_61 \
               -gencode=arch=compute_70,code=compute_70 \
               -gencode=arch=compute_72,code=compute_72 \
               -gencode=arch=compute_75,code=compute_75" \
    conda build faiss-gpu --variants '{ "cudatoolkit": "10.0" }' \
          --no-anaconda-upload -c pytorch --no-test

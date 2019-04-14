./configure --with-cuda=$CUDA_ROOT --with-cuda-arch="$CUDA_ARCH"
make -j $CPU_COUNT
cd python
make
$PYTHON setup.py install --single-version-externally-managed --record=record.txt

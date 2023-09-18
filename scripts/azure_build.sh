ROOT_DIR=$1

AZURE_CMAKE_CMD="cmake \
-DCMAKE_INSTALL_LIBDIR=${ROOT_DIR}/internal/core/output/lib \
${ROOT_DIR}/internal/core/src/storage/azure-blob-storage"
echo ${AZURE_CMAKE_CMD}
${AZURE_CMAKE_CMD}

make & make install
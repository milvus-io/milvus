#include <folly/init/Init.h>

#include "storage/LocalChunkManagerSingleton.h"
#include "storage/RemoteChunkManagerSingleton.h"
#include "storage/MmapManager.h"
#include "test_utils/Constants.h"
#include "test_utils/storage_test_utils.h"
#include <benchmark/benchmark.h>

int
main(int argc, char** argv) {
    folly::Init follyInit(&argc, &argv, false);

    milvus::storage::LocalChunkManagerSingleton::GetInstance().Init(
        TestLocalPath);
    milvus::storage::RemoteChunkManagerSingleton::GetInstance().Init(
        get_default_local_storage_config());
    milvus::storage::MmapManager::GetInstance().Init(get_default_mmap_config());

    benchmark::Initialize(&argc, argv);
    benchmark::RunSpecifiedBenchmarks();
    benchmark::Shutdown();

    return 0;
}

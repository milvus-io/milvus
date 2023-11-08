#include "../AzureBlobChunkManager.h"

using namespace azure;

void
print(Azure::Core::Diagnostics::Logger::Level level,
      std::string const& message) {
    std::cout << "level: "
              << ", message: " << message << std::endl;
}

int
main() {
    const char* containerName = "default";
    const char* blobName = "sample-blob";
    AzureBlobChunkManager::InitLog("info", print);
    AzureBlobChunkManager chunkManager = AzureBlobChunkManager("", "", "");
    std::vector<std::string> buckets = chunkManager.ListBuckets();
    std::cout << "list buckets." << std::endl;
    for (const auto& bucket : buckets) {
        std::cout << bucket << std::endl;
    }
    if (!chunkManager.BucketExists(containerName)) {
        std::cout << "create a bucket named: " << containerName << std::endl;
        chunkManager.CreateBucket(containerName);
    } else {
        std::vector<std::string> objects =
            chunkManager.ListObjects(containerName, blobName);
        for (const auto& object : objects) {
            std::cout << object << std::endl;
        }
    }
    std::cout << chunkManager.BucketExists(containerName) << std::endl;

    if (!chunkManager.ObjectExists(containerName, blobName)) {
        char msg[12];
        memcpy(msg, "Azure hello!", 12);
        chunkManager.PutObjectBuffer(containerName, blobName, msg, 12);
    }
    std::cout << chunkManager.GetObjectSize(containerName, blobName)
              << std::endl;
    std::cout << chunkManager.ObjectExists(containerName, blobName)
              << std::endl;
    std::cout << chunkManager.ObjectExists(containerName, "blobName")
              << std::endl;
    char buffer[1024 * 1024];
    chunkManager.GetObjectBuffer(containerName, blobName, buffer, 1024 * 1024);
    std::cout << buffer << std::endl;
    chunkManager.DeleteObject(containerName, blobName);
    try {
        chunkManager.GetObjectBuffer(
            containerName, "blobName", buffer, 1024 * 1024);
    } catch (const std::exception& e) {
        std::cout << "object('" << containerName << "', 'blobName') not exists"
                  << e.what() << std::endl;
    }
    std::cout << "create a bucket duplicated "
              << chunkManager.CreateBucket(containerName) << std::endl;
    chunkManager.DeleteBucket(containerName);
    exit(EXIT_SUCCESS);
}

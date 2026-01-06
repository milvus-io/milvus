#include "ncs/ncs_c.h"
#include "ncs/ncs.h"
#include "common/EasyAssert.h"
#include <stdexcept>
#include <string>
#include <nlohmann/json.hpp>

using json = nlohmann::json;

// C wrapper implementation for NCS singleton initialization

CStatus
initNcsSingleton(const char* ncsKind, const char* ncsExtras) {
    if (ncsKind == nullptr) {
        return milvus::FailureCStatus(milvus::InvalidParameter,
                                      "NCS kind parameter is null");
    }

    try {
        std::string kind(ncsKind);
        json extras = json::object();

        // Parse extras if provided
        if (ncsExtras != nullptr && ncsExtras[0] != '\0') {
            try {
                extras = json::parse(ncsExtras);
            } catch (const json::exception& e) {
                return milvus::FailureCStatus(
                    milvus::InvalidParameter,
                    std::string("Failed to parse NCS extras JSON: ") +
                        e.what());
            }
        }

        // Initialize NCS with kind and extras
        milvus::NcsSingleton::initNcs(kind, extras);

        // Success
        return milvus::SuccessCStatus();

    } catch (const std::exception& e) {
        return milvus::FailureCStatus(&e);
    }
}

CStatus
createBucket(uint64_t bucketId) {
    try {
        auto* ncs = milvus::NcsSingleton::Instance();
        if (ncs == nullptr) {
            return milvus::FailureCStatus(milvus::UnexpectedError,
                                          "NCS singleton not initialized");
        }

        milvus::NcsStatus status = ncs->createBucket(bucketId);
        if (status != milvus::NcsStatus::OK) {
            std::string errMsg =
                "Failed to create bucket with ID: " + std::to_string(bucketId);
            return milvus::FailureCStatus(milvus::UnexpectedError,
                                          errMsg.c_str());
        }

        return milvus::SuccessCStatus();

    } catch (const std::exception& e) {
        return milvus::FailureCStatus(&e);
    }
}

CStatus
deleteBucket(uint64_t bucketId) {
    try {
        auto* ncs = milvus::NcsSingleton::Instance();
        if (ncs == nullptr) {
            return milvus::FailureCStatus(milvus::UnexpectedError,
                                          "NCS singleton not initialized");
        }

        milvus::NcsStatus status = ncs->deleteBucket(bucketId);
        if (status != milvus::NcsStatus::OK) {
            std::string errMsg =
                "Failed to delete bucket with ID: " + std::to_string(bucketId);
            return milvus::FailureCStatus(milvus::UnexpectedError,
                                          errMsg.c_str());
        }

        return milvus::SuccessCStatus();

    } catch (const std::exception& e) {
        return milvus::FailureCStatus(&e);
    }
}

CStatus
isBucketExist(uint64_t bucketId, bool* exists) {
    if (exists == nullptr) {
        return milvus::FailureCStatus(milvus::InvalidParameter,
                                      "exists parameter is null");
    }

    try {
        auto* ncs = milvus::NcsSingleton::Instance();
        if (ncs == nullptr) {
            return milvus::FailureCStatus(milvus::UnexpectedError,
                                          "NCS singleton not initialized");
        }

        *exists = ncs->isBucketExist(bucketId);

        return milvus::SuccessCStatus();

    } catch (const std::exception& e) {
        return milvus::FailureCStatus(&e);
    }
}

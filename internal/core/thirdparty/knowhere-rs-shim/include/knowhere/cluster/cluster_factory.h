#pragma once

#include <string>

#include "knowhere/config.h"
#include "knowhere/dataset.h"
#include "knowhere/expected.h"

namespace knowhere {

inline constexpr const char* KMEANS_CLUSTER = "KMEANS";

class ClusterNode {};

template <typename Node>
class Cluster {
 public:
    Cluster() = default;

    expected<DataSetPtr>
    Train(const DataSet&, const Config&) {
        return Status::not_implemented;
    }

    expected<DataSetPtr>
    GetCentroids() const {
        return Status::not_implemented;
    }

    expected<DataSetPtr>
    Assign(const DataSet&) {
        return Status::not_implemented;
    }
};

class ClusterFactory {
 public:
    static ClusterFactory&
    Instance() {
        static ClusterFactory instance;
        return instance;
    }

    template <typename T>
    expected<Cluster<ClusterNode>>
    Create(const std::string&) const {
        return Status::invalid_cluster_error;
    }
};

}  // namespace knowhere

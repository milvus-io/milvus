// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#pragma once

#include "SystemInfo.h"
#include "utils/Status.h"

#include <string>

namespace milvus {
namespace server {
class MetricsBase {
 public:
    static MetricsBase&
    GetInstance() {
        static MetricsBase instance;
        return instance;
    }

    virtual Status
    Init() {
        return Status::OK();
    }

    virtual void
    AddVectorsSuccessTotalIncrement(double value = 1) {
    }

    virtual void
    AddVectorsFailTotalIncrement(double value = 1) {
    }

    virtual void
    AddVectorsDurationHistogramOberve(double value) {
    }

    virtual void
    RawFileSizeHistogramObserve(double value) {
    }

    virtual void
    IndexFileSizeHistogramObserve(double value) {
    }

    virtual void
    BuildIndexDurationSecondsHistogramObserve(double value) {
    }

    virtual void
    CpuCacheUsageGaugeSet(double value) {
    }

    virtual void
    GpuCacheUsageGaugeSet() {
    }

    virtual void
    MetaAccessTotalIncrement(double value = 1) {
    }

    virtual void
    MetaAccessDurationSecondsHistogramObserve(double value) {
    }

    virtual void
    FaissDiskLoadDurationSecondsHistogramObserve(double value) {
    }

    virtual void
    FaissDiskLoadSizeBytesHistogramObserve(double value) {
    }

    virtual void
    CacheAccessTotalIncrement(double value = 1) {
    }

    virtual void
    MemTableMergeDurationSecondsHistogramObserve(double value) {
    }

    virtual void
    SearchIndexDataDurationSecondsHistogramObserve(double value) {
    }

    virtual void
    SearchRawDataDurationSecondsHistogramObserve(double value) {
    }

    virtual void
    IndexFileSizeTotalIncrement(double value = 1) {
    }

    virtual void
    RawFileSizeTotalIncrement(double value = 1) {
    }

    virtual void
    IndexFileSizeGaugeSet(double value) {
    }

    virtual void
    RawFileSizeGaugeSet(double value) {
    }

    virtual void
    FaissDiskLoadIOSpeedGaugeSet(double value) {
    }

    virtual void
    QueryResponseSummaryObserve(double value) {
    }

    virtual void
    DiskStoreIOSpeedGaugeSet(double value) {
    }

    virtual void
    DataFileSizeGaugeSet(double value) {
    }

    virtual void
    AddVectorsSuccessGaugeSet(double value) {
    }

    virtual void
    AddVectorsFailGaugeSet(double value) {
    }

    virtual void
    QueryVectorResponseSummaryObserve(double value, int count = 1) {
    }

    virtual void
    QueryVectorResponsePerSecondGaugeSet(double value) {
    }

    virtual void
    CPUUsagePercentSet() {
    }

    virtual void
    RAMUsagePercentSet() {
    }

    virtual void
    QueryResponsePerSecondGaugeSet(double value) {
    }

    virtual void
    GPUPercentGaugeSet() {
    }

    virtual void
    GPUMemoryUsageGaugeSet() {
    }

    virtual void
    AddVectorsPerSecondGaugeSet(int num_vector, int dim, double time) {
    }

    virtual void
    QueryIndexTypePerSecondSet(std::string type, double value) {
    }

    virtual void
    ConnectionGaugeIncrement() {
    }

    virtual void
    ConnectionGaugeDecrement() {
    }

    virtual void
    KeepingAliveCounterIncrement(double value = 1) {
    }

    virtual void
    OctetsSet() {
    }

    virtual void
    CPUCoreUsagePercentSet() {
    }

    virtual void
    GPUTemperature() {
    }

    virtual void
    CPUTemperature() {
    }

    virtual void
    PushToGateway() {
    }
};

}  // namespace server
}  // namespace milvus

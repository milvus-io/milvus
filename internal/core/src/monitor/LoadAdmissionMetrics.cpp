// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include "monitor/Monitor.h"

#include <map>
#include <string>

namespace milvus::monitor {
namespace {
const std::map<std::string, std::string> high_label{{"priority", "high"}};
const std::map<std::string, std::string> low_label{{"priority", "low"}};
const std::map<std::string, std::string> high_admitted_label{
    {"priority", "high"}, {"outcome", "admitted"}};
const std::map<std::string, std::string> high_cancelled_label{
    {"priority", "high"}, {"outcome", "cancelled"}};
const std::map<std::string, std::string> low_admitted_label{
    {"priority", "low"}, {"outcome", "admitted"}};
const std::map<std::string, std::string> low_cancelled_label{
    {"priority", "low"}, {"outcome", "cancelled"}};
const prometheus::Histogram::BucketBoundaries queue_wait_buckets{0.000001,
                                                                 0.00001,
                                                                 0.0001,
                                                                 0.001,
                                                                 0.005,
                                                                 0.01,
                                                                 0.05,
                                                                 0.1,
                                                                 0.5,
                                                                 1,
                                                                 5,
                                                                 10,
                                                                 30,
                                                                 60,
                                                                 300};
}  // namespace

DEFINE_PROMETHEUS_GAUGE_FAMILY(
    internal_load_admission_reserved_bytes,
    "[cpp]Estimated transient bytes reserved by admitted load work, not RSS");
DEFINE_PROMETHEUS_GAUGE(internal_load_admission_reserved_bytes,
                        internal_load_admission_reserved_bytes,
                        {});

DEFINE_PROMETHEUS_GAUGE_FAMILY(
    internal_load_admission_capacity_bytes,
    "[cpp]Effective transient byte capacity; zero means unlimited");
DEFINE_PROMETHEUS_GAUGE(internal_load_admission_capacity_bytes,
                        internal_load_admission_capacity_bytes,
                        {});

DEFINE_PROMETHEUS_GAUGE_FAMILY(
    internal_load_admission_reserved_slots,
    "[cpp]Slots reserved by unfinished admitted load work");
DEFINE_PROMETHEUS_GAUGE(internal_load_admission_reserved_slots,
                        internal_load_admission_reserved_slots,
                        {});

DEFINE_PROMETHEUS_GAUGE_FAMILY(
    internal_load_admission_capacity_slots,
    "[cpp]Effective load slot capacity; zero means unlimited");
DEFINE_PROMETHEUS_GAUGE(internal_load_admission_capacity_slots,
                        internal_load_admission_capacity_slots,
                        {});

DEFINE_PROMETHEUS_GAUGE_FAMILY(
    internal_load_admission_pending_requests,
    "[cpp]Queued load requests holding no reservation");
DEFINE_PROMETHEUS_GAUGE(internal_load_admission_pending_requests_high,
                        internal_load_admission_pending_requests,
                        high_label);
DEFINE_PROMETHEUS_GAUGE(internal_load_admission_pending_requests_low,
                        internal_load_admission_pending_requests,
                        low_label);

DEFINE_PROMETHEUS_GAUGE_FAMILY(
    internal_load_admission_oldest_wait_seconds,
    "[cpp]Age in seconds of the oldest queued load request; zero when empty");
DEFINE_PROMETHEUS_GAUGE(internal_load_admission_oldest_wait_seconds_high,
                        internal_load_admission_oldest_wait_seconds,
                        high_label);
DEFINE_PROMETHEUS_GAUGE(internal_load_admission_oldest_wait_seconds_low,
                        internal_load_admission_oldest_wait_seconds,
                        low_label);

DEFINE_PROMETHEUS_HISTOGRAM_FAMILY(
    internal_load_admission_queue_wait_seconds,
    "[cpp]Seconds from enqueue to admission or cancellation decision; excludes "
    "immediate requests and resumption delay");
DEFINE_PROMETHEUS_HISTOGRAM_WITH_BUCKETS(
    internal_load_admission_queue_wait_seconds_high_admitted,
    internal_load_admission_queue_wait_seconds,
    high_admitted_label,
    queue_wait_buckets);
DEFINE_PROMETHEUS_HISTOGRAM_WITH_BUCKETS(
    internal_load_admission_queue_wait_seconds_high_cancelled,
    internal_load_admission_queue_wait_seconds,
    high_cancelled_label,
    queue_wait_buckets);
DEFINE_PROMETHEUS_HISTOGRAM_WITH_BUCKETS(
    internal_load_admission_queue_wait_seconds_low_admitted,
    internal_load_admission_queue_wait_seconds,
    low_admitted_label,
    queue_wait_buckets);
DEFINE_PROMETHEUS_HISTOGRAM_WITH_BUCKETS(
    internal_load_admission_queue_wait_seconds_low_cancelled,
    internal_load_admission_queue_wait_seconds,
    low_cancelled_label,
    queue_wait_buckets);

}  // namespace milvus::monitor

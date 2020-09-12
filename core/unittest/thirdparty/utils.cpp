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

#include <easyloggingpp/easylogging++.h>
#include "utils.h"

INITIALIZE_EASYLOGGINGPP

void
DabloomsTest::SetUp() {
}

void
DabloomsTest::TearDown() {
}

void
DabloomsTest::bloom_score(int positive, int should_positive, struct stats *stats)
{
    if (should_positive) {
        ASSERT_TRUE(positive);
        if (positive) {
            stats->true_positives++;
        } else {
            stats->false_negatives++;
        }
    } else {
        if (positive) {
            stats->false_positives++;
        } else {
            stats->true_negatives++;
        }
    }
}

int
DabloomsTest::print_results(struct stats *stats)
{
    float false_positive_rate = (float)stats->false_positives /
                                (float) (stats->false_positives + stats->true_negatives);

    printf("True positives:     %7d"   "\n"
           "True negatives:     %7d"   "\n"
           "False positives:    %7d"   "\n"
           "False negatives:    %7d"   "\n"
           "False positive rate: %.4f" "\n",
           stats->true_positives,
           stats->true_negatives,
           stats->false_positives,
           stats->false_negatives,
           false_positive_rate             );

    if (stats->false_negatives > 0) {
        printf("TEST FAIL (false negatives exist)\n");
        return TEST_FAIL;
    } else if (false_positive_rate > ERROR_RATE) {
        printf("TEST WARN (false positive rate too high)\n");
        return TEST_WARN;
    } else {
        printf("TEST PASS\n");
        return TEST_PASS;
    }
}
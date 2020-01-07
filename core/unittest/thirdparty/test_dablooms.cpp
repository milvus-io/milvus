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

#include <cstdio>
#include <string>
#include <cstdlib>
#include <chrono>
#include <mutex>

#include "dablooms/dablooms.h"
#include "utils.h"

TEST_F(DabloomsTest, CORRECTNESS_TEST){
    FILE *fp;
    counting_bloom_t *bloom;
    int32_t i;
    struct stats results = { 0 };

    if ((fp = fopen(bloom_file, "r"))) {
        fclose(fp);
        remove(bloom_file);
    }

    bloom = new_counting_bloom(2 * CAPACITY, ERROR_RATE, bloom_file);
    ASSERT_NE(bloom, nullptr);

    auto start = std::chrono::system_clock::now();

    std::mutex lock;

    for (i = 0; i < 2 * CAPACITY; i++) {
        if (i % 2 == 0) {
            std::string tmp = std::to_string(i);
            lock.lock();
            counting_bloom_add(bloom, tmp.c_str(), tmp.size());
            lock.unlock();
        }
    }

    auto end = std::chrono::system_clock::now();
    auto duration = std::chrono::duration_cast<std::chrono::microseconds>(end - start);
    std::cout << "Time costs for add: " << double(duration.count()) * std::chrono::microseconds::period::num / std::chrono::microseconds::period::den << " s" << std::endl;
    start = std::chrono::system_clock::now();

    for (i = 0; i < 2 * CAPACITY; i++) {
        if (i % 2 == 1) {
            std::string tmp = std::to_string(i);
            lock.lock();
            bloom_score(counting_bloom_check(bloom, tmp.c_str(), tmp.size()), 0, &results);
            lock.unlock();
        }
    }

    end = std::chrono::system_clock::now();
    duration = std::chrono::duration_cast<std::chrono::microseconds>(end - start);
    std::cout << "Time costs for check: " << double(duration.count()) * std::chrono::microseconds::period::num / std::chrono::microseconds::period::den << " s" << std::endl;

    fclose(fp);

    printf("Elements added:   %6d" "\n"
           "Elements checked: %6d" "\n"
           "Total size: %d KiB"  "\n\n",
           (i + 1) / 2, i / 2,
           (int) bloom->num_bytes / 1024);

    free_counting_bloom(bloom);

    print_results(&results);
}

TEST_F(DabloomsTest, FILE_OPT_TEST){
    FILE *fp;
    counting_bloom_t *bloom;
    int i, key_removed;
    struct stats results1 = { 0 };
    struct stats results2 = { 0 };

    if ((fp = fopen(bloom_file, "r"))) {
        fclose(fp);
        remove(bloom_file);
    }

    bloom = new_counting_bloom(CAPACITY, ERROR_RATE, bloom_file);
    ASSERT_NE(bloom, nullptr);

    auto start = std::chrono::system_clock::now();

    for (i = 0; i < CAPACITY; i++) {
        std::string tmp = std::to_string(i);
        counting_bloom_add(bloom, tmp.c_str(), tmp.size());
    }

    auto end = std::chrono::system_clock::now();
    auto duration = std::chrono::duration_cast<std::chrono::microseconds>(end - start);
    std::cout << std::endl << "Time costs for add: " << double(duration.count()) * std::chrono::microseconds::period::num / std::chrono::microseconds::period::den << " s" << std::endl;
    start = std::chrono::system_clock::now();

    for (i = 0; i < CAPACITY; i++) {
        if (i % 5 == 0) {
            std::string tmp = std::to_string(i);
            counting_bloom_remove(bloom, tmp.c_str(), tmp.size());
        }
    }

    for (i = 0; i < CAPACITY; i++) {
        std::string tmp = std::to_string(i);
        key_removed = (i % 5 == 0);
        bloom_score(counting_bloom_check(bloom, tmp.c_str(), tmp.size()), !key_removed, &results1);
    }

    end = std::chrono::system_clock::now();
    duration = std::chrono::duration_cast<std::chrono::microseconds>(end - start);
    std::cout << "Time costs for remove: " << double(duration.count()) * std::chrono::microseconds::period::num / std::chrono::microseconds::period::den << " s" << std::endl;

    free_counting_bloom(bloom);
    bloom = new_counting_bloom_from_file(CAPACITY, ERROR_RATE, bloom_file);

    ASSERT_NE(bloom, nullptr);

    for (i = 0; i < CAPACITY; i++) {
        std::string tmp = std::to_string(i);
        key_removed = (i % 5 == 0);
        bloom_score(counting_bloom_check(bloom, tmp.c_str(), tmp.size()), !key_removed, &results2);
    }
    fclose(fp);

    printf("Elements added:   %6d" "\n"
           "Elements removed: %6d" "\n"
           "Total size: %d KiB"  "\n\n",
           i, i / 5,
           (int) bloom->num_bytes / 1024);

    free_counting_bloom(bloom);

    print_results(&results1);

    ASSERT_EQ(results1.false_negatives, results2.false_negatives);
    ASSERT_EQ(results1.false_positives, results2.false_positives);
    ASSERT_EQ(results1.true_negatives, results2.true_negatives);
    ASSERT_EQ(results1.true_positives, results2.true_positives);
}

TEST_F(DabloomsTest, xxx){
    FILE *fp;
    counting_bloom_t *bloom;
    int32_t i;
    struct stats results = { 0 };

    if ((fp = fopen(bloom_file, "r"))) {
        fclose(fp);
        remove(bloom_file);
    }

    bloom = new_counting_bloom(2 * CAPACITY, ERROR_RATE, bloom_file);
    ASSERT_NE(bloom, nullptr);

    std::mutex lock;

    for (i = 0; i < CAPACITY; i++) {
        std::string tmp = std::to_string(i);
        lock.lock();
        counting_bloom_add(bloom, tmp.c_str(), tmp.size());
        lock.unlock();
    }

    for (i = 0; i < CAPACITY; i++) {
        std::string tmp = std::to_string(i);
        lock.lock();
        counting_bloom_add(bloom, tmp.c_str(), tmp.size());
        lock.unlock();
    }

    for (i = 0; i < CAPACITY; i++) {
        std::string tmp = std::to_string(i);
        lock.lock();
        counting_bloom_remove(bloom, tmp.c_str(), tmp.size());
        lock.unlock();
    }

    for (i = 0; i < CAPACITY; i++) {
        std::string tmp = std::to_string(i);
        lock.lock();
        bloom_score(counting_bloom_check(bloom, tmp.c_str(), tmp.size()), 1, &results);
        lock.unlock();
    }

    print_results(&results);
    results = {0, 0, 0, 0};

    for (i = 0; i < CAPACITY; i++) {
        std::string tmp = std::to_string(i);
        lock.lock();
        counting_bloom_remove(bloom, tmp.c_str(), tmp.size());
        lock.unlock();
    }

    for (i = 0; i < CAPACITY; i++) {
        std::string tmp = std::to_string(i);
        lock.lock();
        bloom_score(counting_bloom_check(bloom, tmp.c_str(), tmp.size()), 0, &results);
        lock.unlock();
    }

    free_counting_bloom(bloom);

    print_results(&results);
}
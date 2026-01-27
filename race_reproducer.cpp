/**
 * Minimal reproducer for the SegmentGrowingImpl race condition.
 *
 * This mimics the exact pattern in Milvus:
 * - sch_mutex_ protects schema updates (Reopen/fill_empty_field)
 * - But Search/Retrieve reads from insert_record_ WITHOUT acquiring sch_mutex_
 *
 * Compile with ThreadSanitizer:
 *   g++ -std=c++17 -g -fsanitize=thread -o race_reproducer race_reproducer.cpp -lpthread
 *
 * Run:
 *   ./race_reproducer
 *
 * Expected:
 *   - Buggy version: TSan reports data race
 *   - Fixed version: No race detected
 */

#include <atomic>
#include <chrono>
#include <iostream>
#include <mutex>
#include <shared_mutex>
#include <thread>
#include <vector>

// Simulates insert_record_ data structure
struct InsertRecord {
    std::vector<int64_t> data;
    std::atomic<size_t> size{0};

    void set_data(size_t count, int64_t value) {
        // This simulates fill_empty_field writing to insert_record_
        if (data.size() < count) {
            data.resize(count);
        }
        for (size_t i = 0; i < count; i++) {
            data[i] = value;  // WRITE - race with readers
        }
        size.store(count);
    }

    int64_t read_data(size_t idx) const {
        // This simulates Search reading from insert_record_
        if (idx < size.load()) {
            return data[idx];  // READ - race with writer
        }
        return -1;
    }
};

// ============================================================================
// BUGGY VERSION: Simulates SegmentGrowingImpl before fix
// ============================================================================
class SegmentBuggy {
public:
    mutable std::shared_mutex sch_mutex_;  // Only used by Insert and Reopen
    InsertRecord insert_record_;
    std::atomic<int> schema_version_{1};

    // Simulates Insert() - acquires shared lock
    void Insert(size_t count, int64_t value) {
        std::shared_lock lock(sch_mutex_);
        insert_record_.set_data(count, value);
    }

    // Simulates Reopen() -> fill_empty_field() - acquires unique lock
    void Reopen(int new_version) {
        std::unique_lock lock(sch_mutex_);
        if (new_version > schema_version_.load()) {
            // fill_empty_field - writes to insert_record_
            // This is protected by sch_mutex_
            insert_record_.set_data(10000, new_version * 100);
            schema_version_.store(new_version);
        }
    }

    // Simulates Search() - NO LOCK! This is the bug.
    int64_t Search() const {
        // BUG: Search reads from insert_record_ without acquiring sch_mutex_
        // This races with Reopen's fill_empty_field
        int64_t sum = 0;
        size_t sz = insert_record_.size.load();
        for (size_t i = 0; i < sz && i < 1000; i++) {
            sum += insert_record_.read_data(i);  // RACE HERE
        }
        return sum;
    }
};

// ============================================================================
// FIXED VERSION: Simulates SegmentGrowingImpl after fix
// ============================================================================
class SegmentFixed {
public:
    mutable std::shared_mutex sch_mutex_;  // Now also used by Search
    InsertRecord insert_record_;
    std::atomic<int> schema_version_{1};

    // Simulates Insert() - acquires shared lock
    void Insert(size_t count, int64_t value) {
        std::shared_lock lock(sch_mutex_);
        insert_record_.set_data(count, value);
    }

    // Simulates Reopen() -> fill_empty_field() - acquires unique lock
    void Reopen(int new_version) {
        std::unique_lock lock(sch_mutex_);
        if (new_version > schema_version_.load()) {
            // fill_empty_field - writes to insert_record_
            insert_record_.set_data(10000, new_version * 100);
            schema_version_.store(new_version);
        }
    }

    // Simulates Search() - NOW ACQUIRES SHARED LOCK (FIX)
    int64_t Search() const {
        // FIX: Acquire shared lock to prevent race with Reopen/fill_empty_field
        std::shared_lock lock(sch_mutex_);
        int64_t sum = 0;
        size_t sz = insert_record_.size.load();
        for (size_t i = 0; i < sz && i < 1000; i++) {
            sum += insert_record_.read_data(i);  // No race - protected by shared lock
        }
        return sum;
    }
};

template<typename Segment>
void run_test(const char* name, Segment& segment) {
    std::cout << "\n" << name << "\n";
    std::cout << std::string(strlen(name), '-') << "\n";

    // Initial data
    segment.Insert(10000, 42);

    std::atomic<bool> start{false};
    std::atomic<bool> stop{false};
    std::atomic<int> search_count{0};
    std::atomic<int> reopen_count{0};

    // Search threads (readers)
    std::vector<std::thread> search_threads;
    for (int i = 0; i < 4; i++) {
        search_threads.emplace_back([&]() {
            while (!start.load()) std::this_thread::yield();
            while (!stop.load()) {
                segment.Search();
                search_count.fetch_add(1);
            }
        });
    }

    // Reopen thread (writer)
    std::thread reopen_thread([&]() {
        while (!start.load()) std::this_thread::yield();
        for (int v = 2; v <= 10; v++) {
            segment.Reopen(v);
            reopen_count.fetch_add(1);
            std::this_thread::sleep_for(std::chrono::milliseconds(10));
        }
    });

    start.store(true);
    std::this_thread::sleep_for(std::chrono::milliseconds(200));
    stop.store(true);

    for (auto& t : search_threads) t.join();
    reopen_thread.join();

    std::cout << "  Search operations: " << search_count.load() << "\n";
    std::cout << "  Reopen operations: " << reopen_count.load() << "\n";
}

int main() {
    std::cout << "Race Condition Reproducer for SegmentGrowingImpl\n";
    std::cout << "=================================================\n";
    std::cout << "\nThis reproduces the bug where:\n";
    std::cout << "- Reopen() holds sch_mutex_ while writing to insert_record_\n";
    std::cout << "- Search() reads from insert_record_ WITHOUT any lock\n";

    std::cout << "\n*** Running BUGGY version (Search without lock) ***\n";
    SegmentBuggy buggy;
    run_test("Buggy Version", buggy);
    std::cout << "\n--> If compiled with -fsanitize=thread, TSan should report data race above.\n";

    std::cout << "\n*** Running FIXED version (Search acquires shared lock) ***\n";
    SegmentFixed fixed;
    run_test("Fixed Version", fixed);
    std::cout << "\n--> No race should be reported for the fixed version.\n";

    return 0;
}

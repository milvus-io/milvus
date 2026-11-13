// Licensed under the Apache License, Version 2.0.

#include <cstring>
#include <mutex>
#include <thread>

extern "C" void
tsan_increment_c(void);
void
tsan_increment_cpp();

int
main(int argc, char** argv) {
    const bool clean = argc == 2 && std::strcmp(argv[1], "clean") == 0;
    const bool c_access = argc == 2 && std::strcmp(argv[1], "race-c") == 0;
    const bool memcpy_race =
        argc == 2 && std::strcmp(argv[1], "race-memcpy") == 0;
    char destination[256] = {};
    const char source[256] = {};
    using Copy = void* (*)(void*, const void*, std::size_t);
    Copy volatile copy = &std::memcpy;
    std::mutex mutex;
    auto increment = [&] {
        for (int i = 0; i < 10000; ++i) {
            if (memcpy_race) {
                copy(destination, source, sizeof(source));
            } else if (clean) {
                std::lock_guard<std::mutex> lock(mutex);
                tsan_increment_c();
                tsan_increment_cpp();
            } else if (c_access) {
                tsan_increment_c();
            } else {
                tsan_increment_cpp();
            }
        }
    };
    std::thread first(increment);
    std::thread second(increment);
    first.join();
    second.join();
    return 0;
}

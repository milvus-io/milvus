// Licensed under the Apache License, Version 2.0.

namespace {
volatile int cpp_value;
}

void
tsan_increment_cpp() {
    cpp_value = cpp_value + 1;
}

// Licensed under the Apache License, Version 2.0.

static volatile int c_value;

void
tsan_increment_c(void) {
    ++c_value;
}
